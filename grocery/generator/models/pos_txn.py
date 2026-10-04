"""The POS transaction writer — one tick's store transactions.

Owns `pos.transactions` / `pos.transaction_items`: `generate_pos_transactions`
plus the price-of-record and basket helpers it draws on, the payment mix, and
the line-tagging helpers for coupon and deal application.

Split out of `models/pos.py` (t_c2eca5dd); `pos.py` re-exports every public name.
"""
import logging
import os
import random
from datetime import date, datetime
from typing import Dict, List, Optional
from uuid import uuid4

from faker import Faker
from psycopg2.extras import execute_values

from config import Config
from scenarios.scenario_engine import ScenarioContext
from elasticity import choose_products

# Package-relative, for the reason documented in `pos.py`: an absolute
# `models.*` import would load a second copy of the sibling under the flat name
# when this tree is imported as `grocery.generator.models`.
from .pos_promotions import _promo_applies_on
from .pos_loyalty import _record_loyalty_points

log = logging.getLogger(__name__)
fake = Faker('en_US')


# ---------------------------------------------------------------------------
# read_schema_sql
# ---------------------------------------------------------------------------

def read_schema_sql() -> str:
    """The generator's own `schema.sql`, for tests that check the DDL.

    Read lazily rather than at import: a model module should not do file I/O
    as a side effect of being imported, and a test that needs the DDL should
    fail loudly when it is missing rather than get an empty string.
    """
    path = os.path.join(os.path.dirname(__file__), '..', 'schema.sql')
    with open(path, 'r') as f:
        return f.read()

# ---------------------------------------------------------------------------
# payment mix
# ---------------------------------------------------------------------------

PAYMENT_METHODS = ['cash', 'credit', 'debit', 'ebt', 'mobile_pay', 'loyalty_points']
PAYMENT_WEIGHTS = [0.12, 0.38, 0.28, 0.08, 0.10, 0.04]

# ---------------------------------------------------------------------------
# price of record / basket
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# The price of record (t_08deeddf)
# ---------------------------------------------------------------------------

def price_of_record(products: List[Dict], ad_prices: Dict[str, float]) -> Dict[str, float]:
    """Map product_id -> the price a shopper faces today.

    For a weekly-ad item that is `promoted_price`; for everything else it is
    `current_price`. The demand curve is keyed on this, so a discount works
    through the same law as any other price move instead of needing a special
    case — and the price actually charged is recorded in
    `transaction_items.unit_price` as before, so `assert_line_totals_balance`
    (`line_total = (unit_price - discount) * quantity`) is untouched.
    """
    prices: Dict[str, float] = {}
    for product in products:
        product_id = product.get('product_id')
        if product_id is None:
            continue
        price = product.get('price')
        if price is None:
            price = product.get('current_price')
        if price is not None:
            prices[product_id] = float(price)
    for product_id, promoted in (ad_prices or {}).items():
        if promoted is not None:
            prices[product_id] = promoted
    return prices


def _price_of(product: Dict, price_record: Dict[str, float]) -> float:
    """The price this product is being sold at, from the tick's price record."""
    price = price_record.get(str(product.get('product_id')))
    if price is None:
        price = product.get('price', product.get('current_price'))
    return float(price) if price is not None else 0.0


def draw_cart(products: List[Dict], num_items: int, cfg: Config,
              ad_prices: Optional[Dict[str, float]] = None) -> List[Dict]:
    """Pick `num_items` products, weighted by price elasticity.

    The fix for t_08deeddf: this used to be `random.choices(products,
    k=num_items)`, which treats a cents item and a ten-dollar item as equally
    likely and ignored the weekly ad entirely — so `price_history` could not
    explain any of the demand it was seeded for. Basket SIZE is still the
    pre-existing distribution: customers do not buy fewer things because one
    of them got dearer, and the POS volume contract (t_94bbf1ce) must not move.
    """
    return choose_products(products, num_items, ad_prices, cfg)

# ---------------------------------------------------------------------------
# generate_pos_transactions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Transaction generation
# ---------------------------------------------------------------------------

def generate_pos_transactions(
    conn,
    cfg: Config,
    simulation_dt: datetime,
    count: int,
    scenario: ScenarioContext,
    store_locations: List[Dict],
    products: List[Dict],
    employees: List[Dict],
    members: List[Dict],
    coupons: List[Dict],
    deals: List[Dict],
    ad_prices: Optional[Dict[str, float]] = None,
    stock_allowance=None,
) -> List[Dict]:
    """
    Generate `count` POS transactions. Returns depletion info for inventory.

    `ad_prices` maps product_id -> the weekly-ad `promoted_price` for the
    simulated date, i.e. the price of record for that SKU this tick. It is
    what makes an ad item sell through the price-elasticity curve instead of
    being decoration on a shelf nobody reorders (t_08deeddf); it defaults to
    None so an existing caller that has no ad context keeps working.

    `stock_allowance` is the tick's shared `inventory.StockAllowance` (t_959cd040).
    Every line is capped at what the shelf can actually cover and the shortfall
    is recorded as a lost sale, so `pos.transaction_items.quantity` is what the
    shopper actually got. main.py builds ONE allowance per tick and passes it to
    both channels, because POS and online sell the same shelf. When it is None
    (or cfg.inventory.enforce_stock_availability is false) nothing is capped and
    this is the pre-t_959cd040 generator — the regression test uses that to
    demonstrate the old behaviour rather than only asserting the new one.
    """
    if count <= 0 or not store_locations or not products:
        return []

    cashiers = [e for e in employees if e['department'] in
                ('store', 'produce', 'deli', 'bakery', 'meat', 'management')
                and e['location_type'] == 'store']

    # Promotions are only applied inside their own validity window. The
    # candidate list is fetched against *today*, so during a backfill it also
    # contains promos that were not on the books yet on the simulated date;
    # tagging those onto a back-dated transaction is what put 24k coupon /
    # 7k deal items outside their promo's window (card t_01b4fe4f). Filtering
    # on the simulated date makes `transaction_dt between valid_from and
    # valid_until` hold by construction.
    txn_date = simulation_dt.date()
    active_coupons = [c for c in coupons if _promo_applies_on(c, txn_date)]
    active_deals = [d for d in deals if _promo_applies_on(d, txn_date)]

    # Active promotions by department name
    promo_dept_discount = {dept: disc for dept, disc in scenario.active_promotions}

    txn_records = []
    item_records = []
    new_members = []
    depletion_info = []
    # Redemptions served by this batch, per coupon. `pos.coupons.uses_count` is
    # incremented once per coupon-tagged transaction (the same definition
    # reconcile_promotions() recomputes), so the counter stays live instead of
    # going stale between reconcile passes.
    coupon_uses: Dict[str, int] = {}

    # The price each SKU is being sold at today: the weekly-ad promoted price
    # where one applies, the shelf price otherwise. Computed ONCE per tick, not
    # per line — it is a property of the date, and recomputing it inside the
    # loop would be both wasteful and a chance for the two to disagree.
    price_record = price_of_record(products, ad_prices or {})

    # Stock-aware capping (t_959cd040). No allowance, or the config switch off,
    # reproduces the old "sell whatever the basket asked for" behaviour.
    enforce_stock = stock_allowance is not None and getattr(
        cfg.inventory, 'enforce_stock_availability', True)

    for _ in range(count):
        loc = random.choice(store_locations)
        loc_cashiers = [e for e in cashiers if e['location_id'] == loc['location_id']]
        employee_id = random.choice(loc_cashiers)['employee_id'] if loc_cashiers else None

        # Loyalty
        member_id = None
        if members and random.random() < cfg.loyalty.loyalty_usage_rate:
            member_id = random.choice(members)['member_id']

        # Number of items
        num_items = random.choices(
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15],
            weights=[5, 8, 12, 14, 13, 12, 10, 8, 6, 5, 4, 3]
        )[0]
        # WHICH products — the elasticity loop (t_08deeddf). Was
        # `random.choices(products, k=num_items)`, i.e. uniform: a 97%-off item
        # and a full-price item were exactly as likely to reach the basket,
        # which is why the 90-day price_history backfill could not explain any
        # demand. The ad price is also what makes an ad item actually sell.
        cart = draw_cart(products, num_items, cfg, ad_prices)

        txn_id = str(uuid4())
        item_recs = []

        # The demand this basket represents, before the shelf has its say.
        # `line_id` only has to be unique within the batch; txn_id already is,
        # so the position in the basket disambiguates a repeated product.
        demand_lines = [
            {
                'line_id': f'{txn_id}:{position}',
                'location_id': loc['location_id'],
                'product_id': product['product_id'],
                'parent_id': txn_id,
                'quantity': (
                    round(random.uniform(0.2, 3.5), 3)
                    if product['uom'] == 'lb'
                    else random.choices([1, 2, 3], weights=[78, 17, 5])[0]
                ),
                'unit_price': price_record.get(product['product_id'], product['price']),
                'department': product['department'],
            }
            for position, product in enumerate(cart)
        ]

        # Resolve the basket against the shelf. Everything downstream —
        # subtotal, tax, the coupon/deal caps, loyalty points — prices what the
        # shopper actually got, so revenue, stock and the loss record all
        # describe the same transaction (t_959cd040).
        if enforce_stock:
            granted = stock_allowance.take(demand_lines)
            keep = [line for line in demand_lines
                    if granted.get(line['line_id'], 0.0) > 0]
            # Narrow `cart` to the lines that survived, preserving order and
            # duplicates, so the promo-scoping helpers below still see the
            # basket as it will be written.
            surviving = {line['line_id'] for line in keep}
            demand_lines = [
                {**line, 'quantity': granted[line['line_id']]}
                for line in demand_lines if line['line_id'] in surviving
            ]
            cart = [
                product for position, product in enumerate(cart)
                if f'{txn_id}:{position}' in surviving
            ]

        if not demand_lines:
            # Every line was out of stock: the shopper leaves with nothing, so
            # there is no transaction to write. The loss is recorded against the
            # tick by main.commit_sales from the uncapped lines.
            continue

        subtotal = 0.0
        for line in demand_lines:
            qty = line['quantity']
            unit_price = line['unit_price']
            discount = 0.0
            coupon_id = None
            deal_id = None

            # Promotional discount
            if line['department'] in promo_dept_discount:
                discount += round(unit_price * promo_dept_discount[line['department']], 2)

            effective_price = max(0.01, unit_price - discount)
            line_total = round(effective_price * qty, 2)
            subtotal += line_total

            item_recs.append((txn_id, line['product_id'], qty,
                               unit_price, discount, coupon_id, deal_id, line_total))

        # Apply a coupon to the whole transaction (loyalty members only)
        coupon_savings = 0.0
        coupon = None
        loyalty_eng = getattr(scenario, 'loyalty_engagement_modifier', 1.0)
        if member_id and active_coupons and random.random() < cfg.coupons.coupon_use_rate * scenario.coupon_multiplier * loyalty_eng:
            coupon = random.choice(active_coupons)
            if coupon['coupon_type'] == 'percent_off':
                coupon_savings = round(subtotal * coupon['discount_value'] * scenario.coupon_multiplier, 2)
            elif coupon['coupon_type'] == 'dollar_off':
                coupon_savings = round(min(subtotal * 0.5, coupon['discount_value'] * scenario.coupon_multiplier), 2)

        # Apply a combo deal
        deal_savings = 0.0
        deal = None
        if active_deals and random.random() < cfg.combo_deals.combo_use_rate:
            deal = random.choice(active_deals)
            deal_dept_products = [p for p in cart
                                   if deal['trigger_department_id'] is None
                                   or _dept_id_for_product(p) == deal['trigger_department_id']]
            if len(deal_dept_products) >= deal['trigger_qty']:
                # Saving = sum of trigger_qty items minus deal_price. Priced
                # off the price OF RECORD (t_08deeddf): comparing a deal
                # against the pre-ad shelf price would book a saving the
                # customer never received, and `mart_promotion_effectiveness`
                # would credit the deal for the weekly ad's discount.
                trigger_items = sorted(deal_dept_products,
                                       key=lambda p: _price_of(p, price_record),
                                       reverse=True)[:deal['trigger_qty']]
                original = sum(_price_of(p, price_record) for p in trigger_items)
                deal_savings = max(0.0, round(original - deal['deal_price'], 2))

        subtotal = round(subtotal, 2)
        # Coupons and combo deals are computed independently and can STACK
        # past the subtotal (seen 2026-08-24/25 in prod: 2 rush_hour txns with
        # coupon+deal > subtotal). Floor the combined discounts so the pre-tax
        # total is never negative: coupons take precedence, deal savings are
        # reduced to whatever room remains. Previously the max(0.01, ...)
        # clamp below silently absorbed this, which broke dbt's
        # assert_e2e_revenue_consistency formula check.
        if coupon_savings > subtotal - 0.01:
            coupon_savings = max(0.0, round(subtotal - 0.01, 2))
        max_deal_savings = round(subtotal - coupon_savings - 0.01, 2)
        if deal_savings > max_deal_savings:
            deal_savings = max(0.0, max_deal_savings)
            if deal_savings == 0.0:
                deal = None  # deal fully crowded out; don't tag line items

        # Tag the applicable line items with the applied coupon/deal IDs so
        # data-lab can attribute redemptions per promotion. transaction_items
        # already has coupon_id/deal_id columns (no schema change); we just
        # populate them on the items the promo actually applied to.
        if coupon is not None:
            coupon_products = _applicable_promo_products(cart, coupon, 'coupon')
            tagged = False
            for i, p in enumerate(cart):
                if p['product_id'] in coupon_products:
                    rec = list(item_recs[i])
                    rec[5] = coupon['coupon_id']
                    item_recs[i] = tuple(rec)
                    tagged = True
            # Only a redemption that actually reached a line item counts as a
            # use — a department/product-scoped coupon whose scope is not in
            # this basket discounts the transaction but attributes to nothing.
            if tagged:
                cid = coupon['coupon_id']
                coupon_uses[cid] = coupon_uses.get(cid, 0) + 1
        if deal is not None:
            deal_products = _applicable_promo_products(cart, deal, 'deal')
            for i, p in enumerate(cart):
                if p['product_id'] in deal_products:
                    rec = list(item_recs[i])
                    rec[6] = deal['deal_id']
                    item_recs[i] = tuple(rec)

        total_before_tax = max(0.01, round(subtotal - coupon_savings - deal_savings, 2))
        tax = round(total_before_tax * cfg.pricing.tax_rate, 2)
        total = round(total_before_tax + tax, 2)

        methods = PAYMENT_METHODS if member_id else [m for m in PAYMENT_METHODS if m != 'loyalty_points']
        weights = PAYMENT_WEIGHTS if member_id else PAYMENT_WEIGHTS[:-1]
        payment = random.choices(methods, weights=weights[:len(methods)])[0]

        txn_records.append((
            txn_id, loc['location_id'], employee_id, member_id,
            simulation_dt, subtotal, coupon_savings, deal_savings, tax, total,
            payment, scenario.scenario_tag
        ))
        # Update item_recs with txn_id (already has it in first position)
        item_records.extend(item_recs)
        depletion_info.append({
            'transaction_id': txn_id,
            'items': [{'product_id': line['product_id'],
                       'quantity': line['quantity']} for line in demand_lines],
        })

        if random.random() < cfg.loyalty.signup_rate:
            first, last = fake.first_name(), fake.last_name()
            email = f"{first.lower()}.{last.lower()}{random.randint(1, 9999)}@email.com"
            new_members.append((first, last, email,
                                 fake.numerify('(###) ###-####'),
                                 simulation_dt.date(), 0, 'bronze'))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO pos.transactions
                (transaction_id, location_id, employee_id, member_id,
                 transaction_dt, subtotal, coupon_savings, deal_savings, tax, total,
                 payment_method, scenario_tag)
            VALUES %s
        """, txn_records,
        template="(%s::uuid,%s::uuid,%s::uuid,%s::uuid,%s,%s,%s,%s,%s,%s,%s,%s)")

        execute_values(cur, """
            INSERT INTO pos.transaction_items
                (transaction_id, product_id, quantity, unit_price, discount,
                 coupon_id, deal_id, line_total)
            VALUES %s
        """, item_records,
        template="(%s::uuid,%s::uuid,%s,%s,%s,%s::uuid,%s::uuid,%s)")

        if new_members:
            execute_values(cur, """
                INSERT INTO pos.loyalty_members
                    (first_name, last_name, email, phone, signup_date, points_balance, tier)
                VALUES %s ON CONFLICT (email) DO NOTHING
            """, new_members)

        # Counter for the redemptions this batch attributed, so the API's
        # uses_count is current rather than waiting for the next reconcile.
        # One statement per touched coupon (the promo set is a handful of
        # rows), and reconcile_promotions() still recomputes the absolute value
        # from the items table, so any drift from a regenerated day self-heals.
        for coupon_id, uses in coupon_uses.items():
            cur.execute(
                "UPDATE pos.coupons SET uses_count = uses_count + %s "
                "WHERE coupon_id = %s::uuid",
                (uses, coupon_id),
            )

    conn.commit()

    # Record loyalty point transactions and update tiers (outside the main insert)
    if txn_records:
        _record_loyalty_points(conn, txn_records)

    return depletion_info

# ---------------------------------------------------------------------------
# line tagging
# ---------------------------------------------------------------------------

def _applicable_promo_products(cart: List[Dict], promo: Dict, kind: str) -> set:
    """Return the set of product_ids in this cart that the promo applies to.

    Used to tag the correct transaction_items rows with the applied
    coupon_id / deal_id. Mirrors the scope rules used when computing the
    savings: a product-scoped promo tags only that product; a department-
    scoped promo tags every cart item in that department; a promo with no
    scope tags the whole basket.
    """
    if kind == 'coupon':
        pid = promo.get('product_id')
        did = promo.get('department_id')
    else:
        pid = promo.get('trigger_product_id')
        did = promo.get('trigger_department_id')
    if pid:
        return {pid}
    if did:
        return {p['product_id'] for p in cart if p.get('department_id') == did}
    return {p['product_id'] for p in cart}


def _dept_id_for_product(product: Dict) -> Optional[str]:
    """Products carry department_id once seed_products enriches them."""
    return product.get('department_id')
