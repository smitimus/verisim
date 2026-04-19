"""
POS model — seeds departments, products, coupons, combo deals, loyalty members;
generates store transactions with coupon/deal application.
"""
import random
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
from uuid import uuid4

from faker import Faker
from psycopg2.extras import execute_values

from config import Config
from scenarios.scenario_engine import ScenarioContext

log = logging.getLogger(__name__)
fake = Faker('en_US')

PAYMENT_METHODS = ['cash', 'credit', 'debit', 'ebt', 'mobile_pay', 'loyalty_points']
PAYMENT_WEIGHTS = [0.12, 0.38, 0.28, 0.08, 0.10, 0.04]

TIERS = ['bronze', 'silver', 'gold', 'platinum']


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def seed_departments(conn, cfg: Config) -> List[Dict]:
    """Seed grocery departments. Idempotent."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pos.departments")
        if cur.fetchone()[0] > 0:
            return _fetch_departments(cur)

        log.info("Seeding departments...")
        records = [(d['name'], d['code'], True) for d in cfg.departments]
        execute_values(cur, """
            INSERT INTO pos.departments (name, code, is_active)
            VALUES %s ON CONFLICT (name) DO NOTHING
        """, records)
        conn.commit()
        return _fetch_departments(cur)


def _fetch_departments(cur) -> List[Dict]:
    cur.execute("SELECT department_id, name, code FROM pos.departments WHERE is_active = TRUE")
    return [{'department_id': str(r[0]), 'name': r[1], 'code': r[2]} for r in cur.fetchall()]


def fetch_departments(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_departments(cur)


def seed_products(conn, cfg: Config, departments: List[Dict]) -> List[Dict]:
    """Seed products across all departments. Idempotent."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pos.products")
        if cur.fetchone()[0] > 0:
            return _fetch_active_products(cur)

    log.info("Seeding products...")
    dept_map = {d['name']: d['department_id'] for d in departments}
    records = []

    for dept_cfg in cfg.departments:
        dept_name = dept_cfg['name']
        dept_id = dept_map.get(dept_name)
        if not dept_id:
            continue

        categories = dept_cfg.get('categories', [])
        products_per_dept = max(1, cfg.initial_product_count // len(cfg.departments))

        for _ in range(products_per_dept):
            if not categories:
                continue
            cat_def = random.choice(categories)
            cat = cat_def['name']
            subcats = cat_def.get('subcategories', [cat])
            subcat = random.choice(subcats)

            cost = round(random.uniform(0.30, 12.00), 4)
            price = round(cost * random.uniform(1.25, 2.20), 2)
            sku = f"{dept_cfg['code']}-{fake.bothify('??####').upper()}"
            upc = fake.numerify('##############')
            brand = fake.company().split()[0]
            name = f"{brand} {subcat}"[:190]
            unit_size = random.choice(['16oz', '1lb', '12oz', '2lb', '1pk', '6pk', '32oz', '1gal', ''])
            uom = _pick_uom(dept_name)
            is_organic = random.random() < 0.15
            is_local = random.random() < 0.10

            records.append((sku, upc, name, brand, dept_id, cat, subcat,
                             unit_size or None, uom, cost, price,
                             is_organic, is_local, True))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO pos.products
                (sku, upc, name, brand, department_id, category, subcategory,
                 unit_size, unit_of_measure, cost, current_price,
                 is_organic, is_local, is_active)
            VALUES %s ON CONFLICT (sku) DO NOTHING
        """, records)
        conn.commit()
        log.info("Seeded %d products", len(records))
        return _fetch_active_products(cur)


def _pick_uom(dept_name: str) -> str:
    if dept_name in ('Produce', 'Meat & Seafood'):
        return random.choices(['each', 'lb'], weights=[0.4, 0.6])[0]
    return 'each'


def _fetch_active_products(cur) -> List[Dict]:
    cur.execute("""
        SELECT p.product_id, p.sku, p.name, p.category, p.current_price,
               p.unit_of_measure, d.name as dept_name
        FROM pos.products p
        JOIN pos.departments d ON d.department_id = p.department_id
        WHERE p.is_active = TRUE
    """)
    return [
        {
            'product_id':    str(r[0]),
            'sku':           r[1],
            'name':          r[2],
            'category':      r[3],
            'price':         float(r[4]),
            'current_price': float(r[4]),   # alias used by promotions module
            'uom':           r[5],
            'department':    r[6],
            'department_name': r[6],        # alias used by promotions module
        }
        for r in cur.fetchall()
    ]


def fetch_active_products(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_active_products(cur)


def seed_named_coupons(conn, departments: List[Dict]) -> None:
    """Seed recognizable pre-defined coupons. Idempotent by code."""
    dept_by_name = {d['name'].lower(): d['department_id'] for d in departments}
    today = date.today()
    valid_until = today + timedelta(days=365)

    NAMED_COUPONS = [
        ("SAVE5OFF50",  "$5 off any purchase of $50 or more",         "dollar_off", 5.00,  50.00, None,                           None),
        ("PRODUCE10",   "10% off all produce",                         "percent_off", 0.10, None,  dept_by_name.get("produce"),    None),
        ("DAIRY1OFF",   "$1 off any dairy purchase",                   "dollar_off", 1.00,  None,  dept_by_name.get("dairy"),      None),
        ("BAKERY2OFF",  "$2 off bakery items",                         "dollar_off", 2.00,  None,  dept_by_name.get("bakery"),     None),
        ("LOYALTY10",   "10% loyalty member discount",                 "percent_off", 0.10, None,  None,                           None),
        ("MEATDEPT15",  "15% off meat department",                     "percent_off", 0.15, None,  dept_by_name.get("meat"),       None),
        ("DELI5PCT",    "5% off deli items",                           "percent_off", 0.05, None,  dept_by_name.get("deli"),       None),
        ("ORGANIC20",   "20% off organic produce",                     "percent_off", 0.20, None,  dept_by_name.get("produce"),    None),
    ]

    records = []
    for code, desc, ctype, disc, min_purch, dept_id, prod_id in NAMED_COUPONS:
        records.append((code, desc, ctype, disc, min_purch, dept_id, prod_id,
                        None, 0, today, valid_until, True))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO pos.coupons
                (code, description, coupon_type, discount_value, min_purchase,
                 department_id, product_id, max_uses, uses_count,
                 valid_from, valid_until, is_active)
            VALUES %s ON CONFLICT (code) DO NOTHING
        """, records, template="(%s,%s,%s,%s,%s,%s::uuid,%s::uuid,%s,%s,%s,%s,%s)")
    conn.commit()
    log.info("Named coupons seeded (or already present).")


def seed_coupons(conn, cfg: Config, departments: List[Dict], products: List[Dict]) -> List[Dict]:
    """Seed initial batch of active coupons. Idempotent."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pos.coupons WHERE is_active = TRUE")
        if cur.fetchone()[0] >= cfg.coupons.active_at_any_time:
            return _fetch_active_coupons(cur)

    log.info("Seeding coupons...")
    dept_ids = [d['department_id'] for d in departments]
    prod_ids = [p['product_id'] for p in products]
    today = date.today()
    records = []

    for i in range(cfg.coupons.active_at_any_time):
        coupon_type = random.choice(['percent_off', 'percent_off', 'dollar_off', 'bogo'])
        discount = round(random.uniform(0.10, 0.30), 2) if coupon_type == 'percent_off' \
            else round(random.uniform(0.50, 2.00), 2)
        dept_id = random.choice(dept_ids) if random.random() < 0.7 else None
        prod_id = random.choice(prod_ids) if (not dept_id and random.random() < 0.5) else None
        valid_from = today - timedelta(days=random.randint(0, 3))
        valid_until = today + timedelta(days=cfg.coupons.valid_duration_days)
        code = f"FRESH{fake.bothify('??##??').upper()}"
        desc = f"{int(discount * 100)}% off {coupon_type.replace('_', ' ')}" \
            if coupon_type == 'percent_off' else f"${discount:.2f} off"
        records.append((code, desc, coupon_type, discount, None, dept_id, prod_id,
                         None, 0, valid_from, valid_until, True))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO pos.coupons
                (code, description, coupon_type, discount_value, min_purchase,
                 department_id, product_id, max_uses, uses_count,
                 valid_from, valid_until, is_active)
            VALUES %s ON CONFLICT (code) DO NOTHING
        """, records, template="(%s,%s,%s,%s,%s,%s::uuid,%s::uuid,%s,%s,%s,%s,%s)")
        conn.commit()
        return _fetch_active_coupons(cur)


def _fetch_active_coupons(cur) -> List[Dict]:
    today = date.today()
    cur.execute("""
        SELECT coupon_id, coupon_type, discount_value, department_id, product_id
        FROM pos.coupons
        WHERE is_active = TRUE AND valid_from <= %s AND valid_until >= %s
    """, (today, today))
    return [
        {'coupon_id': str(r[0]), 'coupon_type': r[1], 'discount_value': float(r[2]),
         'department_id': str(r[3]) if r[3] else None,
         'product_id': str(r[4]) if r[4] else None}
        for r in cur.fetchall()
    ]


def fetch_active_coupons(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_active_coupons(cur)


def seed_combo_deals(conn, cfg: Config, departments: List[Dict], products: List[Dict]) -> List[Dict]:
    """Seed initial combo deals. Idempotent."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pos.combo_deals WHERE is_active = TRUE")
        if cur.fetchone()[0] >= cfg.combo_deals.active_at_any_time:
            return _fetch_active_deals(cur)

    log.info("Seeding combo deals...")
    dept_ids = [d['department_id'] for d in departments]
    today = date.today()
    records = []

    DEAL_TEMPLATES = [
        ('2 for $5', 'x_for_price', 2, 5.00),
        ('3 for $10', 'x_for_price', 3, 10.00),
        ('Buy 2 Get 1 Free', 'bogo', 2, 0.01),
        ('2 for $3', 'x_for_price', 2, 3.00),
    ]

    for i in range(cfg.combo_deals.active_at_any_time):
        template = DEAL_TEMPLATES[i % len(DEAL_TEMPLATES)]
        name, deal_type, trigger_qty, deal_price = template
        dept_id = random.choice(dept_ids)
        valid_from = today - timedelta(days=random.randint(0, 2))
        valid_until = today + timedelta(days=cfg.combo_deals.valid_duration_days)
        desc = f"{name} on selected items"
        records.append((name, desc, deal_type, trigger_qty, None, dept_id,
                         deal_price, valid_from, valid_until, True))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO pos.combo_deals
                (name, description, deal_type, trigger_qty, trigger_product_id,
                 trigger_department_id, deal_price, valid_from, valid_until, is_active)
            VALUES %s
        """, records, template="(%s,%s,%s,%s,%s::uuid,%s::uuid,%s,%s,%s,%s)")
        conn.commit()
        return _fetch_active_deals(cur)


def _fetch_active_deals(cur) -> List[Dict]:
    today = date.today()
    cur.execute("""
        SELECT deal_id, deal_type, trigger_qty, trigger_product_id,
               trigger_department_id, deal_price
        FROM pos.combo_deals
        WHERE is_active = TRUE AND valid_from <= %s AND valid_until >= %s
    """, (today, today))
    return [
        {'deal_id': str(r[0]), 'deal_type': r[1], 'trigger_qty': r[2],
         'trigger_product_id': str(r[3]) if r[3] else None,
         'trigger_department_id': str(r[4]) if r[4] else None,
         'deal_price': float(r[5])}
        for r in cur.fetchall()
    ]


def fetch_active_deals(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_active_deals(cur)


def fetch_loyalty_members(conn) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT member_id FROM pos.loyalty_members")
        return [{'member_id': str(r[0])} for r in cur.fetchall()]


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
) -> List[Dict]:
    """
    Generate `count` POS transactions. Returns depletion info for inventory.
    """
    if count <= 0 or not store_locations or not products:
        return []

    cashiers = [e for e in employees if e['department'] in
                ('store', 'produce', 'deli', 'bakery', 'meat', 'management')
                and e['location_type'] == 'store']

    # Active promotions by department name
    promo_dept_discount = {dept: disc for dept, disc in scenario.active_promotions}

    txn_records = []
    item_records = []
    new_members = []
    depletion_info = []

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
        cart = random.choices(products, k=num_items)

        txn_id = str(uuid4())
        txn_items = []
        item_recs = []
        subtotal = 0.0

        for product in cart:
            # Quantity: lb-based products get fractional qty
            if product['uom'] == 'lb':
                qty = round(random.uniform(0.2, 3.5), 3)
            else:
                qty = random.choices([1, 2, 3], weights=[78, 17, 5])[0]

            unit_price = product['price']
            discount = 0.0
            coupon_id = None
            deal_id = None

            # Promotional discount
            if product['department'] in promo_dept_discount:
                discount += round(unit_price * promo_dept_discount[product['department']], 2)

            effective_price = max(0.01, unit_price - discount)
            line_total = round(effective_price * qty, 2)
            subtotal += line_total

            item_recs.append((txn_id, product['product_id'], qty,
                               unit_price, discount, coupon_id, deal_id, line_total))
            txn_items.append({'product_id': product['product_id'], 'quantity': qty})

        # Apply a coupon to the whole transaction (loyalty members only)
        coupon_savings = 0.0
        if member_id and coupons and random.random() < cfg.coupons.coupon_use_rate * scenario.coupon_multiplier:
            coupon = random.choice(coupons)
            if coupon['coupon_type'] == 'percent_off':
                coupon_savings = round(subtotal * coupon['discount_value'] * scenario.coupon_multiplier, 2)
            elif coupon['coupon_type'] == 'dollar_off':
                coupon_savings = round(min(subtotal * 0.5, coupon['discount_value'] * scenario.coupon_multiplier), 2)

        # Apply a combo deal
        deal_savings = 0.0
        if deals and random.random() < cfg.combo_deals.combo_use_rate:
            deal = random.choice(deals)
            deal_dept_products = [p for p in cart
                                   if deal['trigger_department_id'] is None
                                   or _dept_id_for_product(p) == deal['trigger_department_id']]
            if len(deal_dept_products) >= deal['trigger_qty']:
                # Saving = sum of trigger_qty items minus deal_price
                trigger_items = sorted(deal_dept_products, key=lambda p: p['price'], reverse=True)[:deal['trigger_qty']]
                original = sum(p['price'] for p in trigger_items)
                deal_savings = max(0.0, round(original - deal['deal_price'], 2))

        subtotal = round(subtotal, 2)
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
        depletion_info.append({'transaction_id': txn_id, 'items': txn_items})

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

    conn.commit()

    # Record loyalty point transactions and update tiers (outside the main insert)
    if txn_records:
        _record_loyalty_points(conn, txn_records)

    return depletion_info


# Tier thresholds: minimum points_balance to reach each tier
TIER_THRESHOLDS = {'bronze': 0, 'silver': 500, 'gold': 2000, 'platinum': 5000}
TIER_ORDER      = ['bronze', 'silver', 'gold', 'platinum']


def _record_loyalty_points(conn, txn_records: list) -> None:
    """
    For each transaction that had a loyalty member, earn 1 point per dollar,
    write a pos.loyalty_point_transactions row, update balance, and upgrade
    tier if threshold crossed.
    """
    # txn_records cols: txn_id[0], location_id[1], employee_id[2], member_id[3],
    #                   ..., total[9], ...
    member_txns = [(r[0], r[3], float(r[9])) for r in txn_records if r[3] is not None]
    if not member_txns:
        return

    pt_records = []
    with conn.cursor() as cur:
        for txn_id, member_id, total in member_txns:
            points_earned = max(0, int(total))
            if points_earned == 0:
                continue

            # Lock the row before reading so concurrent batches can't produce
            # stale reads when the same member appears in multiple transactions.
            cur.execute(
                "SELECT points_balance, tier FROM pos.loyalty_members WHERE member_id = %s::uuid FOR UPDATE",
                (member_id,)
            )
            row = cur.fetchone()
            if not row:
                continue
            current_balance, current_tier = row[0], row[1]
            new_balance = current_balance + points_earned

            # Check tier upgrade
            new_tier = current_tier
            for tier in reversed(TIER_ORDER):
                if new_balance >= TIER_THRESHOLDS[tier]:
                    new_tier = tier
                    break

            cur.execute("""
                UPDATE pos.loyalty_members
                SET points_balance = %s, tier = %s, updated_at = NOW()
                WHERE member_id = %s::uuid
            """, (new_balance, new_tier, member_id))

            pt_records.append((
                member_id, txn_id, points_earned, 0,
                'tier_upgrade' if new_tier != current_tier else 'purchase',
                new_balance,
            ))

    if pt_records:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO pos.loyalty_point_transactions
                    (member_id, transaction_id, points_earned, points_redeemed,
                     reason, balance_after)
                VALUES %s
            """, pt_records,
            template="(%s::uuid,%s::uuid,%s,%s,%s,%s)")
    conn.commit()


def _dept_id_for_product(product: Dict) -> Optional[str]:
    """Products carry dept name, not dept_id — used for deal matching."""
    return None  # deal matching in generator uses dept name, not UUID


# ---------------------------------------------------------------------------
# Price changes
# ---------------------------------------------------------------------------

def maybe_update_product_prices(conn, cfg: Config, products: List[Dict]) -> None:
    """Randomly change a small number of product prices."""
    ticks_per_day = (24 * 60) / 15
    prob = 1.0 / (cfg.pricing.product_price_change_frequency_days * ticks_per_day)
    if random.random() > prob * len(products):
        return

    to_change = random.sample(products, min(5, len(products)))
    with conn.cursor() as cur:
        for p in to_change:
            change_pct = random.uniform(-0.06, 0.08)
            new_price = round(p['price'] * (1 + change_pct), 2)
            new_price = max(0.10, new_price)
            cur.execute("""
                UPDATE pos.products SET current_price = %s, updated_at = NOW()
                WHERE product_id = %s::uuid
            """, (new_price, p['product_id']))
            cur.execute("""
                INSERT INTO pos.price_history (product_id, old_price, new_price)
                VALUES (%s::uuid, %s, %s)
            """, (p['product_id'], p['price'], new_price))
            p['price'] = new_price
    conn.commit()
