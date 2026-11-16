"""
POS model — seeds departments, products, coupons, combo deals, loyalty members;
generates store transactions with coupon/deal application.
"""
import os
import random
import logging
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Optional, Tuple
from uuid import uuid4

from faker import Faker
from psycopg2.extras import execute_values

from config import Config
from scenarios.scenario_engine import ScenarioContext
from elasticity import (
    choose_products,
    demand_weight,
    sample_price_paths,
    seed_elasticity_columns,
)

log = logging.getLogger(__name__)
fake = Faker('en_US')


def read_schema_sql() -> str:
    """The generator's own `schema.sql`, for tests that check the DDL.

    Read lazily rather than at import: a model module should not do file I/O
    as a side effect of being imported, and a test that needs the DDL should
    fail loudly when it is missing rather than get an empty string.
    """
    path = os.path.join(os.path.dirname(__file__), '..', 'schema.sql')
    with open(path, 'r') as f:
        return f.read()

PAYMENT_METHODS = ['cash', 'credit', 'debit', 'ebt', 'mobile_pay', 'loyalty_points']
PAYMENT_WEIGHTS = [0.12, 0.38, 0.28, 0.08, 0.10, 0.04]

TIERS = ['bronze', 'silver', 'gold', 'platinum']

# Fallback horizon for promotion validity windows when the caller does not
# pass one in. Keep in sync with GeneratorConfig.backfill_lookback_days.
DEFAULT_PROMO_HISTORY_DAYS = 30


# ---------------------------------------------------------------------------
# Promotion validity windows
# ---------------------------------------------------------------------------

def _as_date(value) -> Optional[date]:
    """Coerce a DATE (or an ISO string from JSON) to a `date`, else None."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _promo_applies_on(promo: Dict, on_date: date) -> bool:
    """True when `on_date` falls inside the promotion's own validity window.

    Promotions are only applied to — and therefore only ever tagged on —
    transactions whose date is inside `[valid_from, valid_until]`. That is the
    contract data-lab asserts (`assert_coupon_dates_valid` /
    `assert_deal_dates_valid`: every item carrying a promo id must satisfy
    `transaction_dt between valid_from and valid_until`).

    A promo with no resolvable window is treated as *not* applicable: the
    window is the contract, so an unknown one fails closed rather than
    silently writing a violating row.
    """
    valid_from = _as_date(promo.get('valid_from'))
    valid_until = _as_date(promo.get('valid_until'))
    if valid_from is None or valid_until is None:
        return False
    return valid_from <= on_date <= valid_until


def _promo_history_start(cur, history_days: int) -> date:
    """Earliest date a promotion seeded *now* has to be valid from.

    The generator back-dates POS transactions across the backfill horizon
    (`main.auto_backfill_if_fresh` / `main.run_backfill`), so a promo row
    created at the end of that horizon must not start after the oldest
    transaction that can carry its id. Take the earlier of the configured
    horizon and the oldest transaction already on disk, so a database older
    than the horizon still gets a window that covers its own history.
    """
    today = date.today()
    horizon = today - timedelta(days=max(0, int(history_days)))
    cur.execute("SELECT min(transaction_dt)::date FROM pos.transactions")
    row = cur.fetchone()
    earliest = row[0] if row else None
    earliest = _as_date(earliest)
    if earliest is not None and earliest < horizon:
        return earliest
    return horizon


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
            # Every SKU starts AT its reference price, so a fresh catalogue
            # has no price signal of its own — the signal arrives from the
            # seeded price_history walk and from weekly ads, both measured
            # against this reference (t_08deeddf).
            reference_price = price
            price_elasticity = _draw_elasticity(cfg)
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
                             reference_price, price_elasticity,
                             is_organic, is_local, True))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO pos.products
                (sku, upc, name, brand, department_id, category, subcategory,
                 unit_size, unit_of_measure, cost, current_price,
                 reference_price, price_elasticity,
                 is_organic, is_local, is_active)
            VALUES %s ON CONFLICT (sku) DO NOTHING
        """, records)
        conn.commit()
        log.info("Seeded %d products", len(records))
        return _fetch_active_products(cur)


def _draw_elasticity(cfg: Config) -> float:
    """One SKU's own price sensitivity, drawn around the configured default.

    Real catalogues are not homogeneous — a staple's volume barely notices a
    price rise while a discretionary treat's collapses — so the spread is a
    configured fraction of the default (`pricing.elasticity_jitter`), not a
    hardcoded constant. A jitter of 0 gives every SKU the default exactly.
    """
    default = cfg.pricing.default_price_elasticity
    jitter = max(0.0, cfg.pricing.elasticity_jitter)
    if not jitter:
        return round(default, 3)
    return round(default * (1.0 + random.uniform(-jitter, jitter)), 3)


def _pick_uom(dept_name: str) -> str:
    if dept_name in ('Produce', 'Meat & Seafood'):
        return random.choices(['each', 'lb'], weights=[0.4, 0.6])[0]
    return 'each'


def _fetch_active_products(cur) -> List[Dict]:
    cur.execute("""
        SELECT p.product_id, p.sku, p.name, p.category, p.current_price,
               p.unit_of_measure, d.department_id, d.name as dept_name,
               p.reference_price, p.price_elasticity
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
            'department_id': str(r[6]),
            'department':    r[7],
            'department_name': r[7],        # alias used by promotions module
            # The elasticity curve's pivot and slope (t_08deeddf). A NULL
            # here is a product row seeded before the columns existed;
            # `elasticity.demand_weight` resolves it to the live price and
            # the configured default rather than failing.
            'reference_price': float(r[8]) if r[8] is not None else None,
            'price_elasticity': float(r[9]) if r[9] is not None else None,
        }
        for r in cur.fetchall()
    ]


def fetch_active_products(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_active_products(cur)


def seed_named_coupons(conn, departments: List[Dict],
                       history_days: int = DEFAULT_PROMO_HISTORY_DAYS) -> None:
    """Seed recognizable pre-defined coupons. Idempotent by code."""
    dept_by_name = {d['name'].lower(): d['department_id'] for d in departments}
    today = date.today()
    valid_until = today + timedelta(days=365)
    with conn.cursor() as cur:
        # Back-date the window to the backfill horizon: these coupons are what
        # the back-dated transactions reference, so a window starting "today"
        # leaves every one of those items outside it (card t_01b4fe4f).
        valid_from = _promo_history_start(cur, history_days)

    NAMED_COUPONS = [
        # code, description, type, discount, min purchase, dept, product, max_uses
        #
        # `max_uses` is the DENOMINATOR of
        # `mart_promotion_redemption.redemption_rate_pct`
        # (uses_count / max_uses). It used to be NULL on every coupon, so the
        # rate came out 100 % NULL even where the redemption count was real
        # (t_74280120). Caps sit above the volume a coupon accrues in a normal
        # data dir (~1000 redemptions over 90 days, measured on dev), so the
        # published rate stays a percentage rather than a multiple of 100.
        ("SAVE5OFF50",  "$5 off any purchase of $50 or more",         "dollar_off", 5.00,  50.00, None,                           None, 5000),
        ("PRODUCE10",   "10% off all produce",                         "percent_off", 0.10, None,  dept_by_name.get("produce"),    None, 3000),
        ("DAIRY1OFF",   "$1 off any dairy purchase",                   "dollar_off", 1.00,  None,  dept_by_name.get("dairy"),      None, 3000),
        ("BAKERY2OFF",  "$2 off bakery items",                         "dollar_off", 2.00,  None,  dept_by_name.get("bakery"),     None, 2500),
        ("LOYALTY10",   "10% loyalty member discount",                 "percent_off", 0.10, None,  None,                           None, 4000),
        ("MEATDEPT15",  "15% off meat department",                     "percent_off", 0.15, None,  dept_by_name.get("meat"),       None, 2500),
        ("DELI5PCT",    "5% off deli items",                           "percent_off", 0.05, None,  dept_by_name.get("deli"),       None, 2000),
        ("ORGANIC20",   "20% off organic produce",                     "percent_off", 0.20, None,  dept_by_name.get("produce"),    None, 2000),
    ]

    records = []
    for code, desc, ctype, disc, min_purch, dept_id, prod_id, max_uses in NAMED_COUPONS:
        records.append((code, desc, ctype, disc, min_purch, dept_id, prod_id,
                        max_uses, 0, valid_from, valid_until, True))

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


def backfill_coupon_max_uses(conn, cfg: Config) -> int:
    """Give every coupon that has no usage cap one, and return how many changed.

    `pos.coupons.max_uses` is the denominator of data-lab's
    `mart_promotion_redemption.redemption_rate_pct` (`uses_count / max_uses`).
    Every coupon this generator has ever made carried NULL there, so that mart
    published 100 % NULL even on rows whose redemption count was real — the
    second, independent reason for the defect t_74280120 fixes. Reseeding alone
    would not heal an existing data dir (the rows already exist and the API
    keeps serving them), so the repair is an idempotent UPDATE that runs on
    every seed pass — boot and every simulated midnight.

    The draw is spread per row (`random()`), so the catalogue does not end up
    with one suspiciously uniform cap. `max_uses_min`/`max_uses_max` come from
    the config, never from a literal here.
    """
    low = int(cfg.coupons.max_uses_min)
    high = max(low, int(cfg.coupons.max_uses_max))
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pos.coupons "
            "SET max_uses = %s + floor(random() * (%s - %s + 1))::int "
            "WHERE max_uses IS NULL",
            (low, high, low),
        )
        repaired = cur.rowcount
    conn.commit()
    if repaired:
        log.info("Coupon max_uses repaired on %d legacy rows (range %d-%d)",
                 repaired, low, high)
    return repaired


def seed_coupons(conn, cfg: Config, departments: List[Dict], products: List[Dict],
                 history_days: int = DEFAULT_PROMO_HISTORY_DAYS) -> List[Dict]:
    """Seed active coupons. Idempotent; deactivates expired coupons and tops
    the active set back up to `active_at_any_time`."""
    # The redemption-rate denominator, before anything else this pass does:
    # a coupon seeded before t_74280120 has no cap, and the mart cannot divide
    # by it whatever its uses_count says. Idempotent (only NULL rows).
    backfill_coupon_max_uses(conn, cfg)
    with conn.cursor() as cur:
        # Deactivate coupons whose validity window has passed so the guard
        # below counts only currently-valid coupons (expired-but-active rows
        # used to block re-seeding forever — see combo_deals incident 2026-08).
        cur.execute(
            "UPDATE pos.coupons SET is_active = FALSE "
            "WHERE is_active = TRUE AND valid_until < CURRENT_DATE"
        )
        conn.commit()
        cur.execute(
            "SELECT COUNT(*) FROM pos.coupons "
            "WHERE is_active = TRUE AND valid_until >= CURRENT_DATE"
        )
        active = cur.fetchone()[0]
        if active >= cfg.coupons.active_at_any_time:
            return _fetch_active_coupons(cur)

    log.info("Seeding coupons (%d short of %d)...",
             cfg.coupons.active_at_any_time - active, cfg.coupons.active_at_any_time)
    dept_ids = [d['department_id'] for d in departments]
    prod_ids = [p['product_id'] for p in products]
    today = date.today()
    with conn.cursor() as cur:
        # Same horizon rule as seed_named_coupons: a freshly created coupon is
        # referenced by the back-dated transactions of the whole horizon, so
        # its window has to start where that history starts.
        horizon = _promo_history_start(cur, history_days)
    records = []

    for i in range(cfg.coupons.active_at_any_time - active):
        coupon_type = random.choice(['percent_off', 'percent_off', 'dollar_off', 'bogo'])
        discount = round(random.uniform(0.10, 0.30), 2) if coupon_type == 'percent_off' \
            else round(random.uniform(0.50, 2.00), 2)
        dept_id = random.choice(dept_ids) if random.random() < 0.7 else None
        prod_id = random.choice(prod_ids) if (not dept_id and random.random() < 0.5) else None
        # Spread the start a little (never later than the horizon).
        valid_from = horizon - timedelta(days=random.randint(0, 3))
        valid_until = today + timedelta(days=cfg.coupons.valid_duration_days)
        code = f"FRESH{fake.bothify('??##??').upper()}"
        desc = f"{int(discount * 100)}% off {coupon_type.replace('_', ' ')}" \
            if coupon_type == 'percent_off' else f"${discount:.2f} off"
        # The usage cap the redemption-rate mart divides by (t_74280120).
        max_uses = random.randint(int(cfg.coupons.max_uses_min),
                                  max(int(cfg.coupons.max_uses_min),
                                      int(cfg.coupons.max_uses_max)))
        records.append((code, desc, coupon_type, discount, None, dept_id, prod_id,
                         max_uses, 0, valid_from, valid_until, True))

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
        SELECT coupon_id, coupon_type, discount_value, department_id, product_id,
               valid_from, valid_until
        FROM pos.coupons
        WHERE is_active = TRUE AND valid_from <= %s AND valid_until >= %s
    """, (today, today))
    return [
        {'coupon_id': str(r[0]), 'coupon_type': r[1], 'discount_value': float(r[2]),
         'department_id': str(r[3]) if r[3] else None,
         'product_id': str(r[4]) if r[4] else None,
         'valid_from': r[5], 'valid_until': r[6]}
        for r in cur.fetchall()
    ]


def fetch_active_coupons(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_active_coupons(cur)


def seed_combo_deals(conn, cfg: Config, departments: List[Dict], products: List[Dict],
                     history_days: int = DEFAULT_PROMO_HISTORY_DAYS) -> List[Dict]:
    """Seed combo deals. Idempotent; deactivates expired deals and tops the
    active set back up to `active_at_any_time`."""
    with conn.cursor() as cur:
        # Deactivate expired deals (keeps the API + seed guard aligned with
        # CURRENT_DATE — previously expired-but-active rows stayed is_active
        # forever and blocked re-seeding: freshness STALE on combo_deals).
        cur.execute(
            "UPDATE pos.combo_deals SET is_active = FALSE "
            "WHERE is_active = TRUE AND valid_until < CURRENT_DATE"
        )
        conn.commit()
        cur.execute(
            "SELECT COUNT(*) FROM pos.combo_deals "
            "WHERE is_active = TRUE AND valid_until >= CURRENT_DATE"
        )
        active = cur.fetchone()[0]
        if active >= cfg.combo_deals.active_at_any_time:
            return _fetch_active_deals(cur)

    log.info("Seeding combo deals (%d short of %d)...",
             cfg.combo_deals.active_at_any_time - active, cfg.combo_deals.active_at_any_time)
    dept_ids = [d['department_id'] for d in departments]
    today = date.today()
    with conn.cursor() as cur:
        # Same horizon rule as the coupon seeds — see seed_named_coupons.
        horizon = _promo_history_start(cur, history_days)
    records = []

    DEAL_TEMPLATES = [
        ('2 for $5', 'x_for_price', 2, 5.00),
        ('3 for $10', 'x_for_price', 3, 10.00),
        ('Buy 2 Get 1 Free', 'bogo', 2, 0.01),
        ('2 for $3', 'x_for_price', 2, 3.00),
    ]

    for i in range(cfg.combo_deals.active_at_any_time - active):
        template = DEAL_TEMPLATES[i % len(DEAL_TEMPLATES)]
        name, deal_type, trigger_qty, deal_price = template
        dept_id = random.choice(dept_ids)
        valid_from = horizon - timedelta(days=random.randint(0, 2))
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
               trigger_department_id, deal_price, valid_from, valid_until
        FROM pos.combo_deals
        WHERE is_active = TRUE AND valid_from <= %s AND valid_until >= %s
    """, (today, today))
    return [
        {'deal_id': str(r[0]), 'deal_type': r[1], 'trigger_qty': r[2],
         'trigger_product_id': str(r[3]) if r[3] else None,
         'trigger_department_id': str(r[4]) if r[4] else None,
         'deal_price': float(r[5]),
         'valid_from': r[6], 'valid_until': r[7]}
        for r in cur.fetchall()
    ]


def fetch_active_deals(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_active_deals(cur)


# ---------------------------------------------------------------------------
# The price of record (t_08deeddf)
# ---------------------------------------------------------------------------

def price_of_record(products: List[Dict], ad_prices: Dict[str, float]) -> Dict[str, float]:
    """Map product_id -> the price a shopper faces today.

    For a weekly-ad item that is `promoted_price`; for everything else it is
    `current_price`. The demand curve is keyed on this, so a discount works
    through the same law as any other price move instead of needing a special
    case — and the price actually charged is recorded in
    `transaction_items.unit_price` as before, so the line's own money
    (`line_total = quantity * unit_price - discount`, t_0a72c6a5) is untouched.
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


# ---------------------------------------------------------------------------
# Discount money, in cents (t_0a72c6a5)
# ---------------------------------------------------------------------------
# `pos.transaction_items.discount` is a LINE-LEVEL dollar amount and
# `line_total = quantity * unit_price - discount`. Every allocation below works
# in integer cents: a coupon/deal saving has to be split across the lines that
# earned it and the parts have to add back up to the header exactly, and float
# dollars do not add up.

def _cents(amount) -> int:
    """Money as integer cents — every dollar figure in this module is 2-dp."""
    return int(Decimal(str(amount)).quantize(Decimal('0.01'),
                                             rounding=ROUND_HALF_UP) * 100)


def _line_gross_cents(qty, unit_price) -> int:
    """`quantity * unit_price` in cents, rounded the way NUMERIC rounds it.

    A weighed line can land exactly on a half cent: 2.06 lb x $7.75 is $15.965,
    which the till — and every SQL consumer — calls $15.97, while Python's
    banker's `round()` on an inexact float says $15.96. Postgres rounds a
    NUMERIC half away from zero, so the generator has to do its line arithmetic
    in Decimal to keep `line_total = quantity * unit_price - discount` exactly
    true (39 lines of a 10-day backfill disagreed before this).
    """
    gross = Decimal(str(qty)) * Decimal(str(unit_price))
    return int(gross.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) * 100)


def _line_room_cents(item_recs: List[List], indices: List[int]) -> int:
    """Cents still discountable on the given lines (gross minus what they carry).

    A promo may never take a line below zero, so "room" is the honest ceiling
    for an allocation.
    """
    room = 0
    for i in indices:
        rec = item_recs[i]
        room += max(0, _line_gross_cents(rec[2], rec[3]) - _cents(rec[4]))
    return room


def _split_cents(total_c: int, weights: List[int]) -> List[int]:
    """Split `total_c` cents across `weights` so the parts sum EXACTLY.

    Largest-remainder (Hamilton) apportionment: floor each exact share, then
    give the leftover cents to the largest fractional remainders. No part ever
    exceeds its own weight, which is what keeps a line's discount inside the
    value of the line.
    """
    shares = [0] * len(weights)
    live = [(w, i) for i, w in enumerate(weights) if w > 0]
    if total_c <= 0 or not live:
        return shares
    denom = sum(w for w, _ in live)
    remainders = []
    for weight, i in live:
        exact = total_c * weight / denom
        shares[i] = int(exact)
        remainders.append((exact - shares[i], i))
    leftover = total_c - sum(shares)
    for _, i in sorted(remainders, key=lambda pair: (-pair[0], pair[1]))[:leftover]:
        shares[i] += 1
    return shares


def _apply_line_discount(item_recs: List[List], indices: List[int], amount,
                         coupon_id: Optional[str] = None,
                         deal_id: Optional[str] = None) -> float:
    """Move `amount` of transaction-level saving onto the lines that earned it.

    Writes the share into `discount`, keeps `line_total = quantity * unit_price
    - discount` true for the line, and stamps the promo id on the same rows —
    so a line that references a coupon or deal always carries a non-zero
    `discount` and the references are never decorative.

    Pro-rata is by remaining *room*, not by unit price: a weighed or multi-unit
    line has to absorb its own share of the money. Returns the amount actually
    applied (cents-exact); the caller books that figure on the transaction
    header, which is what makes
    `sum(discount) = coupon_savings + deal_savings` hold for every transaction.
    """
    rooms = [_line_room_cents(item_recs, [i]) for i in indices]
    total_room = sum(rooms)
    applied_c = min(_cents(amount), total_room)
    if applied_c <= 0:
        return 0.0
    for i, share in zip(indices, _split_cents(applied_c, rooms)):
        rec = item_recs[i]
        if share <= 0:
            # No money landed on this line, so it must not claim the promo: a
            # reference with a zero discount is exactly the decoration this card
            # removes (and ROWS the acceptance criterion forbids).
            continue
        rec[4] = (_cents(rec[4]) + share) / 100.0
        if coupon_id:
            rec[5] = coupon_id
        if deal_id:
            rec[6] = deal_id
        rec[7] = (_line_gross_cents(rec[2], rec[3]) - _cents(rec[4])) / 100.0
    return applied_c / 100.0


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


def reconcile_promotions(conn) -> Dict[str, int]:
    """Make the promotion rows agree with the history that references them.

    Two derived facts are recomputed from `pos.transaction_items` — the only
    place a redemption is ever written:

    * `pos.coupons.uses_count` — one redemption per coupon-tagged transaction.
      The write path (`generate_pos_transactions`) increments it as it goes;
      this pass recomputes the *absolute* value from `pos.transaction_items`,
      which is the source of truth — so a regenerated backfill day or a
      deleted item cannot leave a drifting counter behind. Before t_01b4fe4f
      nothing ever wrote it, so every coupon read 0 uses while tens of
      thousands of line items referenced it.
    * `valid_from` / `valid_until` — widened to cover usage that already falls
      outside the window. Seeding only stamps the window from *now* forward,
      so a database generated before the windowing fix keeps its out-of-window
      redemptions. This repairs those rows in place instead of requiring a
      wipe. It is idempotent: the generator no longer tags outside the window
      (`_promo_applies_on`), so after the first pass there is nothing to widen.

    Cheap enough for a per-simulated-day lifecycle step; commits its own work
    and returns the row counts touched, for logging.
    """
    touched = {'coupons': 0, 'deals': 0, 'coupons_zeroed': 0}
    with conn.cursor() as cur:
        # Coupons: window + redemption count in one aggregate pass.
        cur.execute("""
            WITH usage AS (
                SELECT ti.coupon_id,
                       count(DISTINCT ti.transaction_id) AS redemptions,
                       min(t.transaction_dt)::date AS min_used,
                       max(t.transaction_dt)::date AS max_used
                FROM pos.transaction_items ti
                JOIN pos.transactions t ON t.transaction_id = ti.transaction_id
                WHERE ti.coupon_id IS NOT NULL
                GROUP BY ti.coupon_id
            )
            UPDATE pos.coupons c
               SET valid_from  = LEAST(c.valid_from, u.min_used),
                   valid_until = GREATEST(c.valid_until, u.max_used),
                   uses_count  = u.redemptions
              FROM usage u
             WHERE u.coupon_id = c.coupon_id
               AND (c.valid_from > u.min_used
                    OR c.valid_until < u.max_used
                    OR c.uses_count IS DISTINCT FROM u.redemptions)
        """)
        touched['coupons'] = cur.rowcount

        # Combo deals have no counter column — windows only.
        cur.execute("""
            WITH usage AS (
                SELECT ti.deal_id,
                       min(t.transaction_dt)::date AS min_used,
                       max(t.transaction_dt)::date AS max_used
                FROM pos.transaction_items ti
                JOIN pos.transactions t ON t.transaction_id = ti.transaction_id
                WHERE ti.deal_id IS NOT NULL
                GROUP BY ti.deal_id
            )
            UPDATE pos.combo_deals d
               SET valid_from  = LEAST(d.valid_from, u.min_used),
                   valid_until = GREATEST(d.valid_until, u.max_used)
              FROM usage u
             WHERE u.deal_id = d.deal_id
               AND (d.valid_from > u.min_used OR d.valid_until < u.max_used)
        """)
        touched['deals'] = cur.rowcount

        # A coupon whose redemptions were removed (API item delete) must not
        # keep a stale count. Bounded by the distinct coupon ids in usage.
        cur.execute("""
            WITH used AS (
                SELECT DISTINCT coupon_id FROM pos.transaction_items
                WHERE coupon_id IS NOT NULL
            )
            UPDATE pos.coupons c
               SET uses_count = 0
             WHERE c.uses_count <> 0
               AND NOT EXISTS (SELECT 1 FROM used u WHERE u.coupon_id = c.coupon_id)
        """)
        touched['coupons_zeroed'] = cur.rowcount
    conn.commit()
    if any(touched.values()):
        log.info("Promotion reconcile: %d coupons, %d deals, %d counters zeroed",
                 touched['coupons'], touched['deals'], touched['coupons_zeroed'])
    return touched


def seed_loyalty_members(conn, cfg: Config) -> List[Dict]:
    """
    Seed initial loyalty members with varied tiers and matching bonus point
    transactions. Idempotent — skips if members already exist.
    Returns the list of newly-created members.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pos.loyalty_members")
        if cur.fetchone()[0] > 0:
            return _fetch_loyalty_members(cur)

    log.info("Seeding %d initial loyalty members...", cfg.loyalty.initial_member_count)
    today = date.today()
    TIER_DIST = [('bronze', 0, 0.40), ('silver', 500, 0.35),
                 ('gold', 2000, 0.17), ('platinum', 5000, 0.08)]

    member_records = []
    pt_records = []

    for i in range(cfg.loyalty.initial_member_count):
        first = fake.first_name()
        last = fake.last_name()
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 99999)}@email.com"
        phone = fake.numerify('(###) ###-####')
        signup_date = today - timedelta(days=random.randint(1, 180))

        tier_name, tier_min, _ = random.choices(
            TIER_DIST,
            weights=[w for _, _, w in TIER_DIST],
            k=1
        )[0]
        # Starting balance includes a welcome bonus plus some random points
        bonus_points = random.randint(0, 300)
        points_balance = tier_min + bonus_points

        member_id = str(uuid4())
        member_records.append(
            (member_id, first, last, email, phone, signup_date, points_balance, tier_name))

        if points_balance > 0:
            pt_records.append((
                member_id, None, points_balance, 0, 'bonus', points_balance
            ))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO pos.loyalty_members
                (member_id, first_name, last_name, email, phone,
                 signup_date, points_balance, tier)
            VALUES %s
        """, member_records,
        template="(%s::uuid,%s,%s,%s,%s,%s,%s,%s)")

        if pt_records:
            execute_values(cur, """
                INSERT INTO pos.loyalty_point_transactions
                    (member_id, transaction_id, points_earned, points_redeemed,
                     reason, balance_after)
                VALUES %s
            """, pt_records,
            template="(%s::uuid,%s::uuid,%s,%s,%s,%s)")
        conn.commit()

    log.info("Seeded %d loyalty members with %d bonus point transactions",
             len(member_records), len(pt_records))
    return fetch_loyalty_members(conn)


def _fetch_loyalty_members(cur) -> List[Dict]:
    cur.execute("SELECT member_id FROM pos.loyalty_members")
    return [{'member_id': str(r[0])} for r in cur.fetchall()]


def fetch_loyalty_members(conn) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT member_id FROM pos.loyalty_members")
        return [{'member_id': str(r[0])} for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Price history seeding (for price-elasticity analysis)
# ---------------------------------------------------------------------------

def seed_price_history(conn, cfg: Config, products: List[Dict]) -> None:
    """Seed N days of historical price_history so fresh databases have full
    coverage for elasticity analysis (no enforced retention cap exists; the
    generator simply appends, so this is about dataset age, not pruning).

    Idempotent: skips if any price_history row is older than the configured
    window, so re-seeding a fresh DB is safe and an already-aged DB is left
    untouched. Rows are stamped with explicit historical `changed_at` values
    (the column defaults to NOW(), which would erase the signal). The walk
    ends at each product's current `current_price` so realtime continues
    smoothly from the seeded history.
    """
    if not products:
        return

    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pos.price_history "
            "WHERE changed_at < NOW() - (%s * INTERVAL '1 day') LIMIT 1",
            (cfg.pricing.price_history_backfill_days,),
        )
        if cur.fetchone():
            log.info("price_history already has historical coverage; skipping seed.")
            return

    log.info("Seeding %d days of price_history for %d products...",
             cfg.pricing.price_history_backfill_days, len(products))
    today = date.today()
    n_days = cfg.pricing.price_history_backfill_days
    steps = 12  # ~12 price points spread across the window

    # The walk itself lives in `elasticity.sample_price_paths` so the history
    # written here and the curve that reads it cannot drift apart. What
    # changed at t_08deeddf is the shape of the walk, not its destination: it
    # is still `price_history` for elasticity analysis, but it now carries a
    # store-wide market factor, so a product's own price move is separable
    # from the market's. Under the old independent-per-product walk the two
    # were the same thing, which is exactly why the elasticity downstream came
    # out coincidental.
    skus = [(str(p['product_id']), float(p['price'])) for p in products]
    records = []
    for point in sample_price_paths(skus, n_days, steps, cfg):
        ts = datetime(today.year, today.month, today.day) - timedelta(days=point['days_ago'])
        records.append((point['product_id'], point['old_price'], point['price'], ts))

    if records:
        # NOTE: the read cursor from the idempotency check above is closed
        # once its `with` block exits, so use a fresh cursor for the bulk insert.
        with conn.cursor() as ins_cur:
            execute_values(ins_cur, """
                INSERT INTO pos.price_history (product_id, old_price, new_price, changed_at)
                VALUES %s
            """, records, template="(%s::uuid,%s,%s,%s)")
        conn.commit()
        log.info("Seeded %d price_history rows", len(records))


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

        # The line is where the money has to be (t_0a72c6a5).
        #
        # `transaction_items.discount` is a LINE-LEVEL dollar amount and
        # `line_total = quantity * unit_price - discount`. Before this card the
        # only thing that ever reached it was a scenario department promotion,
        # so a normal backfill wrote 0 on every one of 7.7 M rows while the
        # header carried $5.1 M of coupon/deal savings — a permanently-zero
        # money column that four marts published as if it were a measurement.
        #
        # `transactions.subtotal` keeps its old meaning: the line net BEFORE the
        # transaction-level coupon/deal, i.e.
        #   subtotal = sum(line_total) + coupon_savings + deal_savings
        # so data-lab's `total = subtotal + tax - coupon/deal_savings` is
        # untouched. The gross a line was sold at is `quantity * unit_price`.
        subtotal_c = 0
        for line in demand_lines:
            qty = line['quantity']
            unit_price = line['unit_price']
            gross_c = _line_gross_cents(qty, unit_price)
            discount_c = 0
            promo_pct = promo_dept_discount.get(line['department'])
            if promo_pct:
                # A department promotion the scenario switched on. A line keeps
                # at least one cent, as the old per-unit floor did.
                discount_c = min(
                    int((Decimal(gross_c) * Decimal(str(promo_pct)))
                        .quantize(Decimal('1'), rounding=ROUND_HALF_UP)),
                    max(0, gross_c - 1))
            net_c = gross_c - discount_c
            subtotal_c += net_c
            item_recs.append([txn_id, line['product_id'], qty, unit_price,
                              discount_c / 100.0, None, None, net_c / 100.0])
        subtotal = subtotal_c / 100.0

        # Apply a coupon to the whole transaction (loyalty members only).
        # Scope is resolved FIRST: a "10% off produce" coupon rung on a basket
        # with no produce is not a redemption, and a saving that cannot reach a
        # line is exactly the unattributable money this card removes.
        coupon_savings = 0.0
        coupon = None
        coupon_scope: List[int] = []
        loyalty_eng = getattr(scenario, 'loyalty_engagement_modifier', 1.0)
        if (member_id and active_coupons and random.random() <
                cfg.coupons.coupon_use_rate * scenario.coupon_multiplier * loyalty_eng):
            coupon = random.choice(active_coupons)
            coupon_scope = _promo_line_indices(cart, coupon, 'coupon')
            scoped_c = _line_room_cents(item_recs, coupon_scope)
            if scoped_c > 0:
                # The saving is computed against the coupon's OWN scope, not the
                # whole basket: a percentage off produce must not discount the
                # beer, and pro-rating a basket-wide figure onto the produce
                # lines would write a discount larger than its own terms.
                scoped_value = scoped_c / 100.0
                if coupon['coupon_type'] == 'percent_off':
                    coupon_savings = round(
                        scoped_value * coupon['discount_value'] * scenario.coupon_multiplier, 2)
                elif coupon['coupon_type'] == 'dollar_off':
                    coupon_savings = round(
                        min(scoped_value, coupon['discount_value'] * scenario.coupon_multiplier), 2)
            if coupon_savings < 0.01:
                # A redemption worth nothing is not recorded. A `bogo` coupon
                # carries no line amount, and a cents-level saving would leave a
                # coupon-tagged line at discount = 0 — the defect this card is
                # about.
                coupon, coupon_savings, coupon_scope = None, 0.0, []

        # Apply a combo deal
        deal_savings = 0.0
        deal = None
        deal_scope: List[int] = []
        if active_deals and random.random() < cfg.combo_deals.combo_use_rate:
            candidate = random.choice(active_deals)
            scope = _promo_line_indices(cart, candidate, 'deal')
            if len(scope) >= candidate['trigger_qty']:
                # Saving = sum of trigger_qty items minus deal_price. Priced
                # off the price OF RECORD (t_08deeddf): comparing a deal
                # against the pre-ad shelf price would book a saving the
                # customer never received, and `mart_promotion_effectiveness`
                # would credit the deal for the weekly ad's discount.
                trigger = sorted(scope,
                                 key=lambda i: _price_of(cart[i], price_record),
                                 reverse=True)[:candidate['trigger_qty']]
                original = sum(_price_of(cart[i], price_record) for i in trigger)
                # Never book more than the lines it applies to can absorb.
                saving = min(max(0.0, round(original - candidate['deal_price'], 2)),
                             _line_room_cents(item_recs, scope) / 100.0)
                if saving >= 0.01:
                    deal, deal_savings, deal_scope = candidate, round(saving, 2), scope

        # Coupons and combo deals are computed independently and can STACK
        # past the subtotal (seen 2026-08-24/25 in prod: 2 rush_hour txns with
        # coupon+deal > subtotal). Floor the combined discounts so the pre-tax
        # total is never negative: coupons take precedence, deal savings are
        # reduced to whatever room remains. Previously the max(0.01, ...)
        # clamp below silently absorbed this, which broke dbt's
        # assert_e2e_revenue_consistency formula check.
        if coupon_savings > subtotal - 0.01:
            coupon_savings = max(0.0, round(subtotal - 0.01, 2))
            if coupon_savings < 0.01:
                coupon, coupon_savings, coupon_scope = None, 0.0, []
        max_deal_savings = round(subtotal - coupon_savings - 0.01, 2)
        if deal_savings > max_deal_savings:
            deal_savings = max(0.0, max_deal_savings)
            if deal_savings < 0.01:
                deal, deal_savings, deal_scope = None, 0.0, []

        # Put the promo money on the lines, and book on the header exactly what
        # landed — `_apply_line_discount` returns the amount it could place, so
        # `sum(discount) = coupon_savings + deal_savings` (to the cent) for
        # every transaction, and `sum(line_total) = subtotal - coupon - deal`.
        if coupon is not None and coupon_savings:
            coupon_savings = _apply_line_discount(
                item_recs, coupon_scope, coupon_savings,
                coupon_id=coupon['coupon_id'])
            if coupon_savings <= 0.0:
                coupon = None
        if deal is not None and deal_savings:
            deal_savings = _apply_line_discount(
                item_recs, deal_scope, deal_savings, deal_id=deal['deal_id'])
            if deal_savings <= 0.0:
                deal = None

        # Only a redemption that reached a line item counts as a use — which is
        # now every redemption the header records, since the money cannot be
        # booked without a line to carry it.
        if coupon is not None:
            cid = coupon['coupon_id']
            coupon_uses[cid] = coupon_uses.get(cid, 0) + 1

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
        _record_loyalty_points(conn, txn_records, cfg, scenario)

    return depletion_info


# ---------------------------------------------------------------------------
# Loyalty points: earning, redeeming, tiers
# ---------------------------------------------------------------------------

# Tier thresholds: minimum points_balance to reach each tier
TIER_THRESHOLDS = {'bronze': 0, 'silver': 500, 'gold': 2000, 'platinum': 5000}
TIER_ORDER      = ['bronze', 'silver', 'gold', 'platinum']

# A member spends points at 100 points = $1 of basket. A program law, like
# TIER_THRESHOLDS, rather than an operator preference — which is why it lives
# here and not in config.yaml. It is half of what bounds a redemption: nobody
# pays for a $4 basket with 5,000 points.
REDEMPTION_POINTS_PER_DOLLAR = 100
# Redemptions are drawn in whole 100-point increments; a fraction of a point
# redemption is a ledger row no real program writes.
REDEMPTION_POINT_STEP = 100


def _draw_redemption(cfg: Config, scenario, transaction_total: float,
                     available: int) -> int:
    """Points this ticket spends, or 0 for the vast majority of them.

    Bounded three ways, and the bound that matters is the balance:

    * `loyalty.redemption_rate` × the scenario's `loyalty_redemption_modifier`
      — a few percent of member tickets by default, and the lever the
      `redemption_rush` scenario pulls for the A/B harness;
    * never more than `available`, the member's balance at this exact point in
      the ledger, so `balance_after` can never go negative;
    * never more than the ticket is worth (100 points = $1) and never more
      than `loyalty.redemption_points_max`.

    Returns 0 when the balance is below `loyalty.redemption_points_min`: a
    member who cannot afford the smallest redemption simply pays as usual.
    """
    floor = max(1, int(cfg.loyalty.redemption_points_min))
    if available < floor:
        return 0
    rate = (float(cfg.loyalty.redemption_rate)
            * float(getattr(scenario, 'loyalty_redemption_modifier', 1.0) or 1.0))
    if rate <= 0 or random.random() >= rate:
        return 0
    affordable = min(
        int(available),
        int(transaction_total * REDEMPTION_POINTS_PER_DOLLAR),
        int(cfg.loyalty.redemption_points_max),
    )
    steps = affordable // REDEMPTION_POINT_STEP
    if steps < 1:
        return 0
    floor_steps = max(1, floor // REDEMPTION_POINT_STEP)
    return random.randint(min(floor_steps, steps), steps) * REDEMPTION_POINT_STEP


def _record_loyalty_points(conn, txn_records: list, cfg: Config,
                           scenario=None) -> None:
    """
    For each transaction that had a loyalty member, earn 1 point per dollar and,
    for `loyalty.redemption_rate` of them, spend points at the till — one
    `pos.loyalty_point_transactions` row per event. Updates the member's
    points_balance and tier, and computes balance_after as a running sum from
    committed point_transactions (not from the inflated cumulative member
    balance).

    Why both directions exist (t_74280120): the ledger only ever CREDITED —
    534,250 earn rows, zero redemption rows, `sum(points_redeemed) = 0` — so
    `int_loyalty_enriched.points_redeemed`,
    `mart_loyalty_rfm.lifetime_points_redeemed` and both
    redemption-rate marts published 0 or NULL. A points program that never
    pays out is not a program.

    This prevents backfill from writing inflated balance_after values when
    realtime has already processed chronologically-later transactions.
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

            # Lock the member row so we can update points_balance + tier safely.
            # Note: points_balance is the global cumulative total (includes
            # future realtime transactions) — we do NOT use it for balance_after.
            cur.execute(
                "SELECT points_balance, tier FROM pos.loyalty_members WHERE member_id = %s::uuid FOR UPDATE",
                (member_id,)
            )
            row = cur.fetchone()
            if not row:
                continue
            current_balance, current_tier = row[0], row[1]

            # Compute balance_after as running sum from committed transactions
            # plus pending points from the current batch — NOT from the
            # cumulative member balance (which includes future realtime points).
            cur.execute("""
                SELECT COALESCE(SUM(points_earned - points_redeemed)::INT, 0)
                FROM pos.loyalty_point_transactions
                WHERE member_id = %s::uuid
            """, (member_id,))
            committed = cur.fetchone()[0]
            # Add pending points from earlier items in this batch for this member
            pending = sum(p[2] - p[3] for p in pt_records if p[0] == member_id)
            available = int(committed + pending)

            # Spend points FIRST, then credit the sale. The shopper pays with
            # points at the till and the purchase credits points afterwards, so
            # the ledger's balance_after chain rises across the pair. That order
            # is load-bearing: data-lab's `assert_loyalty_points_balance` orders
            # same-instant rows by balance_after, and an earn-then-redeem pair
            # reads as a chain break because the redeem row lands lower.
            redeemed = _draw_redemption(cfg, scenario, total, available)
            if redeemed:
                pt_records.append((
                    member_id, txn_id, 0, redeemed, 'redemption',
                    available - redeemed,
                ))

            # A redemption is part of the member's balance too. `max(0, ...)`
            # guards a legacy data dir whose member row lags its ledger; the
            # ledger (`available`) is what bounds the redemption, so the chain
            # on disk can never go negative.
            new_balance = max(0, current_balance + points_earned - redeemed)

            # Check tier upgrade. A redemption must never DEMOTE a member: a
            # program that takes points back does not take the tier back, and
            # re-deriving from the lower balance would do exactly that.
            new_tier = current_tier
            for tier in reversed(TIER_ORDER):
                if new_balance >= TIER_THRESHOLDS[tier]:
                    new_tier = tier
                    break
            if TIER_THRESHOLDS.get(new_tier, 0) <= TIER_THRESHOLDS.get(current_tier, 0):
                new_tier = current_tier

            cur.execute("""
                UPDATE pos.loyalty_members
                SET points_balance = %s, tier = %s, updated_at = NOW()
                WHERE member_id = %s::uuid
            """, (new_balance, new_tier, member_id))

            balance_after = available + points_earned - redeemed

            pt_records.append((
                member_id, txn_id, points_earned, 0,
                'tier_upgrade' if new_tier != current_tier else 'purchase',
                balance_after,
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


def _promo_line_indices(cart: List[Dict], promo: Dict, kind: str) -> List[int]:
    """Indices (into `cart`, hence into `item_recs`) of the lines a promo covers.

    Mirrors the scope rules used when computing the savings: a product-scoped
    promo covers only that product's lines; a department-scoped promo covers
    every line in that department; a promo with no scope covers the basket.
    Returns positions, not product ids, because the same product can appear on
    two lines and each of them has to carry its own share of the money.
    """
    if kind == 'coupon':
        pid = promo.get('product_id')
        did = promo.get('department_id')
    else:
        pid = promo.get('trigger_product_id')
        did = promo.get('trigger_department_id')
    if pid:
        return [i for i, product in enumerate(cart) if product['product_id'] == pid]
    if did:
        return [i for i, product in enumerate(cart)
                if product.get('department_id') == did]
    return list(range(len(cart)))


# ---------------------------------------------------------------------------
# Price changes
# ---------------------------------------------------------------------------

def maybe_update_product_prices(conn, cfg: Config, products: List[Dict], scenario=None) -> None:
    """Randomly change a small number of product prices.

    When scenario.price_modifier != 1.0, price change magnitudes are scaled
    (e.g. inflation_pressure=1.15 → 15% larger price swings upward).
    """
    ticks_per_day = (24 * 60) / 15
    prob = 1.0 / (cfg.pricing.product_price_change_frequency_days * ticks_per_day)
    if random.random() > prob * len(products):
        return

    price_mod = 1.0
    if scenario is not None:
        price_mod = getattr(scenario, 'price_modifier', 1.0)

    to_change = random.sample(products, min(5, len(products)))
    with conn.cursor() as cur:
        for p in to_change:
            change_pct = random.uniform(-0.06, 0.08) * price_mod
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
