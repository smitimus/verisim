"""Product catalogue and price movement — the reference data every tick reads.

Owns `pos.departments` / `pos.products` / `pos.price_history`: the seed that
builds the catalogue, the fetches that refresh it, the `price_history` walk that
gives price-elasticity analysis a signal to read, and the per-tick price moves.

Split out of `models/pos.py` (t_c2eca5dd). `pos.py` re-exports every public
name here, so `from models import pos` callers and the tests are unaffected.
"""
import logging
import random
from datetime import date, datetime, timedelta
from typing import Dict, List

from faker import Faker
from psycopg2.extras import execute_values

from config import Config
from elasticity import sample_price_paths

log = logging.getLogger(__name__)
fake = Faker('en_US')


# ---------------------------------------------------------------------------
# departments + products
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

# ---------------------------------------------------------------------------
# price_history seed
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
# price changes
# ---------------------------------------------------------------------------

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
