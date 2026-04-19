"""
POS model — seeds products/loyalty and generates store transactions.
"""
import random
import logging
import math
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Optional
from uuid import uuid4

from faker import Faker
from psycopg2.extras import execute_values

from config import Config
from scenarios.scenario_engine import ScenarioContext

log = logging.getLogger(__name__)
fake = Faker('en_US')

PAYMENT_METHODS = ['cash', 'credit', 'debit', 'mobile_pay', 'loyalty_points']
PAYMENT_WEIGHTS = [0.15, 0.40, 0.30, 0.10, 0.05]

TIERS = ['bronze', 'silver', 'gold', 'platinum']
TIER_THRESHOLDS = [0, 500, 2000, 5000]  # points to reach each tier


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def seed_products(conn, cfg: Config) -> List[Dict]:
    """Seed ~200 products across configured categories. Idempotent."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pos.products")
        if cur.fetchone()[0] > 0:
            return _fetch_active_products(cur)

        log.info("Seeding products...")
        records = []
        per_cat = max(1, cfg.initial_product_count // len(cfg.product_categories))
        for cat_def in cfg.product_categories:
            cat = cat_def['name']
            subcats = cat_def.get('subcategories', [cat])
            for _ in range(per_cat):
                subcat = random.choice(subcats)
                cost = round(random.uniform(0.25, 15.00), 4)
                price = round(cost * random.uniform(1.3, 2.5), 2)
                sku = f"{cat[:3].upper()}-{fake.bothify('??####').upper()}"
                name = _generate_product_name(cat, subcat)
                records.append((sku, name, cat, subcat, cost, price, True))

        execute_values(cur, """
            INSERT INTO pos.products (sku, name, category, subcategory, cost, current_price, is_active)
            VALUES %s
            ON CONFLICT (sku) DO NOTHING
        """, records)
        conn.commit()
        log.info("Seeded %d products", len(records))
        return _fetch_active_products(cur)


def _fetch_active_products(cur) -> List[Dict]:
    cur.execute("""
        SELECT product_id, sku, name, category, subcategory, current_price
        FROM pos.products WHERE is_active = TRUE
    """)
    return [
        {'product_id': str(r[0]), 'sku': r[1], 'name': r[2],
         'category': r[3], 'subcategory': r[4], 'price': float(r[5])}
        for r in cur.fetchall()
    ]


def fetch_active_products(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_active_products(cur)


def fetch_loyalty_members(conn) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT member_id FROM pos.loyalty_members")
        return [{'member_id': str(r[0])} for r in cur.fetchall()]


def _generate_product_name(category: str, subcategory: str) -> str:
    brand = fake.company().split()[0]
    size_opts = ['12oz', '16oz', '20oz', '1L', '2L', 'Regular', 'Large', 'XL', '1pk', '6pk']
    size = random.choice(size_opts)
    return f"{brand} {subcategory} {size}"[:190]


# ---------------------------------------------------------------------------
# Transaction generation
# ---------------------------------------------------------------------------

def generate_pos_transactions(
    conn,
    cfg: Config,
    simulation_dt: datetime,
    count: int,
    scenario: ScenarioContext,
    locations: List[Dict],
    products: List[Dict],
    employees: List[Dict],
    members: List[Dict],
) -> List[Dict]:
    """
    Generate `count` POS transactions at simulation_dt.
    Returns list of {transaction_id, items: [{product_id, quantity}]} for inventory depletion.
    """
    if count <= 0 or not locations or not products:
        return []

    store_employees = [e for e in employees if e['department'] in ('store', 'management')]
    promo_cats = {cat for cat, _ in scenario.active_promotions}
    promo_discount = {cat: disc for cat, disc in scenario.active_promotions}

    txn_records = []
    item_records = []
    new_members = []
    depletion_info = []

    for _ in range(count):
        loc = random.choice(locations)
        loc_employees = [e for e in store_employees if e['location_id'] == loc['location_id']]
        employee_id = random.choice(loc_employees)['employee_id'] if loc_employees else None

        # Loyalty
        member_id = None
        if members and random.random() < cfg.loyalty.loyalty_usage_rate:
            member_id = random.choice(members)['member_id']

        # Build line items (1-8 items per transaction)
        num_items = random.choices([1, 2, 3, 4, 5, 6, 7, 8],
                                   weights=[30, 25, 18, 12, 7, 4, 2, 2])[0]
        items = random.choices(products, k=num_items)

        subtotal = 0.0
        txn_id = str(uuid4())
        txn_items = []

        for product in items:
            qty = random.choices([1, 2, 3], weights=[80, 15, 5])[0]
            unit_price = product['price']
            discount = 0.0
            if product['category'] in promo_cats:
                discount = round(unit_price * promo_discount[product['category']], 2)
            line_total = round((unit_price - discount) * qty, 2)
            subtotal += line_total
            item_records.append((
                txn_id, product['product_id'], qty,
                unit_price, discount, line_total
            ))
            txn_items.append({'product_id': product['product_id'], 'quantity': qty})

        subtotal = round(subtotal, 2)
        tax = round(subtotal * cfg.pricing.tax_rate, 2)
        total = round(subtotal + tax, 2)

        # Payment method — loyalty_points only if member present
        methods = PAYMENT_METHODS if member_id else PAYMENT_METHODS[:-1]
        weights = PAYMENT_WEIGHTS if member_id else PAYMENT_WEIGHTS[:-1]
        payment = random.choices(methods, weights=weights)[0]

        txn_records.append((
            txn_id, loc['location_id'], employee_id, member_id,
            simulation_dt, subtotal, tax, total, payment, scenario.scenario_tag
        ))
        depletion_info.append({'transaction_id': txn_id, 'items': txn_items})

        # Maybe sign up a new loyalty member
        if random.random() < cfg.loyalty.signup_rate:
            first, last = fake.first_name(), fake.last_name()
            email = f"{first.lower()}.{last.lower()}{random.randint(1,9999)}@email.com"
            new_members.append((first, last, email,
                                fake.numerify('(###) ###-####'), simulation_dt.date(), 0, 'bronze'))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO pos.transactions
                (transaction_id, location_id, employee_id, member_id,
                 transaction_dt, subtotal, tax, total, payment_method, scenario_tag)
            VALUES %s
        """, txn_records, template="(%s::uuid,%s::uuid,%s::uuid,%s::uuid,%s,%s,%s,%s,%s,%s)")

        execute_values(cur, """
            INSERT INTO pos.transaction_items
                (transaction_id, product_id, quantity, unit_price, discount, line_total)
            VALUES %s
        """, item_records, template="(%s::uuid,%s::uuid,%s,%s,%s,%s)")

        if new_members:
            execute_values(cur, """
                INSERT INTO pos.loyalty_members
                    (first_name, last_name, email, phone, signup_date, points_balance, tier)
                VALUES %s ON CONFLICT (email) DO NOTHING
            """, new_members)

        # Award loyalty points (1 point per dollar)
        if members:
            cur.execute("""
                UPDATE pos.loyalty_members lm
                SET points_balance = points_balance + t.total_int,
                    updated_at = NOW()
                FROM (
                    SELECT member_id, SUM(total::INTEGER) AS total_int
                    FROM pos.transactions
                    WHERE transaction_dt = %s AND member_id IS NOT NULL
                    GROUP BY member_id
                ) t
                WHERE lm.member_id = t.member_id
            """, (simulation_dt,))

    conn.commit()
    return depletion_info


# ---------------------------------------------------------------------------
# Price changes
# ---------------------------------------------------------------------------

def maybe_update_product_prices(conn, cfg: Config, products: List[Dict]) -> None:
    """Randomly change a small number of product prices (monthly cadence)."""
    # probability per tick: 1 / (days * 24*60/sim_minutes) ≈ monthly
    ticks_per_day = (24 * 60) / 15  # at default 15 sim-min per tick
    prob_per_tick = 1.0 / (cfg.pricing.product_price_change_frequency_days * ticks_per_day)
    # Change ~1-3 products per event
    if random.random() > prob_per_tick * len(products):
        return

    to_change = random.sample(products, min(3, len(products)))
    with conn.cursor() as cur:
        for p in to_change:
            change_pct = random.uniform(-0.05, 0.10)
            new_price = round(p['price'] * (1 + change_pct), 2)
            new_price = max(0.25, new_price)
            cur.execute("""
                UPDATE pos.products SET current_price = %s, updated_at = NOW()
                WHERE product_id = %s::uuid
            """, (new_price, p['product_id']))
            cur.execute("""
                INSERT INTO pos.price_history (product_id, old_price, new_price)
                VALUES (%s::uuid, %s, %s)
            """, (p['product_id'], p['price'], new_price))
            p['price'] = new_price  # update in-memory cache
    conn.commit()
