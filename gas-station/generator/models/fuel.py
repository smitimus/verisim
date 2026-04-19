"""
Fuel model — seeds pumps and generates fuel dispensing transactions.
"""
import random
import logging
from datetime import datetime
from typing import List, Dict
from uuid import uuid4

from faker import Faker
from psycopg2.extras import execute_values

from config import Config
from scenarios.scenario_engine import ScenarioContext

log = logging.getLogger(__name__)
fake = Faker('en_US')

FUEL_PAYMENT_METHODS = ['pay_at_pump', 'credit', 'debit', 'cash', 'mobile_pay', 'loyalty_points']
FUEL_PAYMENT_WEIGHTS = [0.40, 0.25, 0.20, 0.08, 0.05, 0.02]


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def seed_pumps(conn, cfg: Config, locations: List[Dict]) -> List[Dict]:
    """Create pumps for each location. Idempotent."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fuel.pumps")
        if cur.fetchone()[0] > 0:
            return _fetch_active_pumps(cur)

        log.info("Seeding fuel pumps...")
        records = []
        for loc in locations:
            num_pumps = random.randint(cfg.locations.pumps_per_location_min,
                                       cfg.locations.pumps_per_location_max)
            for pump_num in range(1, num_pumps + 1):
                records.append((loc['location_id'], pump_num, 2, True))

        execute_values(cur, """
            INSERT INTO fuel.pumps (location_id, pump_number, num_sides, is_active)
            VALUES %s
            ON CONFLICT (location_id, pump_number) DO NOTHING
        """, records)
        conn.commit()
        log.info("Seeded %d pumps across %d locations", len(records), len(locations))
        return _fetch_active_pumps(cur)


def _fetch_active_pumps(cur) -> List[Dict]:
    cur.execute("""
        SELECT pump_id, location_id, pump_number
        FROM fuel.pumps WHERE is_active = TRUE
    """)
    return [
        {'pump_id': str(r[0]), 'location_id': str(r[1]), 'pump_number': r[2]}
        for r in cur.fetchall()
    ]


def fetch_active_pumps(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_active_pumps(cur)


def fetch_fuel_grades(conn) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT grade_id, name, current_price
            FROM fuel.grades WHERE is_active = TRUE
        """)
        return [
            {'grade_id': str(r[0]), 'name': r[1], 'price': float(r[2])}
            for r in cur.fetchall()
        ]


# ---------------------------------------------------------------------------
# Transaction generation
# ---------------------------------------------------------------------------

def generate_fuel_transactions(
    conn,
    cfg: Config,
    simulation_dt: datetime,
    count: int,
    scenario: ScenarioContext,
    locations: List[Dict],
    pumps: List[Dict],
    grades: List[Dict],
    employees: List[Dict],
    members: List[Dict],
) -> None:
    """Generate `count` fuel transactions at simulation_dt."""
    if count <= 0 or not pumps or not grades:
        return

    # Apply fuel price modifier from scenario (e.g. fuel_spike)
    effective_grades = [
        {**g, 'price': round(g['price'] * scenario.fuel_price_modifier, 4)}
        for g in grades
    ]

    store_employees = [e for e in employees if e['department'] in ('store', 'management')]
    records = []

    for _ in range(count):
        pump = random.choice(pumps)

        # Grade selection: Regular is most common
        grade = random.choices(
            effective_grades,
            weights=[0.55, 0.20, 0.15, 0.10][:len(effective_grades)]
        )[0]

        # Gallons: realistic distribution for a fill-up
        gallons = round(random.gauss(mu=10.0, sigma=5.0), 4)
        gallons = max(1.0, min(30.0, gallons))

        price_per_gallon = grade['price']
        total_amount = round(gallons * price_per_gallon, 2)

        # Pay at pump = no employee; pre-pay inside = cashier assigned
        payment = random.choices(FUEL_PAYMENT_METHODS, weights=FUEL_PAYMENT_WEIGHTS)[0]
        if payment == 'pay_at_pump':
            employee_id = None
        else:
            loc_emps = [e for e in store_employees if e['location_id'] == pump['location_id']]
            employee_id = random.choice(loc_emps)['employee_id'] if loc_emps else None

        member_id = None
        if members and random.random() < 0.15:
            member_id = random.choice(members)['member_id']

        records.append((
            str(uuid4()), pump['pump_id'], pump['location_id'],
            employee_id, member_id, simulation_dt,
            grade['grade_id'], round(gallons, 4), price_per_gallon, total_amount,
            payment, scenario.scenario_tag
        ))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fuel.transactions
                (transaction_id, pump_id, location_id, employee_id, member_id,
                 transaction_dt, grade_id, gallons, price_per_gallon, total_amount,
                 payment_method, scenario_tag)
            VALUES %s
        """, records,
        template="(%s::uuid,%s::uuid,%s::uuid,%s::uuid,%s::uuid,%s,%s::uuid,%s,%s,%s,%s,%s)")
    conn.commit()


# ---------------------------------------------------------------------------
# Price changes
# ---------------------------------------------------------------------------

def maybe_change_fuel_price(conn, cfg: Config, grades: List[Dict]) -> None:
    """
    Probabilistically change fuel prices based on configured frequency.
    All grades change together (as in real life).
    """
    ticks_per_day = (24 * 60) / 15
    prob_per_tick = 1.0 / (cfg.pricing.fuel_price_change_frequency_days * ticks_per_day)
    if random.random() > prob_per_tick:
        return

    max_pct = cfg.pricing.fuel_price_change_pct_max
    change_pct = random.uniform(-max_pct, max_pct)
    log.info("Fuel price change: %+.1f%%", change_pct * 100)

    with conn.cursor() as cur:
        for grade in grades:
            new_price = round(grade['price'] * (1 + change_pct), 4)
            new_price = max(1.999, min(8.999, new_price))  # reasonable bounds
            cur.execute("""
                INSERT INTO fuel.price_history (grade_id, old_price, new_price)
                VALUES (%s::uuid, %s, %s)
            """, (grade['grade_id'], grade['price'], new_price))
            cur.execute("""
                UPDATE fuel.grades
                SET current_price = %s, updated_at = NOW()
                WHERE grade_id = %s::uuid
            """, (new_price, grade['grade_id']))
            grade['price'] = new_price  # update in-memory cache
    conn.commit()
