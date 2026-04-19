"""
HR model — seeds and maintains hr.locations and hr.employees.
Grocery locations include both stores and warehouses.
HR is the source of truth: all other schemas reference these IDs.
"""
import random
import logging
from datetime import date
from typing import List, Dict

from faker import Faker
from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)
fake = Faker('en_US')

STORE_STATES = ['TX', 'FL', 'GA', 'TN', 'OH', 'IN', 'IL', 'PA', 'NY', 'NC']

STORE_DEPARTMENTS = [
    'store', 'store', 'store', 'store',  # cashiers / floor associates (weighted)
    'produce', 'deli', 'bakery', 'meat',
    'management',
]

WAREHOUSE_DEPARTMENTS = [
    'warehouse', 'warehouse', 'warehouse', 'warehouse',
    'transport',
    'management',
]

JOB_TITLES = {
    'store':      ['Cashier', 'Customer Service', 'Grocery Associate', 'Shift Supervisor'],
    'produce':    ['Produce Clerk', 'Produce Lead', 'Produce Manager'],
    'deli':       ['Deli Clerk', 'Deli Lead', 'Deli Manager'],
    'bakery':     ['Bakery Clerk', 'Baker', 'Bakery Manager'],
    'meat':       ['Meat Cutter', 'Meat Clerk', 'Butcher', 'Meat Manager'],
    'warehouse':  ['Warehouse Associate', 'Picker', 'Receiver', 'Warehouse Lead', 'Forklift Operator'],
    'transport':  ['Driver', 'CDL Driver', 'Driver Lead'],
    'management': ['Assistant Manager', 'Store Manager', 'District Manager', 'Operations Manager'],
}


def seed_locations(conn, cfg: Config) -> Dict[str, List[Dict]]:
    """
    Create store and warehouse locations if they don't exist.
    Returns dict: {'stores': [...], 'warehouses': [...]}
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM hr.locations")
        if cur.fetchone()[0] > 0:
            return _fetch_locations(cur)

        log.info("Seeding %d stores + %d warehouses...",
                 cfg.locations.store_count, cfg.locations.warehouse_count)
        records = []

        for i in range(cfg.locations.store_count):
            state = random.choice(STORE_STATES)
            opened = fake.date_between(start_date=date(2005, 1, 1), end_date=date(2022, 12, 31))
            records.append((
                f"Mega Lo Mart #{i + 1}",
                fake.street_address(), fake.city(), state,
                fake.zipcode_in_state(state), fake.numerify('(###) ###-####'),
                opened, 'store',
                random.randint(25000, 65000),   # store_sqft
                random.randint(12, 24),          # num_aisles
                True,
            ))

        for i in range(cfg.locations.warehouse_count):
            state = random.choice(STORE_STATES)
            opened = fake.date_between(start_date=date(2000, 1, 1), end_date=date(2018, 12, 31))
            records.append((
                f"Mega Lo Mart Distribution Center #{i + 1}",
                fake.street_address(), fake.city(), state,
                fake.zipcode_in_state(state), fake.numerify('(###) ###-####'),
                opened, 'warehouse',
                None, None, True,
            ))

        execute_values(cur, """
            INSERT INTO hr.locations
                (name, address, city, state, zip, phone, opened_date, location_type,
                 store_sqft, num_aisles, is_active)
            VALUES %s
        """, records)
        conn.commit()

    with conn.cursor() as cur:
        return _fetch_locations(cur)


def _fetch_locations(cur) -> Dict[str, List[Dict]]:
    cur.execute("""
        SELECT location_id, name, location_type
        FROM hr.locations WHERE is_active = TRUE
    """)
    stores, warehouses = [], []
    for loc_id, name, loc_type in cur.fetchall():
        entry = {'location_id': str(loc_id), 'name': name, 'location_type': loc_type}
        if loc_type == 'store':
            stores.append(entry)
        else:
            warehouses.append(entry)
    return {'stores': stores, 'warehouses': warehouses}


def fetch_locations(conn) -> Dict[str, List[Dict]]:
    with conn.cursor() as cur:
        return _fetch_locations(cur)


def seed_employees(conn, cfg: Config, locations: Dict[str, List[Dict]]) -> List[Dict]:
    """Create employees for all locations. Returns all active employees."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM hr.employees WHERE status = 'active'")
        if cur.fetchone()[0] > 0:
            return _fetch_active_employees(cur)

    log.info("Seeding employees...")
    records = []

    for loc in locations['stores']:
        count = random.randint(cfg.locations.store_employees_per_location_min,
                               cfg.locations.store_employees_per_location_max)
        for _ in range(count):
            dept = random.choice(STORE_DEPARTMENTS)
            records.append(_build_employee_record(loc['location_id'], dept, 'store'))

    for loc in locations['warehouses']:
        count = random.randint(cfg.locations.warehouse_employees_per_location_min,
                               cfg.locations.warehouse_employees_per_location_max)
        for _ in range(count):
            dept = random.choice(WAREHOUSE_DEPARTMENTS)
            records.append(_build_employee_record(loc['location_id'], dept, 'warehouse'))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO hr.employees
                (location_id, first_name, last_name, email, hire_date,
                 department, job_title, hourly_rate, status)
            VALUES %s
            ON CONFLICT (email) DO NOTHING
        """, records)
        conn.commit()
        log.info("Seeded %d employees", len(records))
        return _fetch_active_employees(cur)


def _build_employee_record(location_id: str, dept: str, location_type: str):
    title = random.choice(JOB_TITLES[dept])
    rate_range = (12.0, 22.0) if location_type == 'store' else (15.0, 28.0)
    rate = round(random.uniform(*rate_range), 2)
    hire = fake.date_between(start_date=date(2010, 1, 1), end_date=date.today())
    first, last = fake.first_name(), fake.last_name()
    suffix = 'megalomart'
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@example-{suffix}.com"
    return (location_id, first, last, email, hire, dept, title, rate, 'active')


def _fetch_active_employees(cur) -> List[Dict]:
    cur.execute("""
        SELECT e.employee_id, e.location_id, e.department, e.status,
               l.location_type
        FROM hr.employees e
        JOIN hr.locations l ON l.location_id = e.location_id
        WHERE e.status = 'active'
    """)
    return [
        {'employee_id': str(r[0]), 'location_id': str(r[1]),
         'department': r[2], 'status': r[3], 'location_type': r[4]}
        for r in cur.fetchall()
    ]


def fetch_active_employees(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_active_employees(cur)


def maybe_hire_employee(conn, cfg: Config, locations: Dict[str, List[Dict]]) -> None:
    """~0.1% chance per tick to hire a new employee."""
    if random.random() > 0.001:
        return
    all_locs = locations['stores'] + locations['warehouses']
    loc = random.choice(all_locs)
    if loc['location_type'] == 'store':
        dept = random.choice(STORE_DEPARTMENTS)
    else:
        dept = random.choice(WAREHOUSE_DEPARTMENTS)
    rec = _build_employee_record(loc['location_id'], dept, loc['location_type'])
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO hr.employees
                (location_id, first_name, last_name, email, hire_date,
                 department, job_title, hourly_rate, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (email) DO NOTHING
        """, rec)
    conn.commit()


def maybe_terminate_employee(conn) -> None:
    """~0.02% chance per tick to terminate a random active employee."""
    if random.random() > 0.0002:
        return
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE hr.employees
            SET status = 'terminated',
                termination_date = NOW()::date,
                updated_at = NOW()
            WHERE employee_id = (
                SELECT employee_id FROM hr.employees
                WHERE status = 'active'
                ORDER BY RANDOM() LIMIT 1
            )
        """)
    conn.commit()
