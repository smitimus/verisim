"""
HR model — seeds and maintains hr.locations and hr.employees.
HR is the source of truth: all other systems reference these IDs.
"""
import random
import logging
from datetime import date, timedelta
from typing import List, Dict

from faker import Faker
from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)
fake = Faker('en_US')

DEPARTMENTS = ['store', 'store', 'store', 'fuel', 'management']  # weighted toward store
JOB_TITLES = {
    'store':      ['Cashier', 'Sales Associate', 'Shift Supervisor', 'Assistant Manager'],
    'fuel':       ['Fuel Attendant', 'Pump Technician'],
    'management': ['Store Manager', 'District Manager', 'General Manager'],
}
STORE_STATES = ['TX', 'FL', 'GA', 'TN', 'OH', 'IN', 'IL', 'PA', 'NY', 'NC']


def seed_locations(conn, cfg: Config) -> List[Dict]:
    """Create store locations if they don't exist yet. Returns all active locations."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM hr.locations")
        if cur.fetchone()[0] >= cfg.locations.count:
            cur.execute("SELECT location_id, name FROM hr.locations WHERE is_active = TRUE")
            rows = cur.fetchall()
            return [{'location_id': str(r[0]), 'name': r[1]} for r in rows]

        log.info("Seeding %d locations...", cfg.locations.count)
        records = []
        for i in range(cfg.locations.count):
            state = random.choice(STORE_STATES)
            opened = fake.date_between(start_date=date(2010, 1, 1), end_date=date(2022, 12, 31))
            records.append((
                fake.company().replace("'", "''")[:80] + f" #{i+1}",
                fake.street_address(),
                fake.city(),
                state,
                fake.zipcode_in_state(state),
                fake.numerify('(###) ###-####'),
                opened,
                'combo',
                True,
            ))

        execute_values(cur, """
            INSERT INTO hr.locations (name, address, city, state, zip, phone, opened_date, type, is_active)
            VALUES %s
            RETURNING location_id, name
        """, records)
        rows = cur.fetchall()
        conn.commit()
        log.info("Created %d locations", len(rows))
        return [{'location_id': str(r[0]), 'name': r[1]} for r in rows]


def seed_employees(conn, cfg: Config, locations: List[Dict]) -> List[Dict]:
    """Create employees for each location if they don't exist yet. Returns all active employees."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM hr.employees WHERE status = 'active'")
        if cur.fetchone()[0] > 0:
            return _fetch_active_employees(cur)

        log.info("Seeding employees for %d locations...", len(locations))
        records = []
        for loc in locations:
            count = random.randint(cfg.locations.employees_per_location_min,
                                   cfg.locations.employees_per_location_max)
            for _ in range(count):
                dept = random.choice(DEPARTMENTS)
                title = random.choice(JOB_TITLES[dept])
                rate = round(random.uniform(11.0, 28.0), 2)
                hire = fake.date_between(start_date=date(2015, 1, 1), end_date=date.today())
                first, last = fake.first_name(), fake.last_name()
                email = f"{first.lower()}.{last.lower()}{random.randint(1,99)}@example-gasstation.com"
                records.append((
                    loc['location_id'], first, last, email,
                    hire, None, dept, title, rate, 'active',
                ))

        execute_values(cur, """
            INSERT INTO hr.employees
                (location_id, first_name, last_name, email, hire_date,
                 termination_date, department, job_title, hourly_rate, status)
            VALUES %s
            ON CONFLICT (email) DO NOTHING
        """, records)
        conn.commit()

        # Seed pos.employees for all store/management workers
        cur.execute("""
            INSERT INTO pos.employees (employee_id, location_id, pin)
            SELECT e.employee_id, e.location_id,
                   LPAD(FLOOR(RANDOM()*999999)::TEXT, 6, '0')
            FROM hr.employees e
            WHERE e.department IN ('store', 'management')
            ON CONFLICT DO NOTHING
        """)
        conn.commit()
        log.info("Employees seeded")
        return _fetch_active_employees(cur)


def _fetch_active_employees(cur) -> List[Dict]:
    cur.execute("""
        SELECT employee_id, location_id, department, status
        FROM hr.employees WHERE status = 'active'
    """)
    return [
        {'employee_id': str(r[0]), 'location_id': str(r[1]),
         'department': r[2], 'status': r[3]}
        for r in cur.fetchall()
    ]


def fetch_active_employees(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_active_employees(cur)


def maybe_hire_employee(conn, cfg: Config, locations: List[Dict]) -> None:
    """~0.1% chance per tick to hire a new employee."""
    if random.random() > 0.001:
        return
    loc = random.choice(locations)
    dept = random.choice(DEPARTMENTS)
    title = random.choice(JOB_TITLES[dept])
    first, last = fake.first_name(), fake.last_name()
    email = f"{first.lower()}.{last.lower()}{random.randint(100,999)}@example-gasstation.com"
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO hr.employees
                (location_id, first_name, last_name, email, hire_date,
                 department, job_title, hourly_rate, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'active')
            ON CONFLICT (email) DO NOTHING
            RETURNING employee_id, department
        """, (loc['location_id'], first, last, email, date.today(),
              dept, title, round(random.uniform(11.0, 22.0), 2)))
        row = cur.fetchone()
        if row and dept in ('store', 'management'):
            cur.execute("""
                INSERT INTO pos.employees (employee_id, location_id, pin)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
            """, (str(row[0]), loc['location_id'],
                  str(random.randint(100000, 999999)).zfill(6)))
    conn.commit()
    log.debug("Hired new employee at %s", loc['name'])


def maybe_terminate_employee(conn) -> None:
    """~0.02% chance per tick to terminate a random active employee."""
    if random.random() > 0.0002:
        return
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE hr.employees
            SET status = 'terminated',
                termination_date = %s,
                updated_at = NOW()
            WHERE employee_id = (
                SELECT employee_id FROM hr.employees
                WHERE status = 'active'
                ORDER BY RANDOM() LIMIT 1
            )
        """, (date.today(),))
        if cur.rowcount:
            log.debug("Terminated an employee")
    conn.commit()
