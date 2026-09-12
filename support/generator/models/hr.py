"""
HR model — contact-center locations and agents.
Source of truth for people: queues, tickets, calls, chats, surveys all
reference hr.employees. Agents carry skill_groups (queue codes they can
take) and an aht_factor (individual handle-time multiplier).
"""
import random
import logging
from datetime import date

from faker import Faker
from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)
fake = Faker('en_US')

CENTER_STATES = ['TX', 'FL', 'GA', 'TN', 'OH', 'IN', 'IL', 'PA', 'NY', 'NC']

DEPARTMENT_WEIGHTS = [
    'agent', 'agent', 'agent', 'agent', 'agent', 'agent',  # weighted majority
    'team_lead', 'qa', 'training', 'management',
]

JOB_TITLES = {
    'agent':      ['Support Agent', 'Senior Support Agent', 'Technical Support Agent',
                   'Billing Specialist'],
    'team_lead':  ['Team Lead', 'Shift Lead'],
    'qa':         ['Quality Analyst', 'QA Coach'],
    'training':   ['Training Coordinator', 'Instructional Designer'],
    'management': ['Contact Center Manager', 'Operations Manager', 'VP Customer Care'],
}

RATE_RANGE = {
    'agent': (16.0, 26.0),
    'team_lead': (24.0, 34.0),
    'qa': (26.0, 36.0),
    'training': (28.0, 38.0),
    'management': (38.0, 65.0),
}


def seed_locations(conn, cfg: Config):
    """Create contact centers + satellite sites. Returns {'centers': [...], 'all': [...]}"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM hr.locations")
        if cur.fetchone()[0] > 0:
            return _fetch_locations(cur)

        log.info("Seeding %d contact centers + %d satellites...",
                 cfg.locations.contact_center_count, cfg.locations.satellite_count)
        records = []
        for i in range(cfg.locations.contact_center_count):
            state = random.choice(CENTER_STATES)
            records.append((
                f"CarePoint Contact Center #{i + 1}",
                fake.street_address(), fake.city(), state,
                fake.zipcode_in_state(state), fake.numerify('(###) ###-####'),
                fake.date_between(start_date=date(2012, 1, 1), end_date=date(2021, 12, 31)),
                'contact_center', random.randint(60, 180),
            ))
        for i in range(cfg.locations.satellite_count):
            state = random.choice(CENTER_STATES)
            records.append((
                f"CarePoint Remote Hub #{i + 1}",
                fake.street_address(), fake.city(), state,
                fake.zipcode_in_state(state), fake.numerify('(###) ###-####'),
                fake.date_between(start_date=date(2018, 1, 1), end_date=date(2024, 12, 31)),
                'satellite', random.randint(10, 40),
            ))
        execute_values(cur, """
            INSERT INTO hr.locations
                (name, address, city, state, zip, phone, opened_date,
                 location_type, seat_capacity)
            VALUES %s
        """, records)
        conn.commit()

    with conn.cursor() as cur:
        return _fetch_locations(cur)


def _fetch_locations(cur):
    cur.execute("SELECT location_id, name, location_type FROM hr.locations WHERE is_active = TRUE")
    all_locs, centers = [], []
    for loc_id, name, ltype in cur.fetchall():
        entry = {'location_id': str(loc_id), 'name': name, 'location_type': ltype}
        all_locs.append(entry)
        if ltype == 'contact_center':
            centers.append(entry)
    return {'centers': centers, 'all': all_locs}


def fetch_locations(conn):
    with conn.cursor() as cur:
        return _fetch_locations(cur)


def seed_employees(conn, cfg: Config, locations, queues):
    """Seed agents + support staff at every site. queue_codes = skill pool."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM hr.employees WHERE status = 'active'")
        if cur.fetchone()[0] > 0:
            return _fetch_active_employees(cur)

    log.info("Seeding employees...")
    queue_codes = [q['code'] for q in queues]
    records = []
    for loc in locations['all']:
        count = random.randint(cfg.locations.agents_per_location_min,
                               cfg.locations.agents_per_location_max)
        for idx in range(count):
            # Guarantee one manager per contact center
            dept = 'management' if (idx == 0 and loc['location_type'] == 'contact_center') \
                else random.choice(DEPARTMENT_WEIGHTS)
            records.append(_build_employee_record(loc['location_id'], dept, queue_codes))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO hr.employees
                (location_id, first_name, last_name, email, hire_date, department,
                 job_title, skill_groups, aht_factor, hourly_rate, status)
            VALUES %s
            ON CONFLICT (email) DO NOTHING
        """, records)
        conn.commit()
        log.info("Seeded %d employees", len(records))
        return _fetch_active_employees(cur)


def _build_employee_record(location_id, dept, queue_codes):
    title = random.choice(JOB_TITLES[dept])
    rate = round(random.uniform(*RATE_RANGE[dept]), 2)
    hire = fake.date_between(start_date=date(2015, 1, 1), end_date=date.today())
    first, last = fake.first_name(), fake.last_name()
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@example-carepoint.com"
    if dept == 'agent':
        n_skills = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
        skills = ','.join(random.sample(queue_codes, min(n_skills, len(queue_codes))))
        aht = round(random.uniform(0.80, 1.35), 2)
    else:
        skills = ','.join(queue_codes) if dept in ('team_lead', 'qa', 'management') else ''
        aht = 1.0
    return (location_id, first, last, email, hire, dept, title, skills, aht, rate, 'active')


def _fetch_active_employees(cur):
    cur.execute("""
        SELECT employee_id, location_id, department, skill_groups, aht_factor, status
        FROM hr.employees WHERE status = 'active'
    """)
    return [
        {'employee_id': str(r[0]), 'location_id': str(r[1]), 'department': r[2],
         'skill_groups': (r[3] or '').split(',') if r[3] else [],
         'aht_factor': float(r[4]), 'status': r[5]}
        for r in cur.fetchall()
    ]


def fetch_active_employees(conn):
    with conn.cursor() as cur:
        return _fetch_active_employees(cur)


def maybe_hire_employee(conn, cfg: Config, locations, queue_codes):
    """~0.15% chance per tick to hire a new agent (realtime only)."""
    if random.random() > 0.0015:
        return
    loc = random.choice(locations['all'])
    dept = random.choice(DEPARTMENT_WEIGHTS)
    rec = _build_employee_record(loc['location_id'], dept, queue_codes)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO hr.employees
                (location_id, first_name, last_name, email, hire_date, department,
                 job_title, skill_groups, aht_factor, hourly_rate, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (email) DO NOTHING
        """, rec)
    conn.commit()


def maybe_terminate_employee(conn):
    """~0.03% chance per tick to terminate one active agent (never the last one)."""
    if random.random() > 0.0003:
        return
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE hr.employees
            SET status = 'terminated', termination_date = NOW()::date, updated_at = NOW()
            WHERE employee_id = (
                SELECT employee_id FROM hr.employees
                WHERE status = 'active' AND department = 'agent'
                  AND (SELECT COUNT(*) FROM hr.employees WHERE status='active'
                       AND department='agent') > 4
                ORDER BY RANDOM() LIMIT 1
            )
        """)
    conn.commit()
