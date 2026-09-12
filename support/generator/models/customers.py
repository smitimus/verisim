"""
Customers + queue reference data for the support industry.

Queues are the ticket destinations ("movements to different ticket queues");
customers drive repeat-contact realism and survey attachment.
"""
import random
import logging
from datetime import date, timedelta

from faker import Faker
from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)
fake = Faker('en_US')

QUEUES = [
    # code, name, description, sla_hours, avg_resolve_hours, weight key
    ('billing',      'Billing & Payments',   'Invoices, charges, refunds, payment methods',      24,  6.0),
    ('technical',    'Technical Support',    'Product faults, integrations, troubleshooting',    16, 10.0),
    ('account',      'Account Management',   'Sign-in, profile, subscription changes',           24,  5.0),
    ('shipping',     'Order & Shipping',     'Delivery status, tracking, address changes',       36,  7.0),
    ('returns',      'Returns & Exchanges',  'RMA creation, exchange processing, credits',       48, 12.0),
    ('escalations',  'Escalations & Retention', 'VIP issues, legal threats, churn save attempts',  8, 30.0),
]

CATEGORIES = {
    'billing':     ['Duplicate charge', 'Refund not received', 'Payment failed', 'Invoice question', 'Pricing dispute'],
    'technical':   ['App crash', 'Login error', 'Feature not working', 'Sync failure', 'API error', 'Slow performance'],
    'account':     ['Password reset', 'Email change', 'Subscription upgrade', 'Subscription cancel', 'Data export'],
    'shipping':    ['Where is my order', 'Damaged on arrival', 'Wrong item shipped', 'Address correction', 'Delivery reschedule'],
    'returns':     ['Return request', 'Exchange request', 'Store credit question', 'Defective product return'],
    'escalations': ['Threat to churn', 'Legal complaint', 'Social media threat', 'Executive complaint', 'Repeated failure'],
}

SUBJECT_TEMPLATES = {
    'billing':     ["Charged twice for order {n}", "Refund still not showing after {d} days",
                    "Payment method keeps declining — order {n}", "Question about invoice {n}"],
    'technical':   ["App crashes when I open {x}", "Can't log in since the update",
                    "Error 500 on the reports page", "Sync stuck at {p}% for hours"],
    'account':     ["Need to change account email", "Cancel my subscription",
                    "Upgrade plan but not billed", "Request my data export"],
    'shipping':    ["Order {n} stuck in transit", "Received damaged item — order {n}",
                    "Wrong size delivered, order {n}", "Can I change delivery address?"],
    'returns':     ["Return for order {n}", "Exchange request — item {x}",
                    "When do I get my store credit?", "Item defective on arrival"],
    'escalations': ["Third failed attempt — considering legal action",
                    "Long-time customer, done with this service",
                    "Manager requested yesterday, no callback",
                    "Going to post about this if unresolved"],
}


def seed_queues(conn, cfg: Config):
    """Idempotent seed of queues + categories. Returns list of queue dicts."""
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO support.queues (code, name, description, sla_hours, avg_resolve_hours)
            VALUES %s
            ON CONFLICT (code) DO NOTHING
        """, [(c, n, d, s, r) for c, n, d, s, r in QUEUES])
        for code, cats in CATEGORIES.items():
            cur.execute("SELECT queue_id FROM support.queues WHERE code = %s", (code,))
            row = cur.fetchone()
            if row:
                execute_values(cur, """
                    INSERT INTO support.categories (queue_id, name, severity)
                    VALUES %s
                    ON CONFLICT (queue_id, name) DO NOTHING
                """, [(row[0], c, random.choice(['normal', 'normal', 'low', 'high'])) for c in cats])
        conn.commit()

        cur.execute("""
            SELECT queue_id, code, name, sla_hours, avg_resolve_hours
            FROM support.queues WHERE is_active = TRUE ORDER BY code
        """)
        queues = [{'queue_id': str(r[0]), 'code': r[1], 'name': r[2],
                   'sla_hours': r[3], 'avg_resolve_hours': float(r[4])} for r in cur.fetchall()]

        cur.execute("""
            SELECT category_id::text, queue_id::text, name
            FROM support.categories WHERE is_active = TRUE
        """)
        cats = [{'category_id': r[0], 'queue_id': r[1], 'name': r[2]} for r in cur.fetchall()]
        by_q = {}
        for c in cats:
            by_q.setdefault(c['queue_id'], []).append(c)
        for q in queues:
            q['categories'] = by_q.get(q['queue_id'], [])
    return queues


def weighted_queue(cfg: Config):
    codes = list(cfg.queues.ticket_weight.keys())
    weights = [cfg.queues.ticket_weight[c] for c in codes]
    return random.choices(codes, weights=weights, k=1)[0]


def seed_customers(conn, cfg: Config):
    """Seed the customer base. Returns count."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM support.customers")
        if cur.fetchone()[0] > 0:
            cur.execute("SELECT COUNT(*) FROM support.customers")
            return cur.fetchone()[0]

        log.info("Seeding %d customers...", cfg.customers.initial_customer_count)
        records = []
        seen = set()
        for _ in range(cfg.customers.initial_customer_count):
            first, last = fake.first_name(), fake.last_name()
            base = f"{first.lower()}.{last.lower()}"
            email = base
            suffix_n = 0
            while email in seen:
                suffix_n += 1
                email = f"{base}{suffix_n}@example.com"
            seen.add(email)
            signup = fake.date_between(start_date=date(2019, 1, 1), end_date=date.today())
            tier = random.choices(
                ['standard', 'plus', 'premium', 'vip'], weights=[0.62, 0.22, 0.12, 0.04])[0]
            ltv = {'standard': (80, 900), 'plus': (600, 2500),
                   'premium': (1800, 7000), 'vip': (5000, 25000)}[tier]
            records.append((first, last, email, fake.numerify('(###) ###-####') if random.random() < 0.75 else None,
                            tier, signup, round(random.uniform(*ltv), 2)))
        execute_values(cur, """
            INSERT INTO support.customers
                (first_name, last_name, email, phone, tier, signup_date, lifetime_value)
            VALUES %s
            ON CONFLICT (email) DO NOTHING
        """, records)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM support.customers")
        return cur.fetchone()[0]


def fetch_customer_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id::text, tier FROM support.customers")
        return [{'customer_id': r[0], 'tier': r[1]} for r in cur.fetchall()]


def new_customers_today(conn, cfg: Config, sim_dt):
    """Add a handful of new customers each simulated day (backfill-safe)."""
    count = random.randint(cfg.customers.new_customer_daily_min,
                           cfg.customers.new_customer_daily_max)
    records = []
    for _ in range(count):
        first, last = fake.first_name(), fake.last_name()
        email = fake.email()
        tier = random.choices(['standard', 'plus', 'premium', 'vip'],
                              weights=[0.66, 0.20, 0.11, 0.03])[0]
        ltv = {'standard': (20, 400), 'plus': (100, 900),
               'premium': (400, 2200), 'vip': (1000, 8000)}[tier]
        records.append((first, last, email,
                        fake.numerify('(###) ###-####') if random.random() < 0.7 else None,
                        tier, sim_dt.date(), round(random.uniform(*ltv), 2)))
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO support.customers
                (first_name, last_name, email, phone, tier, signup_date, lifetime_value)
            VALUES %s
            ON CONFLICT (email) DO NOTHING
        """, records)
    conn.commit()
    return count


def subject_for(queue_code):
    tpl = random.choice(SUBJECT_TEMPLATES[queue_code])
    return tpl.format(n=random.randint(100000, 999999), d=random.randint(3, 21),
                      x=random.choice(['checkout', 'the dashboard', 'photo upload', 'search']),
                      p=random.randint(20, 99))
