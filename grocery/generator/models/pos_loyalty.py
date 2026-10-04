"""Loyalty members, points and tiers.

Owns `pos.loyalty_members` / `pos.loyalty_point_transactions`: the member seed
with its welcome-bonus point rows, the member fetch, and `_record_loyalty_points`
— the per-batch earn/tier step that runs right after the transaction write.

Split out of `models/pos.py` (t_c2eca5dd); `pos.py` re-exports every public name.
"""
import logging
import random
from datetime import date, timedelta
from typing import Dict, List
from uuid import uuid4

from faker import Faker
from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)
fake = Faker('en_US')


# ---------------------------------------------------------------------------
# tier names
# ---------------------------------------------------------------------------

TIERS = ['bronze', 'silver', 'gold', 'platinum']

# ---------------------------------------------------------------------------
# member seed + fetch
# ---------------------------------------------------------------------------

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
# tier thresholds
# ---------------------------------------------------------------------------

# Tier thresholds: minimum points_balance to reach each tier
TIER_THRESHOLDS = {'bronze': 0, 'silver': 500, 'gold': 2000, 'platinum': 5000}
TIER_ORDER      = ['bronze', 'silver', 'gold', 'platinum']

# ---------------------------------------------------------------------------
# _record_loyalty_points
# ---------------------------------------------------------------------------

def _record_loyalty_points(conn, txn_records: list) -> None:
    """
    For each transaction that had a loyalty member, earn 1 point per dollar,
    write a pos.loyalty_point_transactions row, update member points_balance
    and tier, and compute balance_after as a running sum from committed
    point_transactions (not from the inflated cumulative member balance).

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
            balance_after = committed + pending + points_earned

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
