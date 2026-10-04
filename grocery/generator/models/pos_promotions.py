"""Coupons and combo deals — the promotion catalogue and its repair pass.

Owns `pos.coupons` / `pos.combo_deals`: the validity-window helpers the seeds
and the transaction writer share, the seeds themselves, the fetches, and
`reconcile_promotions` (the per-day pass that re-derives uses_count and the
validity windows from recorded redemptions).

Split out of `models/pos.py` (t_c2eca5dd); `pos.py` re-exports every public name.
"""
import logging
import random
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from faker import Faker
from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)
fake = Faker('en_US')


# ---------------------------------------------------------------------------
# history-horizon default
# ---------------------------------------------------------------------------

# Fallback horizon for promotion validity windows when the caller does not
# pass one in. Keep in sync with GeneratorConfig.backfill_lookback_days.
DEFAULT_PROMO_HISTORY_DAYS = 30

# ---------------------------------------------------------------------------
# promotion validity windows
# ---------------------------------------------------------------------------

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
# coupon seeds + fetch
# ---------------------------------------------------------------------------

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
    log.info("Named coupons seeded (or already present).")


def seed_coupons(conn, cfg: Config, departments: List[Dict], products: List[Dict],
                 history_days: int = DEFAULT_PROMO_HISTORY_DAYS) -> List[Dict]:
    """Seed active coupons. Idempotent; deactivates expired coupons and tops
    the active set back up to `active_at_any_time`."""
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


# ---------------------------------------------------------------------------
# combo-deal seeds + fetch
# ---------------------------------------------------------------------------

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
# reconcile_promotions
# ---------------------------------------------------------------------------

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
