"""
Promotions model — manages 7-day weekly ad cycles.

Each Monday a new weekly ad is created covering Mon–Sun.
Featured products get a promoted_price and is_on_ad = TRUE.
Expired ad products have is_on_ad reset to FALSE.

The weekly ad product list is returned so POS generation can weight
ad items more heavily in transaction selection.
"""
import random
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# Departments featured in weekly ads and how many items they contribute
AD_DEPT_CONFIG = {
    'Produce':          (3, 5),
    'Meat & Seafood':   (2, 4),
    'Dairy & Eggs':     (2, 3),
    'Bakery':           (1, 3),
    'Frozen Foods':     (2, 4),
    'Snacks & Candy':   (2, 4),
    'Beverages':        (2, 3),
    'Canned & Dry Goods': (1, 3),
}

# Discount tiers for weekly ad items
DISCOUNT_TIERS = [0.10, 0.15, 0.20, 0.25, 0.30, 0.33]


def _week_start(d: date) -> date:
    """Return the Monday of the week containing date d."""
    return d - timedelta(days=d.weekday())


def ensure_current_ad(conn, sim_date: date,
                      products: List[Dict]) -> List[Tuple[str, float]]:
    """
    Ensures a weekly ad exists for the week containing sim_date.
    Creates one if missing. Returns list of (product_id, discount_pct)
    for all active ad items this week.
    """
    week_start = _week_start(sim_date)
    week_end   = week_start + timedelta(days=6)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT ad_id FROM pricing.weekly_ads
            WHERE start_date = %s AND end_date = %s
            LIMIT 1
        """, (week_start, week_end))
        row = cur.fetchone()

    if row:
        # Ad already exists — fetch items
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ai.product_id::text, ai.discount_pct
                FROM pricing.ad_items ai
                WHERE ai.ad_id = %s
            """, (row[0],))
            return [(r[0], float(r[1])) for r in cur.fetchall()]

    # Build product index by department name
    dept_products: Dict[str, List[Dict]] = {}
    for p in products:
        dept_products.setdefault(p.get('department_name', ''), []).append(p)

    ad_items = []  # (ad_id_placeholder, product_id, promoted_price, discount_pct)
    for dept_name, (min_count, max_count) in AD_DEPT_CONFIG.items():
        pool = dept_products.get(dept_name, [])
        if not pool:
            continue
        count  = random.randint(min_count, min(max_count, len(pool)))
        chosen = random.sample(pool, count)
        for p in chosen:
            discount = random.choice(DISCOUNT_TIERS)
            promoted = round(float(p['current_price']) * (1 - discount), 2)
            ad_items.append((p['product_id'], promoted, round(discount * 100, 1)))

    if not ad_items:
        return []

    ad_name = f"Weekly Ad {week_start.strftime('%b %d')}–{week_end.strftime('%b %d, %Y')}"

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pricing.weekly_ads (ad_name, start_date, end_date)
            VALUES (%s, %s, %s) RETURNING ad_id
        """, (ad_name, week_start, week_end))
        ad_id = cur.fetchone()[0]

        execute_values(cur, """
            INSERT INTO pricing.ad_items (ad_id, product_id, promoted_price, discount_pct)
            VALUES %s ON CONFLICT (ad_id, product_id) DO NOTHING
        """, [(ad_id, p[0], p[1], p[2]) for p in ad_items],
            template="(%s::uuid, %s::uuid, %s, %s)")

        # Mark products as on_ad
        on_ad_ids = [p[0] for p in ad_items]
        cur.execute("""
            UPDATE pos.products SET is_on_ad = TRUE, updated_at = NOW()
            WHERE product_id = ANY(%s::uuid[])
        """, (on_ad_ids,))

    conn.commit()
    log.info("Created weekly ad '%s' (%s–%s) with %d items",
             ad_name, week_start, week_end, len(ad_items))
    return [(p[0], p[2]) for p in ad_items]


def expire_old_ads(conn, sim_date: date) -> None:
    """
    Clears is_on_ad flag for products whose weekly ad has ended.
    Called once per simulated day.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pos.products p
            SET    is_on_ad    = FALSE,
                   updated_at  = NOW()
            WHERE  p.is_on_ad = TRUE
              AND  NOT EXISTS (
                  SELECT 1
                  FROM   pricing.ad_items ai
                  JOIN   pricing.weekly_ads a ON a.ad_id = ai.ad_id
                  WHERE  ai.product_id = p.product_id
                    AND  a.start_date <= %s
                    AND  a.end_date   >= %s
              )
        """, (sim_date, sim_date))
    conn.commit()


def get_ad_product_ids(conn, sim_date: date) -> List[str]:
    """Returns product_ids currently featured in the active weekly ad."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ai.product_id::text
            FROM   pricing.ad_items ai
            JOIN   pricing.weekly_ads a ON a.ad_id = ai.ad_id
            WHERE  a.start_date <= %s AND a.end_date >= %s
        """, (sim_date, sim_date))
        return [r[0] for r in cur.fetchall()]
