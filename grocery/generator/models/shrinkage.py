"""
Shrinkage model — simulates perishable product expiry, spoilage, damage, theft.

Called once per simulated day after POS depletion.

Flow:
  1. mark_perishable_products()  — one-time setup, sets is_perishable + shelf_life_days
  2. set_expiry_dates()           — assigns expiry_date to stock rows that lack one
  3. generate_shrinkage_events()  — expires overdue stock, applies random shrink rates
"""
import random
import logging
from datetime import datetime
from typing import List, Dict

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# Departments whose products are perishable and their typical shelf life in days
PERISHABLE_DEPARTMENTS = {
    'Produce':       3,
    'Dairy & Eggs':  7,
    'Meat & Seafood': 3,
    'Bakery':        2,
    'Deli':          4,
}

# Daily random shrink rates by reason (probability AND magnitude)
# Each entry: (daily_probability, fraction_of_stock_affected)
DAILY_SHRINK = {
    'spoilage': (0.30, 0.008),   # 30% chance, ~0.8% of stock
    'damaged':  (0.15, 0.004),   # 15% chance, ~0.4% of stock
    'theft':    (0.10, 0.002),   # 10% chance, ~0.2% of stock
}


def mark_perishable_products(conn) -> None:
    """
    One-time setup: marks products in perishable departments as perishable
    and assigns shelf_life_days. Skips if already done.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pos.products WHERE is_perishable = TRUE")
        if cur.fetchone()[0] > 0:
            return

        for dept_name, days in PERISHABLE_DEPARTMENTS.items():
            cur.execute("""
                UPDATE pos.products p
                SET    is_perishable   = TRUE,
                       shelf_life_days = %s,
                       updated_at      = NOW()
                WHERE  EXISTS (
                    SELECT 1 FROM pos.departments d
                    WHERE  d.department_id = p.department_id
                    AND    d.name = %s
                )
            """, (days, dept_name))
    conn.commit()
    log.info("Marked perishable products by department.")


def set_expiry_dates(conn, sim_dt: datetime) -> None:
    """
    Assigns expiry_date to stock_levels rows that have a perishable product
    but no expiry_date yet. Called once per simulated day.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE inv.stock_levels sl
            SET    expiry_date  = (%s::date + p.shelf_life_days),
                   last_updated = NOW()
            FROM   pos.products p
            WHERE  sl.product_id     = p.product_id
              AND  p.is_perishable   = TRUE
              AND  p.shelf_life_days IS NOT NULL
              AND  sl.expiry_date    IS NULL
              AND  sl.quantity_on_hand > 0
        """, (sim_dt.date(),))
    conn.commit()


def generate_shrinkage_events(conn, sim_dt: datetime,
                               locations: List[Dict]) -> int:
    """
    Generates shrinkage events for perishable products at store locations.
      - Expired stock: all remaining quantity is written off as 'expired'
      - Random daily shrink: spoilage / damaged / theft at configured rates
    Returns the number of shrinkage events recorded.
    """
    store_ids = [loc['location_id'] for loc in locations]
    today = sim_dt.date()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT sl.stock_id::text, sl.product_id::text,
                   sl.location_id::text, sl.quantity_on_hand,
                   p.cost, sl.expiry_date
            FROM   inv.stock_levels sl
            JOIN   pos.products p ON p.product_id = sl.product_id
            WHERE  p.is_perishable        = TRUE
              AND  sl.location_id         = ANY(%s::uuid[])
              AND  sl.quantity_on_hand    > 0
        """, (store_ids,))
        stock_rows = cur.fetchall()

    if not stock_rows:
        return 0

    shrinkage_records = []   # (product_id, location_id, qty, reason, cost, recorded_at)
    depletions = {}          # stock_id -> total qty to deduct
    expiry_resets = set()    # stock_ids whose expiry_date should be NULLed

    for stock_id, product_id, location_id, qty_on_hand, cost, expiry_date in stock_rows:
        remaining = qty_on_hand

        # --- Expiry: write off entire remaining stock ---
        if expiry_date and expiry_date <= today and remaining > 0:
            shrinkage_records.append((
                product_id, location_id, remaining,
                'expired', round(float(cost) * remaining, 2), sim_dt,
            ))
            depletions[stock_id] = remaining
            expiry_resets.add(stock_id)
            continue  # no further shrink on already-expired stock

        # --- Random daily shrink for perishables ---
        for reason, (daily_prob, rate) in DAILY_SHRINK.items():
            if random.random() < daily_prob and remaining > 0:
                shrink_qty = max(1, round(remaining * rate * random.uniform(0.5, 2.0)))
                shrink_qty = min(shrink_qty, remaining)
                shrinkage_records.append((
                    product_id, location_id, shrink_qty,
                    reason, round(float(cost) * shrink_qty, 2), sim_dt,
                ))
                depletions[stock_id] = depletions.get(stock_id, 0) + shrink_qty
                remaining -= shrink_qty

    if not shrinkage_records:
        return 0

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO inv.shrinkage_events
                (product_id, location_id, quantity, reason, estimated_cost, recorded_at)
            VALUES %s
        """, shrinkage_records)

        for stock_id, qty in depletions.items():
            if stock_id in expiry_resets:
                # Expired — zero out stock and clear expiry date
                cur.execute("""
                    UPDATE inv.stock_levels
                    SET    quantity_on_hand = 0,
                           expiry_date      = NULL,
                           last_updated     = NOW()
                    WHERE  stock_id = %s::uuid
                """, (stock_id,))
            else:
                cur.execute("""
                    UPDATE inv.stock_levels
                    SET    quantity_on_hand = GREATEST(0, quantity_on_hand - %s),
                           last_updated     = NOW()
                    WHERE  stock_id = %s::uuid
                """, (qty, stock_id))

    conn.commit()
    log.debug("Shrinkage: %d events recorded for %s", len(shrinkage_records), today)
    return len(shrinkage_records)
