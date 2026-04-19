"""
Fulfillment model — warehouse processes approved store orders.

Flow:
  1. process_pending_orders() — called after ordering, same simulated day:
     - Finds approved orders
     - Creates fulfillment.orders for each
     - Creates fulfillment.items (picks all requested qty, simulates occasional shorts)
     - Marks fulfillment as 'packed'
     - Updates store_order status to 'picking' → then 'shipped'

  2. Returns list of (fulfillment_id, store_order_id) tuples for transport.
"""
import random
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from uuid import uuid4

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)


def process_pending_orders(
    conn,
    warehouse_employees: List[Dict],
    sim_dt: datetime,
) -> List[Tuple[str, str, str]]:
    """
    Fulfill all approved orders. Returns list of
    (fulfillment_id, store_order_id, store_location_id).
    """
    # Fetch approved orders
    with conn.cursor() as cur:
        cur.execute("""
            SELECT so.order_id, so.warehouse_location_id, so.store_location_id
            FROM ordering.store_orders so
            WHERE so.status = 'approved'
        """)
        approved = cur.fetchall()

    if not approved:
        return []

    pickers = [e for e in warehouse_employees if e['department'] == 'warehouse']

    fulfilled = []
    with conn.cursor() as cur:
        for order_id, wh_loc_id, store_loc_id in approved:
            fulfillment_id = str(uuid4())
            assigned_to = random.choice(pickers)['employee_id'] if pickers else None

            # Get order items
            cur.execute("""
                SELECT product_id, quantity_approved
                FROM ordering.store_order_items
                WHERE order_id = %s::uuid AND quantity_approved IS NOT NULL
            """, (str(order_id),))
            items = cur.fetchall()

            # Create fulfillment order
            cur.execute("""
                INSERT INTO fulfillment.orders
                    (fulfillment_id, store_order_id, warehouse_location_id,
                     assigned_to, status, started_at, completed_at)
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, 'packed', %s, %s)
            """, (fulfillment_id, str(order_id), str(wh_loc_id),
                   assigned_to, sim_dt, sim_dt))

            # Create fulfillment items (occasional shorts: ~5% chance per line)
            item_records = []
            for prod_id, qty_req in items:
                short = random.random() < 0.05
                qty_picked = max(0, qty_req - random.randint(1, 5)) if short else qty_req
                pick_status = 'short' if short else 'picked'
                item_records.append((fulfillment_id, str(prod_id), qty_req, qty_picked, pick_status))

            if item_records:
                execute_values(cur, """
                    INSERT INTO fulfillment.items
                        (fulfillment_id, product_id, quantity_requested,
                         quantity_picked, pick_status)
                    VALUES %s
                """, item_records, template="(%s::uuid,%s::uuid,%s,%s,%s)")

            # Advance store order to 'picking' → 'shipped'
            cur.execute("""
                UPDATE ordering.store_orders
                SET status = 'shipped', updated_at = %s
                WHERE order_id = %s::uuid
            """, (sim_dt, str(order_id)))

            fulfilled.append((fulfillment_id, str(order_id), str(store_loc_id)))

    conn.commit()
    log.info("Fulfilled %d orders", len(fulfilled))
    return fulfilled
