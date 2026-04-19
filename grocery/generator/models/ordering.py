"""
Ordering model — store employees create purchase orders to the warehouse
when inventory falls below reorder thresholds.

Flow:
  1. check_and_create_orders() — called once per simulated day:
     - Finds store/product combos below reorder_point in inv.stock_levels
     - Creates ordering.store_orders + store_order_items
     - Auto-approves (same day in simulation)

  2. Returns list of created order IDs for fulfillment to pick up.
"""
import random
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict
from uuid import uuid4

from faker import Faker
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)
fake = Faker('en_US')


def check_and_create_orders(
    conn,
    store_locations: List[Dict],
    warehouse_locations: List[Dict],
    managers: List[Dict],
    sim_dt: datetime,
) -> List[str]:
    """
    Find low-stock items at each store and create store orders.
    Returns list of new order_ids.
    """
    if not warehouse_locations:
        return []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT sl.location_id, sl.product_id, sl.quantity_on_hand,
                   ip.reorder_point, ip.reorder_qty
            FROM inv.stock_levels sl
            JOIN inv.products ip ON ip.product_id = sl.product_id
            JOIN hr.locations l ON l.location_id = sl.location_id
            WHERE l.location_type = 'store'
              AND sl.quantity_on_hand < ip.reorder_point
        """)
        low_stock = cur.fetchall()

    if not low_stock:
        return []

    # Group by store location
    by_store: Dict[str, list] = {}
    for loc_id, prod_id, qty_oh, reorder_pt, reorder_qty in low_stock:
        key = str(loc_id)
        if key not in by_store:
            by_store[key] = []
        by_store[key].append((str(prod_id), reorder_qty))

    warehouse = random.choice(warehouse_locations)
    order_ids = []
    mgr_map = {m['location_id']: m['employee_id'] for m in managers}

    with conn.cursor() as cur:
        for store_loc_id, items in by_store.items():
            order_id = str(uuid4())
            created_by = mgr_map.get(store_loc_id)
            requested_delivery = (sim_dt + timedelta(days=random.randint(1, 3))).date()
            approved_dt = sim_dt  # auto-approved in simulation

            cur.execute("""
                INSERT INTO ordering.store_orders
                    (order_id, store_location_id, warehouse_location_id,
                     created_by, order_dt, requested_delivery_dt,
                     approved_by, approved_dt, status)
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, %s, %s, %s::uuid, %s, 'approved')
            """, (order_id, store_loc_id, warehouse['location_id'],
                   created_by, sim_dt, requested_delivery,
                   created_by, approved_dt))

            item_records = [(order_id, prod_id, qty, qty) for prod_id, qty in items]
            execute_values(cur, """
                INSERT INTO ordering.store_order_items
                    (order_id, product_id, quantity_requested, quantity_approved)
                VALUES %s
            """, item_records, template="(%s::uuid,%s::uuid,%s,%s)")

            order_ids.append(order_id)

    conn.commit()
    log.info("Created %d store orders for %d low-stock combos",
             len(order_ids), len(low_stock))
    return order_ids
