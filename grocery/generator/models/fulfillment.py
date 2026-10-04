"""
Fulfillment model — warehouse processes approved store orders.

Flow:
  1. process_pending_orders() — called after ordering, same simulated day:
     - Finds approved orders
     - Creates fulfillment.orders for each
     - Creates fulfillment.items (picks all requested qty, simulates occasional shorts)
     - Marks fulfillment as 'packed'
     - Updates store order status to 'picking' → then 'shipped'

  2. Returns list of (fulfillment_id, store_order_id) tuples for transport.

Short-picks are the vendor's behaviour, not the warehouse's (t_57b1a1ab)
--------------------------------------------------------------------------
This used to be `random.random() < 0.05` on every line of every order: one flat
rate for every SKU, from every vendor, forever. A mart built on that could not
rank vendors — they were all identical by construction.

`vendor_cfg` is the `cfg.vendors` block. When it is supplied, the short rate
for a line is that PRODUCT's vendor rate (`inv.suppliers.short_ship_rate` via
`suppliers.short_probability_for`), so a vendor with a 15% rate really is
shorted three times as often as one at 5%. Supply disruption still stacks on
top, because a scenario is a departure from normal rather than a vendor's
character.

The parameter is optional so existing callers and tests are unaffected, and so
a pre-t_57b1a1ab data dir (no `inv.suppliers` at all) keeps the old behaviour
rather than failing to pick anything.
"""
import random
import logging
from datetime import datetime
from typing import Dict, List, Tuple
from uuid import uuid4

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# The flat short rate used when no vendor config is available (the pre-
# t_57b1a1ab behaviour, and the fallback for a data dir with no inv.suppliers).
# Named so it is greppable rather than a bare 0.05 literal.
DEFAULT_SHORT_RATE = 0.05

# Units a short pick takes off the requested quantity.
SHORT_PICK_MIN_UNITS = 1
SHORT_PICK_MAX_UNITS = 5

# How much worse a short pick gets under scenario.supply_disruption.
SUPPLY_DISRUPTION_SHORT_MULTIPLIER = 2.0


def process_pending_orders(
    conn,
    warehouse_employees: List[Dict],
    sim_dt: datetime,
    vendor_cfg=None,
    scenario=None,
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

    # One vendor lookup per pass, not per line (t_57b1a1ab).
    vendor_by_product: Dict[str, Dict] = {}
    if vendor_cfg is not None:
        try:
            from models import suppliers as _suppliers
            vendor_by_product = _suppliers.vendor_by_product(conn)
        except Exception:
            log.warning("Vendor lookup unavailable — using the flat short rate "
                        "(pre-t_57b1a1ab data dir?)")
            vendor_by_product = {}

    # A disruption makes every vendor worse, on top of its own rate — a
    # scenario is a departure from normal, not a vendor's character.
    disruption = bool(scenario is not None
                      and getattr(scenario, 'supply_disruption', False))

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

            # Create fulfillment items. The short rate is the vendor's own.
            item_records = []
            for prod_id, qty_req in items:
                if vendor_by_product:
                    from models import suppliers as _suppliers
                    vendor = vendor_by_product.get(str(prod_id))
                    rate = (_suppliers.short_probability_for(vendor, vendor_cfg)
                            if vendor else DEFAULT_SHORT_RATE)
                else:
                    rate = DEFAULT_SHORT_RATE
                if disruption:
                    rate = min(1.0, rate * SUPPLY_DISRUPTION_SHORT_MULTIPLIER)
                short = random.random() < rate
                qty_picked = (
                    max(0, qty_req - random.randint(SHORT_PICK_MIN_UNITS,
                                                    SHORT_PICK_MAX_UNITS))
                    if short else qty_req)
                pick_status = 'short' if short else 'picked'
                item_records.append((fulfillment_id, str(prod_id), qty_req,
                                     qty_picked, pick_status))

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
