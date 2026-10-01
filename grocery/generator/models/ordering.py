"""
Ordering model — store employees create purchase orders to the warehouse
when inventory falls below reorder thresholds.

Flow:
  1. check_and_create_orders() — called once per simulated day:
     - Finds store/product combos below reorder_point in inv.stock_levels
     - Sizes the order off MEASURED demand (inv.sku_demand_daily) rather than
       the seeded reorder_qty alone
     - Creates ordering.store_orders + store_order_items
     - Auto-approves (same day in simulation)

  2. Returns list of created order IDs for fulfillment to pick up.

Demand-aware sizing (t_959cd040)
--------------------------------
`reorder_qty` was seeded once at startup and never moved, and
`restock_threshold_pct` was read by nothing at all. That was survivable while
depletion floored at zero and sales ignored the shelf: a fast SKU could sell
more than it had, the shelf sat at zero, and nothing was ever short.

It stops being survivable now that a sale is capped at on-hand. A fixed
`reorder_qty` against a measured demand of, say, 700 units/day means the shelf
empties and stays empty — the simulation degenerates into an empty shop. So the
quantity ordered is now

    expected_demand + safety fraction of expected_demand

where `expected_demand` is the daily rate the ledger actually observed over the
configured window, and the safety fraction is `restock_threshold_pct` — the
config key that used to be decorative. The result is clamped to
`reorder_qty_max_multiple` × the seeded `reorder_qty` so one hot SKU, or a SKU
whose ledger has no history yet, cannot produce an unbounded order line.
"""
import random
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
from uuid import uuid4

from faker import Faker
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)
fake = Faker('en_US')

# Fraction of low-stock items a supplier cannot fulfil when scenario
# .supply_disruption is set. Named, not a bare literal, so it is tunable and
# greppable (t_959cd040).
SUPPLY_DISRUPTION_SKIP_RATE = 0.40


def expected_daily_demand(conn, window_days: int = 1,
                          as_of: Optional[date] = None) -> Dict:
    """
    Average units/day demanded per store-SKU over the last `window_days`
    completed days, from inv.sku_demand_daily.

    Returns {(location_id, product_id): units_per_day}. Uses completed days only
    — today's ledger row is still accumulating, and counting it would read as a
    full day when it may be minutes old, under-ordering exactly the SKUs that
    are on fire right now.
    """
    as_of = as_of or date.today()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT location_id::text, product_id::text,
                   SUM(requested_units) / %s::numeric AS units_per_day
            FROM inv.sku_demand_daily
            WHERE demand_date > (%s::date - %s::int)
              AND demand_date <= %s::date
            GROUP BY 1, 2
        """, (max(1, window_days), as_of, max(1, window_days), as_of))
        return {(str(loc), str(prod)): float(units)
                for loc, prod, units in cur.fetchall()}


def reorder_quantity(reorder_qty: int, demand_per_day: Optional[float],
                     safety_pct: float, max_multiple: float) -> int:
    """
    How many units to put on one reorder line.

    Pure function, so the sizing rule is testable without a database.

    With no measured demand the seeded `reorder_qty` is the whole answer — that
    is the first day of a backfill, before the ledger has anything. Once demand
    is known the line covers `window_days` of it plus the safety fraction, so a
    SKU selling 700/day does not get topped up with the seeded 150.

    Clamped to `max_multiple` × the seeded quantity: that keeps a cold SKU (or
    one with a spike) from turning into a warehouse-sized order.
    """
    cap = max(1, int(round(reorder_qty * max_multiple)))
    if not demand_per_day or demand_per_day <= 0:
        return min(reorder_qty, cap)
    target = demand_per_day * (1.0 + max(0.0, safety_pct))
    return int(max(1, min(round(target), cap)))


def check_and_create_orders(
    conn,
    store_locations: List[Dict],
    warehouse_locations: List[Dict],
    managers: List[Dict],
    sim_dt: datetime,
    scenario=None,
    inventory_cfg=None,
) -> List[str]:
    """
    Find low-stock items at each store and create store orders.
    Returns list of new order_ids.

    The quantity per line is demand-aware — see the module docstring and
    `reorder_quantity`. When `scenario.supply_disruption` is True, ~40% of
    low-stock items are randomly skipped (supplier can't fulfill).
    """
    if not warehouse_locations:
        return []

    window_days = 1
    safety_pct = 0.25
    max_multiple = 4.0
    if inventory_cfg is not None:
        window_days = getattr(inventory_cfg, 'reorder_demand_window_days', 1)
        safety_pct = getattr(inventory_cfg, 'restock_threshold_pct', 0.25)
        max_multiple = getattr(inventory_cfg, 'reorder_qty_max_multiple', 4.0)

    # Only the completed days before the day being ordered for — see
    # expected_daily_demand().
    as_of = sim_dt.date() - timedelta(days=1)

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

    demand = expected_daily_demand(conn, window_days, as_of)

    # Supplier disruption: randomly skip ~40% of low-stock items
    if scenario is not None and getattr(scenario, 'supply_disruption', False):
        low_stock = [r for r in low_stock
                     if random.random() > SUPPLY_DISRUPTION_SKIP_RATE]

    if not low_stock:
        return []

    # Group by store location
    by_store: Dict[str, list] = {}
    for loc_id, prod_id, qty_oh, reorder_pt, reorder_qty in low_stock:
        key = str(loc_id)
        if key not in by_store:
            by_store[key] = []
        by_store[key].append((
            str(prod_id),
            reorder_quantity(
                reorder_qty,
                demand.get((str(loc_id), str(prod_id))),
                safety_pct,
                max_multiple,
            ),
        ))

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