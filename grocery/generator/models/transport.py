"""
Transport model — seeds trucks and manages load dispatch and delivery.

Flow:
  1. seed_trucks() — creates a small fleet on startup.
  2. dispatch_loads() — after fulfillment, assigns packed orders to trucks.
     Computes haversine distance_miles from warehouse/store coordinates.
  3. receive_delivered_loads() — marks in-transit loads as delivered,
     creates inv.receipts + receipt_items, restocks inventory.
"""
import random
import logging
import math
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from uuid import uuid4

from faker import Faker
from psycopg2.extras import execute_values

from models import suppliers

log = logging.getLogger(__name__)
fake = Faker('en_US')

TRUCK_MAKES = ['Freightliner', 'Peterbilt', 'Kenworth', 'Mack', 'Volvo']
TRUCK_MODELS = ['Cascadia', '579', 'T680', 'Anthem', 'VNL']


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles between two (lat, lon) points.

    Uses the mean Earth radius of 3,958.8 miles.
    """
    R = 3958.8
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def seed_trucks(conn, truck_count: int = 4) -> List[Dict]:
    """Seed a fleet of delivery trucks. Idempotent."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM transport.trucks")
        if cur.fetchone()[0] > 0:
            return _fetch_trucks(cur)

        log.info("Seeding %d trucks...", truck_count)
        records = []
        for _ in range(truck_count):
            records.append((
                fake.license_plate(),
                random.choice(TRUCK_MAKES),
                random.choice(TRUCK_MODELS),
                random.randint(2015, 2023),
                random.choice([18, 22, 24, 26]),
                True,
            ))
        execute_values(cur, """
            INSERT INTO transport.trucks
                (license_plate, make, model, year, capacity_pallets, is_active)
            VALUES %s ON CONFLICT (license_plate) DO NOTHING
        """, records)
        conn.commit()
        return _fetch_trucks(cur)


def _fetch_trucks(cur) -> List[Dict]:
    cur.execute("SELECT truck_id, license_plate, capacity_pallets FROM transport.trucks WHERE is_active = TRUE")
    return [{'truck_id': str(r[0]), 'license_plate': r[1], 'capacity': r[2]} for r in cur.fetchall()]


def fetch_trucks(conn) -> List[Dict]:
    with conn.cursor() as cur:
        return _fetch_trucks(cur)


def dispatch_loads(
    conn,
    fulfilled: List[Tuple[str, str, str]],
    trucks: List[Dict],
    drivers: List[Dict],
    warehouse_location_id: str,
    sim_dt: datetime,
    scenario=None,
) -> List[str]:
    """
    Create transport loads for fulfilled orders.
    Groups fulfillments by destination store, assigns a truck and driver.
    Returns list of load_ids created.
    """
    if not fulfilled or not trucks:
        return []

    # Group by destination store
    by_dest: Dict[str, list] = {}
    for fulfillment_id, order_id, store_loc_id in fulfilled:
        by_dest.setdefault(store_loc_id, []).append((fulfillment_id, order_id))

    load_ids = []
    truck_cycle = list(trucks) * (len(by_dest) // max(len(trucks), 1) + 1)

    with conn.cursor() as cur:
        # Fetch coordinates for warehouse and all destination locations so we
        # can compute haversine distance_miles at dispatch time (verisim#13).
        all_loc_ids = [str(warehouse_location_id)] + list(by_dest.keys())
        cur.execute(
            "SELECT location_id, latitude, longitude FROM hr.locations "
            "WHERE location_id = ANY(%s::uuid[])",
            (all_loc_ids,)
        )
        loc_coords = {str(r[0]): (float(r[1]), float(r[2])) for r in cur.fetchall() if r[1] is not None}

        wh_coords = loc_coords.get(str(warehouse_location_id))

        for i, (dest_loc_id, items) in enumerate(by_dest.items()):
            truck = truck_cycle[i % len(truck_cycle)]
            driver = random.choice(drivers)['employee_id'] if drivers else None
            load_id = str(uuid4())

            # Compute haversine distance (miles); NULL if coords missing.
            dest_coords = loc_coords.get(dest_loc_id)
            if wh_coords and dest_coords:
                dist = round(haversine(wh_coords[0], wh_coords[1],
                                       dest_coords[0], dest_coords[1]), 2)
            else:
                dist = None

            cur.execute("""
                INSERT INTO transport.loads
                    (load_id, truck_id, driver_id, warehouse_location_id,
                     destination_location_id, departed_at, status, distance_miles)
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, %s::uuid, %s, 'in_transit', %s)
            """, (load_id, truck['truck_id'], driver,
                   warehouse_location_id, dest_loc_id, sim_dt, dist))

            load_item_records = [(load_id, f_id, o_id) for f_id, o_id in items]
            execute_values(cur, """
                INSERT INTO transport.load_items (load_id, fulfillment_id, store_order_id)
                VALUES %s
            """, load_item_records, template="(%s::uuid,%s::uuid,%s::uuid)")

            load_ids.append(load_id)

    conn.commit()
    log.info("Dispatched %d truck loads", len(load_ids))
    return load_ids


def receive_delivered_loads(conn, sim_dt: datetime, scenario=None,
                            vendor_cfg=None) -> int:
    """
    Mark in-transit loads as delivered (simulated arrival = dispatch + 1 day).
    For each delivered load, create inv.receipts + receipt_items and restock.
    Returns number of loads received.

    When scenario.supply_disruption is True, delivery cutoff is extended
    from 18h to 36h (deliveries take longer to arrive).

    SHORT LINES (t_57b1a1ab). Before this, the item query filtered
    `pick_status = 'picked'` and the short lines were simply not there: a
    vendor who could not fill an order left no trace at all — no shortage, no
    vendor, no credit. The query below selects BOTH statuses and splits them,
    so the receipt carries what arrived and `suppliers.record_short_ships`
    writes `inv.short_ship_events` for what did not.

    `vendor_cfg` is the `cfg.vendors` block (passed as a whole Config's
    vendors, or None to skip the short-ship path entirely). It is optional so
    this function's existing callers and tests keep working unchanged.
    """
    # Loads dispatched more than N simulated hours ago are considered delivered
    delay_hours = 36 if (scenario is not None and getattr(scenario, 'supply_disruption', False)) else 18
    cutoff = sim_dt - timedelta(hours=delay_hours)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.load_id, l.destination_location_id
            FROM transport.loads l
            WHERE l.status = 'in_transit' AND l.departed_at <= %s
        """, (cutoff,))
        pending = cur.fetchall()

    if not pending:
        return 0

    # One vendor lookup for the whole pass, not one per short line — see
    # suppliers.vendor_by_product.
    vendor_by_product: Optional[Dict[str, Dict]] = None
    if vendor_cfg is not None:
        try:
            vendor_by_product = suppliers.vendor_by_product(conn)
        except Exception:
            # A data dir generated before t_57b1a1ab has no inv.suppliers at all.
            # Receiving is still the most important thing this function does, so
            # a failure here must not stop the receipt.
            log.warning("Vendor lookup unavailable — short-ships not recorded "
                        "(pre-t_57b1a1ab data dir?)")
            vendor_by_product = None

    short_ships = 0
    with conn.cursor() as cur:
        for load_id, dest_loc_id in pending:
            load_id = str(load_id)
            dest_loc_id = str(dest_loc_id)

            # Mark load delivered
            cur.execute("""
                UPDATE transport.loads
                SET status = 'delivered', arrived_at = %s
                WHERE load_id = %s::uuid
            """, (sim_dt, load_id))

            # Get fulfillment items for this load. BOTH pick statuses: the short
            # lines are what the receipt must NOT contain and what the
            # short-ship event must describe.
            cur.execute("""
                SELECT fi.item_id, fi.fulfillment_id, fi.product_id,
                       fi.quantity_requested, fi.quantity_picked, fi.pick_status
                FROM transport.load_items li
                JOIN fulfillment.items fi ON fi.fulfillment_id = li.fulfillment_id
                WHERE li.load_id = %s::uuid
            """, (load_id,))
            item_rows = cur.fetchall()

            items = [r for r in item_rows if r[5] == 'picked']
            short_items = [r for r in item_rows if r[5] == 'short']

            if not items:
                continue

            # Create inv.receipt
            receipt_id = str(uuid4())
            po_number = f"RCV-{fake.bothify('########').upper()}"
            total_cost = 0.0

            receipt_item_records = []
            # Unit cost per product on this load. A short line is priced at the
            # SAME cost as the line next to it — a vendor does not quote a
            # different price for the goods it failed to send, and the credit
            # memo is only reconcilable against the receipt if the two agree.
            unit_cost_by_product = {}

            for row in items:
                _, _, prod_id, _qty_req, qty, _status = row
                unit_cost = unit_cost_by_product.get(str(prod_id))
                if unit_cost is None:
                    unit_cost = round(random.uniform(0.25, 10.0), 4)
                    unit_cost_by_product[str(prod_id)] = unit_cost
                line_total = round(unit_cost * qty, 2)
                total_cost += line_total
                receipt_item_records.append((receipt_id, str(prod_id), qty, unit_cost, line_total))

            # The receipt's vendor. A load is our own warehouse restock, so the
            # vendors behind the goods are the PRODUCTS' vendors rather than one
            # vendor for the load — receipts.supplier_id is therefore left NULL
            # here and the per-line vendor is reachable through
            # inv.products.supplier_id. That is the honest shape: one pallet
            # from our own DC has six vendors' worth of goods on it.
            cur.execute("""
                INSERT INTO inv.receipts
                    (receipt_id, location_id, received_dt, po_number, load_id, total_cost)
                VALUES (%s::uuid, %s::uuid, %s, %s, %s::uuid, %s)
            """, (receipt_id, dest_loc_id, sim_dt, po_number, load_id, round(total_cost, 2)))

            if receipt_item_records:
                execute_values(cur, """
                    INSERT INTO inv.receipt_items
                        (receipt_id, product_id, quantity, unit_cost, line_total)
                    VALUES %s
                """, receipt_item_records, template="(%s::uuid,%s::uuid,%s,%s,%s)")

                # Restock inventory
                for receipt_id_r, prod_id, qty, _, _ in receipt_item_records:
                    cur.execute("""
                        UPDATE inv.stock_levels
                        SET quantity_on_hand = quantity_on_hand + %s,
                            last_updated = NOW()
                        WHERE product_id = %s::uuid AND location_id = %s::uuid
                    """, (qty, prod_id, dest_loc_id))

            # The short lines of THIS load, now that the receipt is written.
            if short_items and vendor_by_product is not None:
                short_rows = []
                for item_id, fulfillment_id, prod_id, qty_req, qty_picked, _s in short_items:
                    product_id = str(prod_id)
                    vendor = vendor_by_product.get(product_id)
                    if not vendor:
                        continue
                    short_rows.append({
                        'item_id': str(item_id),
                        'fulfillment_id': str(fulfillment_id),
                        'product_id': product_id,
                        'quantity_requested': qty_req,
                        'quantity_picked': qty_picked,
                        'unit_cost': unit_cost_by_product.get(
                            product_id, round(random.uniform(0.25, 10.0), 4)),
                    })
                if short_rows:
                    try:
                        short_ships += suppliers.record_short_ships(
                            conn, vendor_cfg, sim_dt, dest_loc_id,
                            vendor_by_product, short_rows, scenario)
                    except Exception:
                        # Never fail a delivery over the shortage paperwork.
                        conn.rollback()
                        log.exception("Could not record short-ships for load %s",
                                      load_id)

            # Update store order status to delivered
            cur.execute("""
                UPDATE ordering.store_orders so
                SET status = 'delivered', updated_at = %s
                FROM transport.load_items li
                WHERE li.load_id = %s::uuid
                  AND li.store_order_id = so.order_id
            """, (sim_dt, load_id))

    conn.commit()
    log.info("Received %d delivered loads (%d short line(s))", len(pending),
             short_ships)
    return len(pending)
