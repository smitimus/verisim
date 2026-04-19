"""
Transport model — seeds trucks and manages load dispatch and delivery.

Flow:
  1. seed_trucks() — creates a small fleet on startup.
  2. dispatch_loads() — after fulfillment, assigns packed orders to trucks.
  3. receive_delivered_loads() — marks in-transit loads as delivered,
     creates inv.receipts + receipt_items, restocks inventory.
"""
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from uuid import uuid4

from faker import Faker
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)
fake = Faker('en_US')

TRUCK_MAKES = ['Freightliner', 'Peterbilt', 'Kenworth', 'Mack', 'Volvo']
TRUCK_MODELS = ['Cascadia', '579', 'T680', 'Anthem', 'VNL']


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
        for i, (dest_loc_id, items) in enumerate(by_dest.items()):
            truck = truck_cycle[i % len(truck_cycle)]
            driver = random.choice(drivers)['employee_id'] if drivers else None
            load_id = str(uuid4())

            cur.execute("""
                INSERT INTO transport.loads
                    (load_id, truck_id, driver_id, warehouse_location_id,
                     destination_location_id, departed_at, status)
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, %s::uuid, %s, 'in_transit')
            """, (load_id, truck['truck_id'], driver,
                   warehouse_location_id, dest_loc_id, sim_dt))

            load_item_records = [(load_id, f_id, o_id) for f_id, o_id in items]
            execute_values(cur, """
                INSERT INTO transport.load_items (load_id, fulfillment_id, store_order_id)
                VALUES %s
            """, load_item_records, template="(%s::uuid,%s::uuid,%s::uuid)")

            load_ids.append(load_id)

    conn.commit()
    log.info("Dispatched %d truck loads", len(load_ids))
    return load_ids


def receive_delivered_loads(conn, sim_dt: datetime) -> int:
    """
    Mark in-transit loads as delivered (simulated arrival = dispatch + 1 day).
    For each delivered load, create inv.receipts + receipt_items and restock.
    Returns number of loads received.
    """
    # Loads dispatched more than 18 simulated hours ago are considered delivered
    cutoff = sim_dt - timedelta(hours=18)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT l.load_id, l.destination_location_id
            FROM transport.loads l
            WHERE l.status = 'in_transit' AND l.departed_at <= %s
        """, (cutoff,))
        pending = cur.fetchall()

    if not pending:
        return 0

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

            # Get fulfillment items for this load
            cur.execute("""
                SELECT fi.product_id, fi.quantity_picked
                FROM transport.load_items li
                JOIN fulfillment.items fi ON fi.fulfillment_id = li.fulfillment_id
                WHERE li.load_id = %s::uuid AND fi.pick_status = 'picked'
            """, (load_id,))
            items = cur.fetchall()

            if not items:
                continue

            # Create inv.receipt
            receipt_id = str(uuid4())
            po_number = f"RCV-{fake.bothify('########').upper()}"
            total_cost = 0.0

            receipt_item_records = []
            for prod_id, qty in items:
                unit_cost = round(random.uniform(0.25, 10.0), 4)
                line_total = round(unit_cost * qty, 2)
                total_cost += line_total
                receipt_item_records.append((receipt_id, str(prod_id), qty, unit_cost, line_total))

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

            # Update store order status to delivered
            cur.execute("""
                UPDATE ordering.store_orders so
                SET status = 'delivered', updated_at = %s
                FROM transport.load_items li
                WHERE li.load_id = %s::uuid
                  AND li.store_order_id = so.order_id
            """, (sim_dt, load_id))

    conn.commit()
    log.info("Received %d delivered loads", len(pending))
    return len(pending)
