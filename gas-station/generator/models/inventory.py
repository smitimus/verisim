"""
Inventory model — seeds stock levels and handles depletion + restocking.
"""
import random
import logging
from datetime import datetime
from typing import List, Dict
from uuid import uuid4

from faker import Faker
from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)
fake = Faker('en_US')

SUPPLIERS = [
    'McLane Company', 'Core-Mark International', 'Nash Finch',
    'Vistar Corporation', 'GSC Enterprises', 'Eby-Brown Company'
]


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def seed_inventory(conn, cfg: Config, products: List[Dict], locations: List[Dict]) -> None:
    """
    Create inv.products and inv.stock_levels for all product/location combos.
    Idempotent — skips if data already exists.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM inv.products")
        if cur.fetchone()[0] > 0:
            return

        log.info("Seeding inventory...")

        # inv.products — one row per POS product with reorder settings
        inv_prod_records = []
        for p in products:
            inv_prod_records.append((
                p['product_id'],
                random.randint(10, 30),   # reorder_point
                random.randint(50, 200),  # reorder_qty
                'each',
                random.choice(SUPPLIERS),
                random.randint(1, 5),     # lead_time_days
            ))

        execute_values(cur, """
            INSERT INTO inv.products
                (product_id, reorder_point, reorder_qty, unit_of_measure, supplier_name, lead_time_days)
            VALUES %s
            ON CONFLICT (product_id) DO NOTHING
        """, inv_prod_records, template="(%s::uuid,%s,%s,%s,%s,%s)")

        # inv.stock_levels — one row per product per location
        stock_records = []
        for p in products:
            for loc in locations:
                stock_records.append((
                    p['product_id'],
                    loc['location_id'],
                    cfg.inventory.initial_stock_per_product,
                    0,
                ))

        execute_values(cur, """
            INSERT INTO inv.stock_levels
                (product_id, location_id, quantity_on_hand, quantity_reserved)
            VALUES %s
            ON CONFLICT (product_id, location_id) DO NOTHING
        """, stock_records, template="(%s::uuid,%s::uuid,%s,%s)")

        conn.commit()
        log.info("Seeded inventory for %d products × %d locations", len(products), len(locations))


# ---------------------------------------------------------------------------
# Depletion
# ---------------------------------------------------------------------------

def deplete_inventory(conn, depletion_info: List[Dict], locations: List[Dict]) -> None:
    """
    Reduce inv.stock_levels for items sold in a batch of POS transactions.

    depletion_info: list of {transaction_id, items: [{product_id, quantity}]}
    We need to know the location for each transaction — look it up from DB.
    """
    if not depletion_info:
        return

    # Build a flat list of (product_id, location_id, quantity) aggregated by product+location
    txn_ids = [d['transaction_id'] for d in depletion_info]

    with conn.cursor() as cur:
        # Get location for each transaction (already in pos.transactions)
        placeholders = ','.join(['%s::uuid'] * len(txn_ids))
        cur.execute(f"""
            SELECT t.transaction_id, t.location_id, ti.product_id, ti.quantity
            FROM pos.transactions t
            JOIN pos.transaction_items ti ON ti.transaction_id = t.transaction_id
            WHERE t.transaction_id IN ({placeholders})
        """, txn_ids)
        rows = cur.fetchall()

    # Aggregate by (product_id, location_id)
    depletion: Dict[tuple, int] = {}
    for txn_id, loc_id, prod_id, qty in rows:
        key = (str(prod_id), str(loc_id))
        depletion[key] = depletion.get(key, 0) + qty

    if not depletion:
        return

    with conn.cursor() as cur:
        for (product_id, location_id), qty in depletion.items():
            cur.execute("""
                UPDATE inv.stock_levels
                SET quantity_on_hand = GREATEST(0, quantity_on_hand - %s),
                    last_updated = NOW()
                WHERE product_id = %s::uuid AND location_id = %s::uuid
            """, (qty, product_id, location_id))
    conn.commit()


# ---------------------------------------------------------------------------
# Restocking
# ---------------------------------------------------------------------------

def check_and_restock(conn, cfg: Config, locations: List[Dict]) -> int:
    """
    Find products below reorder_point and create inv.receipts + items.
    Called once per simulated day.
    Returns the number of receipt lines created.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sl.product_id, sl.location_id, sl.quantity_on_hand,
                   ip.reorder_point, ip.reorder_qty, ip.supplier_name
            FROM inv.stock_levels sl
            JOIN inv.products ip ON ip.product_id = sl.product_id
            WHERE sl.quantity_on_hand < ip.reorder_point
        """)
        low_stock = cur.fetchall()

    if not low_stock:
        return 0

    # Group by location so we can create one receipt per location per supplier
    by_loc_supplier: Dict[tuple, list] = {}
    for prod_id, loc_id, qty_on_hand, reorder_pt, reorder_qty, supplier in low_stock:
        key = (str(loc_id), supplier or 'Unknown')
        if key not in by_loc_supplier:
            by_loc_supplier[key] = []
        by_loc_supplier[key].append((str(prod_id), reorder_qty))

    total_lines = 0
    with conn.cursor() as cur:
        for (loc_id, supplier), items in by_loc_supplier.items():
            receipt_id = str(uuid4())
            po_number = f"PO-{fake.bothify('########').upper()}"
            received_dt = datetime.now()
            total_cost = 0.0

            receipt_item_records = []
            for prod_id, reorder_qty in items:
                unit_cost = round(random.uniform(0.25, 10.0), 4)
                line_total = round(unit_cost * reorder_qty, 2)
                total_cost += line_total
                receipt_item_records.append((receipt_id, prod_id, reorder_qty, unit_cost, line_total))

            cur.execute("""
                INSERT INTO inv.receipts
                    (receipt_id, location_id, received_dt, supplier_name, po_number, total_cost)
                VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s)
            """, (receipt_id, loc_id, received_dt, supplier, po_number, round(total_cost, 2)))

            execute_values(cur, """
                INSERT INTO inv.receipt_items
                    (receipt_id, product_id, quantity, unit_cost, line_total)
                VALUES %s
            """, receipt_item_records,
            template="(%s::uuid,%s::uuid,%s,%s,%s)")

            # Update stock levels
            for prod_id, reorder_qty in items:
                cur.execute("""
                    UPDATE inv.stock_levels
                    SET quantity_on_hand = quantity_on_hand + %s,
                        last_updated = NOW()
                    WHERE product_id = %s::uuid AND location_id = %s::uuid
                """, (reorder_qty, prod_id, loc_id))

            total_lines += len(items)

    conn.commit()
    log.info("Restocked %d product/location combos across %d receipts",
             total_lines, len(by_loc_supplier))
    return total_lines
