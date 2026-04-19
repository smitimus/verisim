"""
Inventory model — seeds stock levels and handles POS depletion.

Restocking is handled by the ordering → fulfillment → transport → receipt
pipeline (transport.receive_delivered_loads). This module only seeds initial
stock and handles depletion when POS transactions are generated.
"""
import logging
from typing import List, Dict

from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)


def seed_inventory(conn, cfg: Config, products: List[Dict],
                   store_locations: List[Dict]) -> None:
    """
    Create inv.products and inv.stock_levels for all product/store combos.
    Only seeds stock at store locations (warehouse stock managed separately).
    Idempotent.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM inv.products")
        if cur.fetchone()[0] > 0:
            return

    log.info("Seeding inventory for %d products × %d stores...",
             len(products), len(store_locations))

    import random
    SUPPLIERS = [
        'UNFI', 'KeHE Distributors', 'McLane Company',
        'C&S Wholesale Grocers', 'Nash Finch', 'Supervalu'
    ]

    inv_prod_records = []
    for p in products:
        inv_prod_records.append((
            p['product_id'],
            random.randint(15, 40),    # reorder_point
            random.randint(50, 300),   # reorder_qty
            p.get('uom', 'each'),
            random.choice(SUPPLIERS),
            random.randint(1, 4),      # lead_time_days
        ))

    stock_records = []
    for p in products:
        for loc in store_locations:
            stock_records.append((
                p['product_id'],
                loc['location_id'],
                cfg.inventory.initial_stock_per_product,
                0,
            ))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO inv.products
                (product_id, reorder_point, reorder_qty, unit_of_measure,
                 supplier_name, lead_time_days)
            VALUES %s ON CONFLICT (product_id) DO NOTHING
        """, inv_prod_records, template="(%s::uuid,%s,%s,%s,%s,%s)")

        execute_values(cur, """
            INSERT INTO inv.stock_levels
                (product_id, location_id, quantity_on_hand, quantity_reserved)
            VALUES %s ON CONFLICT (product_id, location_id) DO NOTHING
        """, stock_records, template="(%s::uuid,%s::uuid,%s,%s)")

    conn.commit()
    log.info("Seeded inventory")


def refresh_perishable_expiry_on_receipt(conn, location_id: str,
                                          product_ids: List[str],
                                          received_date) -> None:
    """
    When new stock arrives via receipt, update expiry_date for restocked
    perishable items. The clock resets to received_date + shelf_life_days.
    Called by transport.receive_delivered_loads after updating stock levels.
    """
    if not product_ids:
        return
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE inv.stock_levels sl
            SET    expiry_date  = (%s::date + p.shelf_life_days),
                   last_updated = NOW()
            FROM   pos.products p
            WHERE  sl.product_id   = p.product_id
              AND  p.is_perishable = TRUE
              AND  p.shelf_life_days IS NOT NULL
              AND  sl.location_id  = %s::uuid
              AND  sl.product_id   = ANY(%s::uuid[])
        """, (received_date, location_id, product_ids))
    conn.commit()


def deplete_inventory(conn, depletion_info: List[Dict]) -> None:
    """
    Reduce inv.stock_levels for items sold in a batch of POS transactions.
    depletion_info: list of {transaction_id, items: [{product_id, quantity}]}
    """
    if not depletion_info:
        return

    txn_ids = [d['transaction_id'] for d in depletion_info]
    placeholders = ','.join(['%s::uuid'] * len(txn_ids))

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT t.location_id, ti.product_id, SUM(ti.quantity)::integer as total_qty
            FROM pos.transactions t
            JOIN pos.transaction_items ti ON ti.transaction_id = t.transaction_id
            WHERE t.transaction_id IN ({placeholders})
            GROUP BY t.location_id, ti.product_id
        """, txn_ids)
        rows = cur.fetchall()

    if not rows:
        return

    with conn.cursor() as cur:
        for loc_id, prod_id, qty in rows:
            cur.execute("""
                UPDATE inv.stock_levels
                SET quantity_on_hand = GREATEST(0, quantity_on_hand - %s),
                    last_updated = NOW()
                WHERE product_id = %s::uuid AND location_id = %s::uuid
            """, (int(qty), str(prod_id), str(loc_id)))
    conn.commit()
