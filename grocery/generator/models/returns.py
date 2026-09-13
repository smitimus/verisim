"""
Returns & refunds model — reverses POS purchases back into inventory.

A return references the original transaction; refund amounts are prorated
from the transaction total and capped by line totals, so per transaction
SUM(pos.return_items.refund_amount) <= pos.transactions.total holds
(reconcilable in dbt). is_restocked drives stock reintegration.

Lifecycle (runs once per simulated day, alongside shrinkage):
  1. Sample transactions aged 2-14 days that still have unreturned quantity.
  2. Create pos.returns + pos.return_items (1-3 lines per return).
  3. Restock sellable returns into inv.stock_levels at the sale location.

Reasons drive restock-ability: defective / damaged_in_transit goods are
written off; changed_mind / wrong_item / price_found_lower / other go back
on the shelf. Perishables never restock.
"""
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)

# reason -> (weight, restocks)
RETURN_REASONS = [
    ('defective',          0.30, False),
    ('changed_mind',       0.25, True),
    ('wrong_item',         0.18, True),
    ('damaged_in_transit', 0.12, False),
    ('price_found_lower',  0.09, True),
    ('other',              0.06, True),
]

REFUND_METHODS = ('original_payment', 'cash', 'store_credit')

# Per-day pass probability for a transaction inside the age window. With a
# 13-day window this converges to roughly 3-6% of transactions returned —
# slightly generous vs real grocery (1-3%) to keep the mock dataset useful.
RETURN_PROB_PER_DAY_OLD = 0.0045
AGE_MIN_DAYS = 2
AGE_MAX_DAYS = 14


def _restock_by_location(restock_info: List[Dict]) -> List[Dict]:
    """Aggregate restock rows to (location_id, product_id, qty) integers."""
    agg: Dict[tuple, float] = {}
    for r in restock_info:
        key = (r['location_id'], r['product_id'])
        agg[key] = agg.get(key, 0) + float(r['quantity'])
    out = []
    for (loc_id, prod_id), qty in agg.items():
        whole = int(qty)
        # fractional (lb) returns restock at least 1 unit when > 0.5
        if qty - whole >= 0.5:
            whole += 1
        if whole > 0:
            out.append({'location_id': loc_id, 'product_id': prod_id,
                        'quantity': whole})
    return out


def generate_returns(conn, cfg: Config, sim_dt: datetime,
                     scenario) -> List[Dict]:
    """
    One daily pass: create returns for transactions aged 2-14 days.
    Returns restock info: [{location_id, product_id, quantity}] (integers).
    """
    restock_info: List[Dict] = []

    with conn.cursor() as cur:
        # 1) Candidate transactions in the age window that have never been
        #    returned (one return per transaction keeps the model idempotent:
        #    re-running any window creates nothing new).
        cur.execute("""
            SELECT t.transaction_id::text, t.location_id::text,
                   COALESCE(t.member_id::text, ''),
                   t.transaction_dt::timestamp WITHOUT TIME ZONE, t.total
            FROM pos.transactions t
            WHERE t.transaction_dt BETWEEN %s - (%s * interval '1 day')
                                       AND %s - (%s * interval '1 day')
              AND random() < %s
              AND NOT EXISTS (
                    SELECT 1 FROM pos.returns r
                    WHERE r.transaction_id = t.transaction_id)
            ORDER BY random()
            LIMIT 80
        """, (sim_dt, AGE_MAX_DAYS, sim_dt, AGE_MIN_DAYS,
              RETURN_PROB_PER_DAY_OLD))
        txn_rows = cur.fetchall()
        conn.commit()

    if not txn_rows:
        return restock_info

    # 2) Load item lines + already-returned quantities for candidates
    txn_ids = [r[0] for r in txn_rows]
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ti.item_id::text, ti.transaction_id::text,
                   ti.product_id::text, ti.quantity, ti.line_total,
                   p.is_perishable
            FROM pos.transaction_items ti
            JOIN pos.products p ON p.product_id = ti.product_id
            WHERE ti.transaction_id = ANY(%s::uuid[])
            ORDER BY ti.transaction_id
        """, (txn_ids,))
        lines_by_txn: Dict[str, List] = {}
        for item_id, txn_id, product_id, qty, line_total, is_perish in cur.fetchall():
            lines_by_txn.setdefault(txn_id, []).append(
                {'item_id': item_id, 'product_id': product_id,
                 'quantity': float(qty), 'line_total': float(line_total),
                 'is_perishable': is_perish})

        cur.execute("""
            SELECT ri.transaction_item_id::text, SUM(ri.quantity)
            FROM pos.return_items ri
            JOIN pos.returns r ON r.return_id = ri.return_id
            WHERE r.transaction_id = ANY(%s::uuid[])
            GROUP BY 1
        """, (txn_ids,))
        returned_qty = {row[0]: float(row[1]) for row in cur.fetchall()}

    # 3) Build returns
    return_records: List = []
    return_item_payloads: List[List] = []   # parallel to return_records
    for txn_id, loc_id, member_id, txn_dt, total in txn_rows:
        lines = lines_by_txn.get(txn_id, [])
        avail = []
        for ln in lines:
            remaining = ln['quantity'] - returned_qty.get(ln['item_id'], 0.0)
            if remaining > 0.001:
                avail.append(dict(ln, remaining=remaining))
        if not avail:
            continue

        reason, _, base_restock = random.choices(
            RETURN_REASONS, weights=[w for _, w, _ in RETURN_REASONS])[0]

        n_return_lines = min(len(avail),
                             random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0])
        chosen = random.sample(avail, n_return_lines)

        # Perishables never restock regardless of reason
        has_perishable = any(c['is_perishable'] for c in chosen)
        line_restock = base_restock and not has_perishable

        refund_method = random.choice(REFUND_METHODS)

        subtotal = sum(c['line_total'] for c in lines) or 0.01
        header_refund = 0.0
        item_rows = []
        for c in chosen:
            take_qty = round(c['remaining'], 3)
            if c['quantity'] > 1 and random.random() < 0.25:
                take_qty = round(c['remaining'] * random.uniform(0.4, 0.95), 3)
            if take_qty <= 0:
                continue
            # Refund share of the transaction total, capped at line_total
            line_frac = c['line_total'] / subtotal
            refund_line = round(min(float(total) * line_frac, c['line_total']), 2)
            if refund_line <= 0:
                continue
            header_refund += refund_line
            item_rows.append((c['item_id'], c['product_id'], take_qty, refund_line))

        if not item_rows:
            continue
        header_refund = min(round(header_refund, 2), round(float(total), 2))

        return_dt = txn_dt + timedelta(days=random.randint(AGE_MIN_DAYS, 21))
        if return_dt > sim_dt:
            return_dt = sim_dt

        return_records.append((
            txn_id, loc_id, member_id or None, return_dt, reason,
            refund_method, header_refund, line_restock,
            scenario.scenario_tag if scenario else None))
        return_item_payloads.append(item_rows)

        if line_restock:
            for item_id, product_id, take_qty, _ in item_rows:
                restock_info.append({'location_id': loc_id,
                                     'product_id': product_id,
                                     'quantity': take_qty})

    # 4) Insert headers, then items with the returned ids
    with conn.cursor() as cur:
        if return_records:
            rid_rows = execute_values(cur, """
                INSERT INTO pos.returns
                    (transaction_id, location_id, member_id, return_dt, reason,
                     refund_method, refund_amount, is_restocked, scenario_tag)
                VALUES %s
                RETURNING return_id::text
            """, [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8])
                  for r in return_records],
                template="(%s::uuid,%s::uuid,%s::uuid,%s,%s,%s,%s,%s,%s)",
                fetch=True)
            # RETURNING order is guaranteed to match VALUES order for
            # execute_values single-statement inserts in Postgres.
            flat_items = []
            for (rid,), rows in zip(rid_rows, return_item_payloads):
                for item_id, product_id, take_qty, refund_line in rows:
                    flat_items.append((rid, item_id, product_id,
                                       take_qty, refund_line))
            if flat_items:
                execute_values(cur, """
                    INSERT INTO pos.return_items
                        (return_id, transaction_item_id, product_id,
                         quantity, refund_amount)
                    VALUES %s
                """, flat_items,
                template="(%s::uuid,%s::uuid,%s::uuid,%s,%s)")
        conn.commit()

    log.info("Returns: %d created, %d item lines, restock for %d locations",
             len(return_records),
             sum(len(p) for p in return_item_payloads),
             len({r['location_id'] for r in restock_info}))
    return _restock_by_location(restock_info)


def restock_returns(conn, restock_info: List[Dict]) -> None:
    """Add returned sellable stock back to inv.stock_levels (upsert-safe)."""
    if not restock_info:
        return
    with conn.cursor() as cur:
        execute_values(cur, """
            UPDATE inv.stock_levels sl
            SET quantity_on_hand = sl.quantity_on_hand + v.qty,
                last_updated = NOW()
            FROM (VALUES %s) AS v(loc, prod, qty)
            WHERE sl.location_id = v.loc::uuid AND sl.product_id = v.prod::uuid
        """, [(r['location_id'], r['product_id'], r['quantity'])
              for r in restock_info])
    conn.commit()


def backfill_returns_for_range(conn, cfg: Config, start_dt, end_dt, scenario=None):
    """
    Catch-up pass for days already backfilled: walks the window in date order
    running one daily returns pass per day. Idempotent because a transaction
    carries at most one return (NOT EXISTS guard).
    """
    passes = 0
    day = start_dt
    while day <= end_dt:
        restock = generate_returns(conn, cfg, day, scenario)
        if restock:
            restock_returns(conn, restock)
        passes += 1
        day += timedelta(days=1)
    return passes
