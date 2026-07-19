"""
Cross-schema data connectivity & referential integrity harness (Verisim #11).

This module is the single source of truth for the cross-schema integrity
assertions that the grocery generator must satisfy. It is consumed by:

  * ``test_cross_schema_integrity.py``  — pytest runner (DB + DB-free checks)
  * ``grocery/generator/sql/check_cross_schema_integrity.sql`` — generated,
    human-readable copy for DBAs / manual runs via psql.

Two assertion classes
----------------------
* ``hard_fk``   — a child key references a parent row that does not exist.
  Postgres FK constraints already prevent these at write time, so these
  mostly confirm constraints are intact (and catch any table the FK graph
  missed). An orphan row == failure.
* ``semantic_type`` — the key *exists* (FK satisfied) but points at the wrong
  *kind* of parent. These are NOT FK-enforced and are the highest-risk gaps:
  e.g. ``ordering.store_orders.store_location_id`` must resolve to a location
  whose ``location_type = 'store'``, not a warehouse or DC. An orphan row ==
  failure.

Partial-day tolerance
---------------------
The generator only runs the supply-chain block (orders -> fulfillment ->
transport -> receipts -> shrinkage) at the hour-0 midnight boundary. A
*partial* backfill day (today) therefore legitimately has downstream rows
missing. Every assertion below scopes to *completed* days
(``<ts>::date < CURRENT_DATE``) so a mid-generation snapshot does not produce
false orphans.

Run against a live grocery DB::

    python -m grocery.generator.tests.cross_schema_integrity \\
        postgresql://verisim:verisim@127.0.0.1:5499/grocery

Exit code is non-zero if any assertion returns orphan rows.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass(frozen=True)
class AssertionSpec:
    """One cross-schema integrity check."""

    id: str
    dimension: str  # 'hard_fk' or 'semantic_type'
    title: str
    sql: str  # SELECT returning orphan rows; 0 rows == pass


# ---------------------------------------------------------------------------
# Assertion definitions
# ---------------------------------------------------------------------------
# Convention: every query is a SELECT that returns the offending child rows.
# Empty result set == integrity holds. All time-bounded tables add a
# "completed day" filter (< CURRENT_DATE) so partial backfill days are skipped.

ASSERTIONS: List[AssertionSpec] = [
    # ---- HARD FK ORPHANS (cross-schema) -----------------------------------
    AssertionSpec(
        id="HARD-01",
        dimension="hard_fk",
        title="inv.stock_levels.location_id → hr.locations",
        sql="""
            SELECT sl.stock_id, sl.location_id
            FROM inv.stock_levels sl
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = sl.location_id)
        """,
    ),
    AssertionSpec(
        id="HARD-02",
        dimension="hard_fk",
        title="inv.stock_levels.product_id → pos.products",
        sql="""
            SELECT sl.stock_id, sl.product_id
            FROM inv.stock_levels sl
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = sl.product_id)
        """,
    ),
    AssertionSpec(
        id="HARD-03",
        dimension="hard_fk",
        title="inv.receipts.location_id → hr.locations",
        sql="""
            SELECT r.receipt_id, r.location_id
            FROM inv.receipts r
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = r.location_id)
              AND r.received_dt::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-04",
        dimension="hard_fk",
        title="inv.receipts.load_id → transport.loads",
        sql="""
            SELECT r.receipt_id, r.load_id
            FROM inv.receipts r
            WHERE r.load_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM transport.loads l WHERE l.load_id = r.load_id)
              AND r.received_dt::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-05",
        dimension="hard_fk",
        title="inv.receipt_items.product_id → pos.products",
        sql="""
            SELECT ri.receipt_item_id, ri.product_id
            FROM inv.receipt_items ri
            JOIN inv.receipts r ON r.receipt_id = ri.receipt_id
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = ri.product_id)
              AND r.received_dt::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-06",
        dimension="hard_fk",
        title="inv.shrinkage_events.product_id → pos.products",
        sql="""
            SELECT se.shrinkage_id, se.product_id
            FROM inv.shrinkage_events se
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = se.product_id)
              AND se.recorded_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-07",
        dimension="hard_fk",
        title="inv.shrinkage_events.location_id → hr.locations",
        sql="""
            SELECT se.shrinkage_id, se.location_id
            FROM inv.shrinkage_events se
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = se.location_id)
              AND se.recorded_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-08",
        dimension="hard_fk",
        title="pos.transaction_items.product_id → pos.products",
        sql="""
            SELECT ti.item_id, ti.product_id
            FROM pos.transaction_items ti
            JOIN pos.transactions t ON t.transaction_id = ti.transaction_id
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = ti.product_id)
              AND t.transaction_dt::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-09",
        dimension="hard_fk",
        title="pos.transactions.location_id → hr.locations",
        sql="""
            SELECT t.transaction_id, t.location_id
            FROM pos.transactions t
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = t.location_id)
              AND t.transaction_dt::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-10",
        dimension="hard_fk",
        title="pos.transactions.employee_id → hr.employees",
        sql="""
            SELECT t.transaction_id, t.employee_id
            FROM pos.transactions t
            WHERE t.employee_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM hr.employees e WHERE e.employee_id = t.employee_id)
              AND t.transaction_dt::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-11",
        dimension="hard_fk",
        title="pos.transactions.member_id → pos.loyalty_members",
        sql="""
            SELECT t.transaction_id, t.member_id
            FROM pos.transactions t
            WHERE t.member_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM pos.loyalty_members m WHERE m.member_id = t.member_id)
              AND t.transaction_dt::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-12",
        dimension="hard_fk",
        title="timeclock.events.employee_id → hr.employees",
        sql="""
            SELECT e.event_id, e.employee_id
            FROM timeclock.events e
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.employees emp WHERE emp.employee_id = e.employee_id)
              AND e.event_dt::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-13",
        dimension="hard_fk",
        title="timeclock.events.location_id → hr.locations",
        sql="""
            SELECT e.event_id, e.location_id
            FROM timeclock.events e
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = e.location_id)
              AND e.event_dt::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-14",
        dimension="hard_fk",
        title="ordering.store_orders.store_location_id → hr.locations",
        sql="""
            SELECT so.order_id, so.store_location_id
            FROM ordering.store_orders so
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = so.store_location_id)
              AND so.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-15",
        dimension="hard_fk",
        title="ordering.store_orders.warehouse_location_id → hr.locations",
        sql="""
            SELECT so.order_id, so.warehouse_location_id
            FROM ordering.store_orders so
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = so.warehouse_location_id)
              AND so.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-16",
        dimension="hard_fk",
        title="fulfillment.orders.store_order_id → ordering.store_orders",
        sql="""
            SELECT fo.fulfillment_id, fo.store_order_id
            FROM fulfillment.orders fo
            WHERE NOT EXISTS (
                SELECT 1 FROM ordering.store_orders so WHERE so.order_id = fo.store_order_id)
              AND fo.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-17",
        dimension="hard_fk",
        title="transport.load_items.store_order_id → ordering.store_orders",
        sql="""
            SELECT li.item_id, li.store_order_id
            FROM transport.load_items li
            WHERE li.store_order_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM ordering.store_orders so WHERE so.order_id = li.store_order_id)
              AND li.load_id IN (
                SELECT load_id FROM transport.loads WHERE created_at::date < CURRENT_DATE)
        """,
    ),
    AssertionSpec(
        id="HARD-18",
        dimension="hard_fk",
        title="transport.load_items.fulfillment_id → fulfillment.orders",
        sql="""
            SELECT li.item_id, li.fulfillment_id
            FROM transport.load_items li
            WHERE li.fulfillment_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM fulfillment.orders fo WHERE fo.fulfillment_id = li.fulfillment_id)
              AND li.load_id IN (
                SELECT load_id FROM transport.loads WHERE created_at::date < CURRENT_DATE)
        """,
    ),
    AssertionSpec(
        id="HARD-19",
        dimension="hard_fk",
        title="transport.loads.truck_id → transport.trucks",
        sql="""
            SELECT l.load_id, l.truck_id
            FROM transport.loads l
            WHERE NOT EXISTS (
                SELECT 1 FROM transport.trucks tr WHERE tr.truck_id = l.truck_id)
              AND l.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-20",
        dimension="hard_fk",
        title="hr.schedules.employee_id → hr.employees",
        sql="""
            SELECT s.schedule_id, s.employee_id
            FROM hr.schedules s
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.employees e WHERE e.employee_id = s.employee_id)
              AND s.scheduled_date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-21",
        dimension="hard_fk",
        title="hr.schedules.location_id → hr.locations",
        sql="""
            SELECT s.schedule_id, s.location_id
            FROM hr.schedules s
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = s.location_id)
              AND s.scheduled_date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="HARD-22",
        dimension="hard_fk",
        title="pricing.ad_items.product_id → pos.products",
        sql="""
            SELECT ai.ad_item_id, ai.product_id
            FROM pricing.ad_items ai
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = ai.product_id)
        """,
    ),

    # ---- SEMANTIC TYPE MISMATCHES (NOT FK-enforced) ----------------------
    AssertionSpec(
        id="SEMA-01",
        dimension="semantic_type",
        title="ordering.store_orders.store_location_id must be a STORE location",
        sql="""
            SELECT so.order_id, so.store_location_id, l.location_type
            FROM ordering.store_orders so
            JOIN hr.locations l ON l.location_id = so.store_location_id
            WHERE l.location_type <> 'store'
              AND so.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="SEMA-02",
        dimension="semantic_type",
        title="ordering.store_orders.warehouse_location_id must be WAREHOUSE/DC",
        sql="""
            SELECT so.order_id, so.warehouse_location_id, l.location_type
            FROM ordering.store_orders so
            JOIN hr.locations l ON l.location_id = so.warehouse_location_id
            WHERE l.location_type NOT IN ('warehouse', 'dc')
              AND so.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="SEMA-03",
        dimension="semantic_type",
        title="transport.loads.warehouse_location_id must be WAREHOUSE/DC",
        sql="""
            SELECT l.load_id, l.warehouse_location_id, loc.location_type
            FROM transport.loads l
            JOIN hr.locations loc ON loc.location_id = l.warehouse_location_id
            WHERE loc.location_type NOT IN ('warehouse', 'dc')
              AND l.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="SEMA-04",
        dimension="semantic_type",
        title="transport.loads.destination_location_id must be a STORE location",
        sql="""
            SELECT l.load_id, l.destination_location_id, loc.location_type
            FROM transport.loads l
            JOIN hr.locations loc ON loc.location_id = l.destination_location_id
            WHERE loc.location_type <> 'store'
              AND l.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="SEMA-05",
        dimension="semantic_type",
        title="fulfillment.orders.warehouse_location_id must be WAREHOUSE/DC",
        sql="""
            SELECT fo.fulfillment_id, fo.warehouse_location_id, loc.location_type
            FROM fulfillment.orders fo
            JOIN hr.locations loc ON loc.location_id = fo.warehouse_location_id
            WHERE loc.location_type NOT IN ('warehouse', 'dc')
              AND fo.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="SEMA-06",
        dimension="semantic_type",
        title="transport.loads.driver_id must be a TRANSPORT-department employee at a warehouse",
        sql="""
            SELECT l.load_id, l.driver_id, e.department, el.location_type
            FROM transport.loads l
            JOIN hr.employees e ON e.employee_id = l.driver_id
            JOIN hr.locations el ON el.location_id = e.location_id
            WHERE (e.department <> 'transport' OR el.location_type <> 'warehouse')
              AND l.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="SEMA-07",
        dimension="semantic_type",
        title="fulfillment.orders.assigned_to must be a WAREHOUSE-department employee at a warehouse",
        sql="""
            SELECT fo.fulfillment_id, fo.assigned_to, e.department, el.location_type
            FROM fulfillment.orders fo
            JOIN hr.employees e ON e.employee_id = fo.assigned_to
            JOIN hr.locations el ON el.location_id = e.location_id
            WHERE (e.department <> 'warehouse' OR el.location_type <> 'warehouse')
              AND fo.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="SEMA-08",
        dimension="semantic_type",
        title="ordering.store_orders.created_by must be a MANAGEMENT employee",
        sql="""
            SELECT so.order_id, so.created_by, e.department
            FROM ordering.store_orders so
            JOIN hr.employees e ON e.employee_id = so.created_by
            WHERE e.department <> 'management'
              AND so.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="SEMA-09",
        dimension="semantic_type",
        title="ordering.store_orders.approved_by must be a MANAGEMENT employee",
        sql="""
            SELECT so.order_id, so.approved_by, e.department
            FROM ordering.store_orders so
            JOIN hr.employees e ON e.employee_id = so.approved_by
            WHERE e.department <> 'management'
              AND so.created_at::date < CURRENT_DATE
        """,
    ),
    AssertionSpec(
        id="SEMA-10",
        dimension="semantic_type",
        title="transport.load_items reconcile: each load_item.fulfillment_id exists",
        sql="""
            SELECT li.item_id, li.load_id, li.fulfillment_id
            FROM transport.load_items li
            WHERE li.fulfillment_id IS NOT NULL
              AND li.load_id IN (
                SELECT load_id FROM transport.loads WHERE created_at::date < CURRENT_DATE)
              AND NOT EXISTS (
                SELECT 1 FROM fulfillment.items fi
                WHERE fi.fulfillment_id = li.fulfillment_id)
        """,
    ),
]


def by_dimension(dimension: str) -> List[AssertionSpec]:
    return [a for a in ASSERTIONS if a.dimension == dimension]


def run_assertion(conn, spec: AssertionSpec) -> List[tuple]:
    """Execute one assertion; return the list of orphan rows (empty == pass).

    Each assertion runs in its own transaction and is rolled back afterwards
    (the queries are read-only). Rolling back also clears an aborted-transaction
    state if an assertion's SQL is malformed, so one bad query cannot poison the
    shared connection for subsequent assertions.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(spec.sql)
            rows = cur.fetchall()
    except Exception:
        conn.rollback()
        raise
    conn.rollback()  # close the read-only transaction
    return rows


def run_all(conn) -> List[Tuple[AssertionSpec, List[tuple]]]:
    """Run every assertion. Returns [(spec, orphan_rows), ...]."""
    return [(spec, run_assertion(conn, spec)) for spec in ASSERTIONS]


def summarize(results) -> dict:
    total = len(results)
    failed = [(spec, rows) for spec, rows in results if rows]
    hard_failed = [spec.id for spec, rows in failed if spec.dimension == "hard_fk"]
    sema_failed = [spec.id for spec, rows in failed if spec.dimension == "semantic_type"]
    return {
        "total": total,
        "passed": total - len(failed),
        "failed": len(failed),
        "failed_ids": [spec.id for spec, _ in failed],
        "hard_failed": hard_failed,
        "semantic_failed": sema_failed,
    }


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if not argv:
        print("usage: cross_schema_integrity.py <postgres-dsn>", file=sys.stderr)
        return 2
    import psycopg2

    dsn = argv[0]
    conn = psycopg2.connect(dsn)
    try:
        results = run_all(conn)
    finally:
        conn.close()

    summary = summarize(results)
    print(f"Cross-schema integrity: {summary['passed']}/{summary['total']} passed")
    if summary["failed"]:
        for spec, rows in results:
            if rows:
                print(f"  FAIL {spec.id} [{spec.dimension}] {spec.title} "
                      f"({len(rows)} orphan(s))")
        return 1
    print("OK — no cross-schema orphans.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
