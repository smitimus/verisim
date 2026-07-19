-- =============================================================================
-- Verisim Grocery — Cross-schema referential integrity checks (Verisim #11)
-- =============================================================================
--
-- Each block returns the OFFENDING child rows for one cross-schema link.
-- Zero rows returned == integrity holds.
--
-- Two classes:
--   hard_fk      : child key references a parent row that does not exist
--                  (Postgres FK constraints normally prevent these).
--   semantic_type: key exists but points at the WRONG KIND of parent
--                  (NOT FK-enforced — highest-risk gaps).
--
-- Partial-day tolerance: time-bounded tables filter to completed days
-- (col::date < CURRENT_DATE) because the supply-chain block only runs at
-- the hour-0 midnight boundary; a partial backfill day legitimately lacks
-- downstream rows.
--
-- This file is generated from grocery/generator/tests/cross_schema_integrity.py
-- (ASSERTIONS). Do not edit by hand — edit the module and regenerate.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- HARD FK ORPHANS (22 checks)
-- ---------------------------------------------------------------------------

-- [HARD-01] inv.stock_levels.location_id → hr.locations
SELECT sl.stock_id, sl.location_id
            FROM inv.stock_levels sl
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = sl.location_id)
;

-- [HARD-02] inv.stock_levels.product_id → pos.products
SELECT sl.stock_id, sl.product_id
            FROM inv.stock_levels sl
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = sl.product_id)
;

-- [HARD-03] inv.receipts.location_id → hr.locations
SELECT r.receipt_id, r.location_id
            FROM inv.receipts r
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = r.location_id)
              AND r.received_dt::date < CURRENT_DATE
;

-- [HARD-04] inv.receipts.load_id → transport.loads
SELECT r.receipt_id, r.load_id
            FROM inv.receipts r
            WHERE r.load_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM transport.loads l WHERE l.load_id = r.load_id)
              AND r.received_dt::date < CURRENT_DATE
;

-- [HARD-05] inv.receipt_items.product_id → pos.products
SELECT ri.receipt_item_id, ri.product_id
            FROM inv.receipt_items ri
            JOIN inv.receipts r ON r.receipt_id = ri.receipt_id
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = ri.product_id)
              AND r.received_dt::date < CURRENT_DATE
;

-- [HARD-06] inv.shrinkage_events.product_id → pos.products
SELECT se.shrinkage_id, se.product_id
            FROM inv.shrinkage_events se
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = se.product_id)
              AND se.recorded_at::date < CURRENT_DATE
;

-- [HARD-07] inv.shrinkage_events.location_id → hr.locations
SELECT se.shrinkage_id, se.location_id
            FROM inv.shrinkage_events se
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = se.location_id)
              AND se.recorded_at::date < CURRENT_DATE
;

-- [HARD-08] pos.transaction_items.product_id → pos.products
SELECT ti.item_id, ti.product_id
            FROM pos.transaction_items ti
            JOIN pos.transactions t ON t.transaction_id = ti.transaction_id
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = ti.product_id)
              AND t.transaction_dt::date < CURRENT_DATE
;

-- [HARD-09] pos.transactions.location_id → hr.locations
SELECT t.transaction_id, t.location_id
            FROM pos.transactions t
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = t.location_id)
              AND t.transaction_dt::date < CURRENT_DATE
;

-- [HARD-10] pos.transactions.employee_id → hr.employees
SELECT t.transaction_id, t.employee_id
            FROM pos.transactions t
            WHERE t.employee_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM hr.employees e WHERE e.employee_id = t.employee_id)
              AND t.transaction_dt::date < CURRENT_DATE
;

-- [HARD-11] pos.transactions.member_id → pos.loyalty_members
SELECT t.transaction_id, t.member_id
            FROM pos.transactions t
            WHERE t.member_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM pos.loyalty_members m WHERE m.member_id = t.member_id)
              AND t.transaction_dt::date < CURRENT_DATE
;

-- [HARD-12] timeclock.events.employee_id → hr.employees
SELECT e.event_id, e.employee_id
            FROM timeclock.events e
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.employees emp WHERE emp.employee_id = e.employee_id)
              AND e.event_dt::date < CURRENT_DATE
;

-- [HARD-13] timeclock.events.location_id → hr.locations
SELECT e.event_id, e.location_id
            FROM timeclock.events e
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = e.location_id)
              AND e.event_dt::date < CURRENT_DATE
;

-- [HARD-14] ordering.store_orders.store_location_id → hr.locations
SELECT so.order_id, so.store_location_id
            FROM ordering.store_orders so
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = so.store_location_id)
              AND so.created_at::date < CURRENT_DATE
;

-- [HARD-15] ordering.store_orders.warehouse_location_id → hr.locations
SELECT so.order_id, so.warehouse_location_id
            FROM ordering.store_orders so
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = so.warehouse_location_id)
              AND so.created_at::date < CURRENT_DATE
;

-- [HARD-16] fulfillment.orders.store_order_id → ordering.store_orders
SELECT fo.fulfillment_id, fo.store_order_id
            FROM fulfillment.orders fo
            WHERE NOT EXISTS (
                SELECT 1 FROM ordering.store_orders so WHERE so.order_id = fo.store_order_id)
              AND fo.created_at::date < CURRENT_DATE
;

-- [HARD-17] transport.load_items.store_order_id → ordering.store_orders
SELECT li.item_id, li.store_order_id
            FROM transport.load_items li
            WHERE li.store_order_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM ordering.store_orders so WHERE so.order_id = li.store_order_id)
              AND li.load_id IN (
                SELECT load_id FROM transport.loads WHERE created_at::date < CURRENT_DATE)
;

-- [HARD-18] transport.load_items.fulfillment_id → fulfillment.orders
SELECT li.item_id, li.fulfillment_id
            FROM transport.load_items li
            WHERE li.fulfillment_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM fulfillment.orders fo WHERE fo.fulfillment_id = li.fulfillment_id)
              AND li.load_id IN (
                SELECT load_id FROM transport.loads WHERE created_at::date < CURRENT_DATE)
;

-- [HARD-19] transport.loads.truck_id → transport.trucks
SELECT l.load_id, l.truck_id
            FROM transport.loads l
            WHERE NOT EXISTS (
                SELECT 1 FROM transport.trucks tr WHERE tr.truck_id = l.truck_id)
              AND l.created_at::date < CURRENT_DATE
;

-- [HARD-20] hr.schedules.employee_id → hr.employees
SELECT s.schedule_id, s.employee_id
            FROM hr.schedules s
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.employees e WHERE e.employee_id = s.employee_id)
              AND s.scheduled_date < CURRENT_DATE
;

-- [HARD-21] hr.schedules.location_id → hr.locations
SELECT s.schedule_id, s.location_id
            FROM hr.schedules s
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = s.location_id)
              AND s.scheduled_date < CURRENT_DATE
;

-- [HARD-22] pricing.ad_items.product_id → pos.products
SELECT ai.ad_item_id, ai.product_id
            FROM pricing.ad_items ai
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = ai.product_id)
;


-- ---------------------------------------------------------------------------
-- SEMANTIC TYPE MISMATCHES (10 checks)
-- ---------------------------------------------------------------------------

-- [SEMA-01] ordering.store_orders.store_location_id must be a STORE location
SELECT so.order_id, so.store_location_id, l.location_type
            FROM ordering.store_orders so
            JOIN hr.locations l ON l.location_id = so.store_location_id
            WHERE l.location_type <> 'store'
              AND so.created_at::date < CURRENT_DATE
;

-- [SEMA-02] ordering.store_orders.warehouse_location_id must be WAREHOUSE/DC
SELECT so.order_id, so.warehouse_location_id, l.location_type
            FROM ordering.store_orders so
            JOIN hr.locations l ON l.location_id = so.warehouse_location_id
            WHERE l.location_type NOT IN ('warehouse', 'dc')
              AND so.created_at::date < CURRENT_DATE
;

-- [SEMA-03] transport.loads.warehouse_location_id must be WAREHOUSE/DC
SELECT l.load_id, l.warehouse_location_id, loc.location_type
            FROM transport.loads l
            JOIN hr.locations loc ON loc.location_id = l.warehouse_location_id
            WHERE loc.location_type NOT IN ('warehouse', 'dc')
              AND l.created_at::date < CURRENT_DATE
;

-- [SEMA-04] transport.loads.destination_location_id must be a STORE location
SELECT l.load_id, l.destination_location_id, loc.location_type
            FROM transport.loads l
            JOIN hr.locations loc ON loc.location_id = l.destination_location_id
            WHERE loc.location_type <> 'store'
              AND l.created_at::date < CURRENT_DATE
;

-- [SEMA-05] fulfillment.orders.warehouse_location_id must be WAREHOUSE/DC
SELECT fo.fulfillment_id, fo.warehouse_location_id, loc.location_type
            FROM fulfillment.orders fo
            JOIN hr.locations loc ON loc.location_id = fo.warehouse_location_id
            WHERE loc.location_type NOT IN ('warehouse', 'dc')
              AND fo.created_at::date < CURRENT_DATE
;

-- [SEMA-06] transport.loads.driver_id must be a TRANSPORT-department employee at a warehouse
SELECT l.load_id, l.driver_id, e.department, el.location_type
            FROM transport.loads l
            JOIN hr.employees e ON e.employee_id = l.driver_id
            JOIN hr.locations el ON el.location_id = e.location_id
            WHERE (e.department <> 'transport' OR el.location_type <> 'warehouse')
              AND l.created_at::date < CURRENT_DATE
;

-- [SEMA-07] fulfillment.orders.assigned_to must be a WAREHOUSE-department employee at a warehouse
SELECT fo.fulfillment_id, fo.assigned_to, e.department, el.location_type
            FROM fulfillment.orders fo
            JOIN hr.employees e ON e.employee_id = fo.assigned_to
            JOIN hr.locations el ON el.location_id = e.location_id
            WHERE (e.department <> 'warehouse' OR el.location_type <> 'warehouse')
              AND fo.created_at::date < CURRENT_DATE
;

-- [SEMA-08] ordering.store_orders.created_by must be a MANAGEMENT employee
SELECT so.order_id, so.created_by, e.department
            FROM ordering.store_orders so
            JOIN hr.employees e ON e.employee_id = so.created_by
            WHERE e.department <> 'management'
              AND so.created_at::date < CURRENT_DATE
;

-- [SEMA-09] ordering.store_orders.approved_by must be a MANAGEMENT employee
SELECT so.order_id, so.approved_by, e.department
            FROM ordering.store_orders so
            JOIN hr.employees e ON e.employee_id = so.approved_by
            WHERE e.department <> 'management'
              AND so.created_at::date < CURRENT_DATE
;

-- [SEMA-10] transport.load_items reconcile: each load_item.fulfillment_id exists
SELECT li.item_id, li.load_id, li.fulfillment_id
            FROM transport.load_items li
            WHERE li.fulfillment_id IS NOT NULL
              AND li.load_id IN (
                SELECT load_id FROM transport.loads WHERE created_at::date < CURRENT_DATE)
              AND NOT EXISTS (
                SELECT 1 FROM fulfillment.items fi
                WHERE fi.fulfillment_id = li.fulfillment_id)
;

