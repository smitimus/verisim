-- =============================================================================
-- Verisim Grocery — Cross-schema referential integrity checks (Verisim #11)
-- =============================================================================
--
-- Each block returns the OFFENDING child rows for one cross-schema link.
-- Zero rows returned == integrity holds.
--
-- Three classes:
--   hard_fk      : child key references a parent row that does not exist
--                  (Postgres FK constraints normally prevent these).
--   semantic_type: key exists but points at the WRONG KIND of parent
--                  (NOT FK-enforced — highest-risk gaps).
--   temporal     : key and parent both exist but the parent's validity window
--                  does not cover the child's timestamp (coupon/deal-tagged
--                  line items outside valid_from..valid_until).
--
-- Partial-day tolerance: time-bounded tables filter to completed days
-- (col::date < CURRENT_DATE) because the supply-chain block only runs at
-- the hour-0 midnight boundary; a partial backfill day legitimately lacks
-- downstream rows.
--
-- This file is generated from grocery/generator/tests/cross_schema_integrity.py
-- (ASSERTIONS). Do not edit by hand — edit the module and regenerate:
--     python3 grocery/generator/scripts/regen_integrity_sql.py
-- =============================================================================


-- --------------------------------------------------------------------------
-- HARD_FK — inv.stock_levels.location_id → hr.locations
-- --------------------------------------------------------------------------

-- [HARD-01] inv.stock_levels.location_id → hr.locations
SELECT sl.stock_id, sl.location_id
            FROM inv.stock_levels sl
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = sl.location_id);

-- [HARD-02] inv.stock_levels.product_id → pos.products
SELECT sl.stock_id, sl.product_id
            FROM inv.stock_levels sl
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = sl.product_id);

-- [HARD-03] inv.receipts.location_id → hr.locations
SELECT r.receipt_id, r.location_id
            FROM inv.receipts r
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = r.location_id)
              AND r.received_dt::date < CURRENT_DATE;

-- [HARD-04] inv.receipts.load_id → transport.loads
SELECT r.receipt_id, r.load_id
            FROM inv.receipts r
            WHERE r.load_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM transport.loads l WHERE l.load_id = r.load_id)
              AND r.received_dt::date < CURRENT_DATE;

-- [HARD-05] inv.receipt_items.product_id → pos.products
SELECT ri.receipt_item_id, ri.product_id
            FROM inv.receipt_items ri
            JOIN inv.receipts r ON r.receipt_id = ri.receipt_id
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = ri.product_id)
              AND r.received_dt::date < CURRENT_DATE;

-- [HARD-06] inv.shrinkage_events.product_id → pos.products
SELECT se.shrinkage_id, se.product_id
            FROM inv.shrinkage_events se
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = se.product_id)
              AND se.recorded_at::date < CURRENT_DATE;

-- [HARD-07] inv.shrinkage_events.location_id → hr.locations
SELECT se.shrinkage_id, se.location_id
            FROM inv.shrinkage_events se
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = se.location_id)
              AND se.recorded_at::date < CURRENT_DATE;

-- [HARD-08] pos.transaction_items.product_id → pos.products
SELECT ti.item_id, ti.product_id
            FROM pos.transaction_items ti
            JOIN pos.transactions t ON t.transaction_id = ti.transaction_id
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = ti.product_id)
              AND t.transaction_dt::date < CURRENT_DATE;

-- [HARD-09] pos.transactions.location_id → hr.locations
SELECT t.transaction_id, t.location_id
            FROM pos.transactions t
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = t.location_id)
              AND t.transaction_dt::date < CURRENT_DATE;

-- [HARD-10] pos.transactions.employee_id → hr.employees
SELECT t.transaction_id, t.employee_id
            FROM pos.transactions t
            WHERE t.employee_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM hr.employees e WHERE e.employee_id = t.employee_id)
              AND t.transaction_dt::date < CURRENT_DATE;

-- [HARD-11] pos.transactions.member_id → pos.loyalty_members
SELECT t.transaction_id, t.member_id
            FROM pos.transactions t
            WHERE t.member_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM pos.loyalty_members m WHERE m.member_id = t.member_id)
              AND t.transaction_dt::date < CURRENT_DATE;

-- [HARD-12] timeclock.events.employee_id → hr.employees
SELECT e.event_id, e.employee_id
            FROM timeclock.events e
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.employees emp WHERE emp.employee_id = e.employee_id)
              AND e.event_dt::date < CURRENT_DATE;

-- [HARD-13] timeclock.events.location_id → hr.locations
SELECT e.event_id, e.location_id
            FROM timeclock.events e
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = e.location_id)
              AND e.event_dt::date < CURRENT_DATE;

-- [HARD-14] ordering.store_orders.store_location_id → hr.locations
SELECT so.order_id, so.store_location_id
            FROM ordering.store_orders so
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = so.store_location_id)
              AND so.created_at::date < CURRENT_DATE;

-- [HARD-15] ordering.store_orders.warehouse_location_id → hr.locations
SELECT so.order_id, so.warehouse_location_id
            FROM ordering.store_orders so
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = so.warehouse_location_id)
              AND so.created_at::date < CURRENT_DATE;

-- [HARD-16] fulfillment.orders.store_order_id → ordering.store_orders
SELECT fo.fulfillment_id, fo.store_order_id
            FROM fulfillment.orders fo
            WHERE NOT EXISTS (
                SELECT 1 FROM ordering.store_orders so WHERE so.order_id = fo.store_order_id)
              AND fo.created_at::date < CURRENT_DATE;

-- [HARD-17] transport.load_items.store_order_id → ordering.store_orders
SELECT li.item_id, li.store_order_id
            FROM transport.load_items li
            WHERE li.store_order_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM ordering.store_orders so WHERE so.order_id = li.store_order_id)
              AND li.load_id IN (
                SELECT load_id FROM transport.loads WHERE created_at::date < CURRENT_DATE);

-- [HARD-18] transport.load_items.fulfillment_id → fulfillment.orders
SELECT li.item_id, li.fulfillment_id
            FROM transport.load_items li
            WHERE li.fulfillment_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM fulfillment.orders fo WHERE fo.fulfillment_id = li.fulfillment_id)
              AND li.load_id IN (
                SELECT load_id FROM transport.loads WHERE created_at::date < CURRENT_DATE);

-- [HARD-19] transport.loads.truck_id → transport.trucks
SELECT l.load_id, l.truck_id
            FROM transport.loads l
            WHERE NOT EXISTS (
                SELECT 1 FROM transport.trucks tr WHERE tr.truck_id = l.truck_id)
              AND l.created_at::date < CURRENT_DATE;

-- [HARD-20] hr.schedules.employee_id → hr.employees
SELECT s.schedule_id, s.employee_id
            FROM hr.schedules s
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.employees e WHERE e.employee_id = s.employee_id)
              AND s.scheduled_date < CURRENT_DATE;

-- [HARD-21] hr.schedules.location_id → hr.locations
SELECT s.schedule_id, s.location_id
            FROM hr.schedules s
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = s.location_id)
              AND s.scheduled_date < CURRENT_DATE;

-- [HARD-22] pricing.ad_items.product_id → pos.products
SELECT ai.ad_item_id, ai.product_id
            FROM pricing.ad_items ai
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = ai.product_id);

-- [HARD-23] inv.stockout_events.product_id → pos.products
SELECT se.stockout_id, se.product_id
            FROM inv.stockout_events se
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = se.product_id)
              AND se.event_dt::date < CURRENT_DATE;

-- [HARD-24] inv.stockout_events.location_id → hr.locations
SELECT se.stockout_id, se.location_id
            FROM inv.stockout_events se
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = se.location_id)
              AND se.event_dt::date < CURRENT_DATE;

-- [HARD-25] inv.stockout_events.pos_transaction_id → pos.transactions (when channel='pos')
SELECT se.stockout_id, se.pos_transaction_id
            FROM inv.stockout_events se
            WHERE se.channel = 'pos'
              AND se.pos_transaction_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM pos.transactions t
                WHERE t.transaction_id = se.pos_transaction_id)
              AND se.event_dt::date < CURRENT_DATE;

-- [HARD-26] inv.stockout_events.online_order_id → online.orders (when channel='online')
SELECT se.stockout_id, se.online_order_id
            FROM inv.stockout_events se
            WHERE se.channel = 'online'
              AND se.online_order_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM online.orders o
                WHERE o.order_id = se.online_order_id)
              AND se.event_dt::date < CURRENT_DATE;

-- [HARD-27] inv.sku_demand_daily.product_id → pos.products
SELECT sd.product_id, sd.demand_date
            FROM inv.sku_demand_daily sd
            WHERE NOT EXISTS (
                SELECT 1 FROM pos.products p WHERE p.product_id = sd.product_id);

-- [HARD-28] inv.sku_demand_daily.location_id → hr.locations
SELECT sd.location_id, sd.demand_date
            FROM inv.sku_demand_daily sd
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = sd.location_id);

-- [HARD-29] inv.products.supplier_id → inv.suppliers
SELECT ip.inv_product_id, ip.supplier_id
            FROM inv.products ip
            WHERE ip.supplier_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM inv.suppliers s
                WHERE s.supplier_id = ip.supplier_id);

-- [HARD-30] inv.receipts.supplier_id → inv.suppliers
SELECT r.receipt_id, r.supplier_id
            FROM inv.receipts r
            WHERE r.supplier_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM inv.suppliers s
                WHERE s.supplier_id = r.supplier_id);

-- [HARD-31] inv.supplier_delivery_schedules.supplier_id → inv.suppliers
SELECT ds.schedule_id, ds.supplier_id
            FROM inv.supplier_delivery_schedules ds
            WHERE NOT EXISTS (
                SELECT 1 FROM inv.suppliers s
                WHERE s.supplier_id = ds.supplier_id);

-- [HARD-32] inv.short_ship_events.fulfillment_item_id → fulfillment.items
SELECT se.short_ship_id, se.fulfillment_item_id
            FROM inv.short_ship_events se
            WHERE NOT EXISTS (
                SELECT 1 FROM fulfillment.items fi
                WHERE fi.item_id = se.fulfillment_item_id)
              AND se.event_dt::date < CURRENT_DATE;

-- [HARD-33] inv.short_ship_events.supplier_id → inv.suppliers
SELECT se.short_ship_id, se.supplier_id
            FROM inv.short_ship_events se
            WHERE NOT EXISTS (
                SELECT 1 FROM inv.suppliers s
                WHERE s.supplier_id = se.supplier_id)
              AND se.event_dt::date < CURRENT_DATE;

-- [HARD-34] inv.short_ship_events.location_id → hr.locations
SELECT se.short_ship_id, se.location_id
            FROM inv.short_ship_events se
            WHERE NOT EXISTS (
                SELECT 1 FROM hr.locations l WHERE l.location_id = se.location_id)
              AND se.event_dt::date < CURRENT_DATE;

-- [HARD-35] inv.supplier_credit_memos.short_ship_id → inv.short_ship_events
SELECT m.credit_memo_id, m.short_ship_id
            FROM inv.supplier_credit_memos m
            WHERE NOT EXISTS (
                SELECT 1 FROM inv.short_ship_events se
                WHERE se.short_ship_id = m.short_ship_id);

-- [HARD-36] inv.supplier_credit_memos.supplier_id → inv.suppliers
SELECT m.credit_memo_id, m.supplier_id
            FROM inv.supplier_credit_memos m
            WHERE NOT EXISTS (
                SELECT 1 FROM inv.suppliers s
                WHERE s.supplier_id = m.supplier_id);

-- [HARD-37] inv.dsd_deliveries.schedule_id → inv.supplier_delivery_schedules
SELECT d.dsd_delivery_id, d.schedule_id
            FROM inv.dsd_deliveries d
            WHERE NOT EXISTS (
                SELECT 1 FROM inv.supplier_delivery_schedules ds
                WHERE ds.schedule_id = d.schedule_id)
              AND d.delivery_date < CURRENT_DATE;

-- [HARD-38] inv.dsd_delivery_items.dsd_delivery_id → inv.dsd_deliveries
SELECT i.dsd_item_id, i.dsd_delivery_id
            FROM inv.dsd_delivery_items i
            WHERE NOT EXISTS (
                SELECT 1 FROM inv.dsd_deliveries d
                WHERE d.dsd_delivery_id = i.dsd_delivery_id);

-- [HARD-39] inv.dsd_deliveries.supplier_id → inv.suppliers, and it must be a DSD vendor
SELECT d.dsd_delivery_id, d.supplier_id, s.fulfillment_model
            FROM inv.dsd_deliveries d
            JOIN inv.suppliers s ON s.supplier_id = d.supplier_id
            WHERE s.fulfillment_model <> 'dsd'
              AND d.delivery_date < CURRENT_DATE;

-- --------------------------------------------------------------------------
-- SEMANTIC_TYPE — ordering.store_orders.store_location_id must be a STORE location
-- --------------------------------------------------------------------------

-- [SEMA-01] ordering.store_orders.store_location_id must be a STORE location
SELECT so.order_id, so.store_location_id, l.location_type
            FROM ordering.store_orders so
            JOIN hr.locations l ON l.location_id = so.store_location_id
            WHERE l.location_type <> 'store'
              AND so.created_at::date < CURRENT_DATE;

-- [SEMA-02] ordering.store_orders.warehouse_location_id must be WAREHOUSE/DC
SELECT so.order_id, so.warehouse_location_id, l.location_type
            FROM ordering.store_orders so
            JOIN hr.locations l ON l.location_id = so.warehouse_location_id
            WHERE l.location_type NOT IN ('warehouse', 'dc')
              AND so.created_at::date < CURRENT_DATE;

-- [SEMA-03] transport.loads.warehouse_location_id must be WAREHOUSE/DC
SELECT l.load_id, l.warehouse_location_id, loc.location_type
            FROM transport.loads l
            JOIN hr.locations loc ON loc.location_id = l.warehouse_location_id
            WHERE loc.location_type NOT IN ('warehouse', 'dc')
              AND l.created_at::date < CURRENT_DATE;

-- [SEMA-04] transport.loads.destination_location_id must be a STORE location
SELECT l.load_id, l.destination_location_id, loc.location_type
            FROM transport.loads l
            JOIN hr.locations loc ON loc.location_id = l.destination_location_id
            WHERE loc.location_type <> 'store'
              AND l.created_at::date < CURRENT_DATE;

-- [SEMA-05] fulfillment.orders.warehouse_location_id must be WAREHOUSE/DC
SELECT fo.fulfillment_id, fo.warehouse_location_id, loc.location_type
            FROM fulfillment.orders fo
            JOIN hr.locations loc ON loc.location_id = fo.warehouse_location_id
            WHERE loc.location_type NOT IN ('warehouse', 'dc')
              AND fo.created_at::date < CURRENT_DATE;

-- [SEMA-06] transport.loads.driver_id must be a TRANSPORT-department employee at a warehouse
SELECT l.load_id, l.driver_id, e.department, el.location_type
            FROM transport.loads l
            JOIN hr.employees e ON e.employee_id = l.driver_id
            JOIN hr.locations el ON el.location_id = e.location_id
            WHERE (e.department <> 'transport' OR el.location_type <> 'warehouse')
              AND l.created_at::date < CURRENT_DATE;

-- [SEMA-07] fulfillment.orders.assigned_to must be a WAREHOUSE-department employee at a warehouse
SELECT fo.fulfillment_id, fo.assigned_to, e.department, el.location_type
            FROM fulfillment.orders fo
            JOIN hr.employees e ON e.employee_id = fo.assigned_to
            JOIN hr.locations el ON el.location_id = e.location_id
            WHERE (e.department <> 'warehouse' OR el.location_type <> 'warehouse')
              AND fo.created_at::date < CURRENT_DATE;

-- [SEMA-08] ordering.store_orders.created_by must be a MANAGEMENT employee
SELECT so.order_id, so.created_by, e.department
            FROM ordering.store_orders so
            JOIN hr.employees e ON e.employee_id = so.created_by
            WHERE e.department <> 'management'
              AND so.created_at::date < CURRENT_DATE;

-- [SEMA-09] ordering.store_orders.approved_by must be a MANAGEMENT employee
SELECT so.order_id, so.approved_by, e.department
            FROM ordering.store_orders so
            JOIN hr.employees e ON e.employee_id = so.approved_by
            WHERE e.department <> 'management'
              AND so.created_at::date < CURRENT_DATE;

-- [SEMA-10] transport.load_items reconcile: each load_item.fulfillment_id exists
SELECT li.item_id, li.load_id, li.fulfillment_id
            FROM transport.load_items li
            WHERE li.fulfillment_id IS NOT NULL
              AND li.load_id IN (
                SELECT load_id FROM transport.loads WHERE created_at::date < CURRENT_DATE)
              AND NOT EXISTS (
                SELECT 1 FROM fulfillment.items fi
                WHERE fi.fulfillment_id = li.fulfillment_id);

-- [SEMA-11] inv.stockout_events: a POS row must not also carry an online_order_id (and vice versa)
SELECT se.stockout_id, se.channel, se.pos_transaction_id,
                   se.online_order_id
            FROM inv.stockout_events se
            WHERE se.event_dt::date < CURRENT_DATE
              AND ((se.channel = 'pos'     AND se.online_order_id IS NOT NULL)
                OR (se.channel = 'online' AND se.pos_transaction_id IS NOT NULL));

-- [SEMA-12] a parented POS stockout's product must have a rung-up line at exactly fulfilled_quantity
SELECT se.stockout_id, se.pos_transaction_id, se.fulfilled_quantity
            FROM inv.stockout_events se
            WHERE se.channel = 'pos'
              AND se.pos_transaction_id IS NOT NULL
              AND se.event_dt::date < CURRENT_DATE
              -- Only rows that were actually PART of the sale are checked. A
              -- basket can be partly fulfilled: the short product's line is
              -- dropped (quantity would be 0, which transaction_items forbids)
              -- while the parent's other lines are written normally. That is
              -- correct behaviour, so a stockout with fulfilled_quantity = 0
              -- legitimately has no matching line — and demanding one would
              -- assert the defect back into the model.
              AND se.fulfilled_quantity > 0
              AND NOT EXISTS (
                SELECT 1
                FROM pos.transaction_items ti
                WHERE ti.transaction_id = se.pos_transaction_id
                  AND ti.product_id = se.product_id
                  AND ti.quantity = se.fulfilled_quantity);

-- [SEMA-13] the daily demand ledger must reconcile (requested = fulfilled + lost) for every row
SELECT sd.location_id, sd.product_id, sd.demand_date,
                   sd.requested_units, sd.fulfilled_units, sd.lost_units
            FROM inv.sku_demand_daily sd
            WHERE sd.lost_units <> sd.requested_units - sd.fulfilled_units
               OR sd.requested_units < sd.fulfilled_units;

-- [SEMA-14] a short-ship's vendor must be the vendor that supplies the product
SELECT se.short_ship_id, se.product_id, se.supplier_id,
                   ip.supplier_id AS product_supplier_id
            FROM inv.short_ship_events se
            JOIN inv.products ip ON ip.product_id = se.product_id
            WHERE se.supplier_id IS DISTINCT FROM ip.supplier_id
              AND se.event_dt::date < CURRENT_DATE;

-- [SEMA-15] a short_ship_event's quantities must match the fulfillment line it came from
SELECT se.short_ship_id, se.quantity_requested, se.quantity_picked,
                   fi.quantity_requested AS fi_requested,
                   fi.quantity_picked AS fi_picked
            FROM inv.short_ship_events se
            JOIN fulfillment.items fi ON fi.item_id = se.fulfillment_item_id
            WHERE (se.quantity_requested <> fi.quantity_requested
               OR se.quantity_picked <> fi.quantity_picked)
              AND se.event_dt::date < CURRENT_DATE;

-- [SEMA-16] a DSD short-ship must be flagged as detected at a DSD delivery
SELECT se.short_ship_id, se.detected_source, s.fulfillment_model
            FROM inv.short_ship_events se
            JOIN inv.suppliers s ON s.supplier_id = se.supplier_id
            WHERE ((s.fulfillment_model = 'dsd' AND se.detected_source = 'receiving')
               OR (s.fulfillment_model = 'warehouse' AND se.detected_source = 'dsd_delivery'))
              AND se.event_dt::date < CURRENT_DATE;

-- [SEMA-17] a credit memo must exist only against a creditable short-ship
SELECT m.credit_memo_id, m.short_ship_id, se.is_creditable
            FROM inv.supplier_credit_memos m
            JOIN inv.short_ship_events se ON se.short_ship_id = m.short_ship_id
            WHERE NOT se.is_creditable;

-- [SEMA-18] a credit memo's vendor, product and location must match the short-ship's
SELECT m.credit_memo_id, m.supplier_id, se.supplier_id,
                   m.product_id, se.product_id, m.location_id, se.location_id
            FROM inv.supplier_credit_memos m
            JOIN inv.short_ship_events se ON se.short_ship_id = m.short_ship_id
            WHERE m.supplier_id IS DISTINCT FROM se.supplier_id
               OR m.product_id IS DISTINCT FROM se.product_id
               OR m.location_id IS DISTINCT FROM se.location_id;

-- [SEMA-19] a credit memo's amount must be the shortfall priced at the short-ship's cost
SELECT m.credit_memo_id, m.credit_amount, m.credit_quantity,
                   se.short_value, se.quantity_short
            FROM inv.supplier_credit_memos m
            JOIN inv.short_ship_events se ON se.short_ship_id = m.short_ship_id
            WHERE m.credit_quantity <> se.quantity_short
               OR m.credit_amount <> se.short_value;

-- [SEMA-20] a submitted claim must be dated on or before its claim_deadline
SELECT m.credit_memo_id, m.submitted_dt, m.claim_deadline
            FROM inv.supplier_credit_memos m
            WHERE m.submitted_dt IS NOT NULL
              AND m.submitted_dt::date > m.claim_deadline;

-- [SEMA-21] a resolved claim must resolve after it was submitted, never before
SELECT m.credit_memo_id, m.submitted_dt, m.resolved_dt
            FROM inv.supplier_credit_memos m
            WHERE m.submitted_dt IS NOT NULL
              AND m.resolved_dt IS NOT NULL
              AND m.resolved_dt < m.submitted_dt;

-- [SEMA-22] a short-ship must never be recorded against a line the warehouse fully picked
SELECT se.short_ship_id, se.quantity_picked, se.quantity_requested,
                   fi.pick_status
            FROM inv.short_ship_events se
            JOIN fulfillment.items fi ON fi.item_id = se.fulfillment_item_id
            WHERE fi.pick_status = 'picked'
              AND se.event_dt::date < CURRENT_DATE;

-- [SEMA-23] a short-ship's realized lead time must never be reported as negative
SELECT se.short_ship_id, se.realized_lead_time_days
            FROM inv.short_ship_events se
            WHERE se.realized_lead_time_days < 0
              AND se.event_dt::date < CURRENT_DATE;

-- [SEMA-24] a DSD delivery's item lines must reconcile with its header totals
SELECT d.dsd_delivery_id, d.total_units, d.total_value,
                   d.line_count, SUM(i.quantity_delivered) AS item_units,
                   SUM(i.line_total) AS item_value, COUNT(*) AS items
            FROM inv.dsd_deliveries d
            JOIN inv.dsd_delivery_items i
              ON i.dsd_delivery_id = d.dsd_delivery_id
            WHERE d.delivery_date < CURRENT_DATE
            GROUP BY d.dsd_delivery_id, d.total_units, d.total_value, d.line_count
            HAVING SUM(i.quantity_delivered) <> d.total_units
                OR ROUND(SUM(i.line_total), 2) <> ROUND(d.total_value, 2)
                OR COUNT(*) <> d.line_count;

-- [SEMA-25] a DSD delivery's supplier must match the schedule's supplier
SELECT d.dsd_delivery_id, d.supplier_id, ds.supplier_id AS schedule_supplier
            FROM inv.dsd_deliveries d
            JOIN inv.supplier_delivery_schedules ds ON ds.schedule_id = d.schedule_id
            WHERE d.supplier_id IS DISTINCT FROM ds.supplier_id
              AND d.delivery_date < CURRENT_DATE;

-- [SEMA-26] a DSD delivery must land on a weekday the schedule actually visits
SELECT d.dsd_delivery_id, d.delivery_date, ds.delivery_weekday
            FROM inv.dsd_deliveries d
            JOIN inv.supplier_delivery_schedules ds
              ON ds.schedule_id = d.schedule_id
            WHERE (EXTRACT(ISODOW FROM d.delivery_date)::int - 1 <> ds.delivery_weekday)
              AND d.delivery_date < CURRENT_DATE;

-- [SEMA-27] a product's denormalised supplier_name must agree with its vendor's name
SELECT ip.product_id, ip.supplier_name, s.supplier_name AS vendor_name
            FROM inv.products ip
            JOIN inv.suppliers s ON s.supplier_id = ip.supplier_id
            WHERE ip.supplier_name IS DISTINCT FROM s.supplier_name;

-- --------------------------------------------------------------------------
-- TEMPORAL — pos.transaction_items.coupon_id — transaction_dt must be inside the coupon's valid_from..valid_until
-- --------------------------------------------------------------------------

-- [TIME-01] pos.transaction_items.coupon_id — transaction_dt must be inside the coupon's valid_from..valid_until
SELECT ti.item_id, ti.coupon_id, t.transaction_dt::date,
                   c.valid_from, c.valid_until
            FROM pos.transaction_items ti
            JOIN pos.transactions t ON t.transaction_id = ti.transaction_id
            JOIN pos.coupons c ON c.coupon_id = ti.coupon_id
            WHERE ti.coupon_id IS NOT NULL
              AND t.transaction_dt::date NOT BETWEEN c.valid_from AND c.valid_until
              AND t.transaction_dt::date < CURRENT_DATE;

-- [TIME-02] pos.transaction_items.deal_id — transaction_dt must be inside the combo deal's valid_from..valid_until
SELECT ti.item_id, ti.deal_id, t.transaction_dt::date,
                   d.valid_from, d.valid_until
            FROM pos.transaction_items ti
            JOIN pos.transactions t ON t.transaction_id = ti.transaction_id
            JOIN pos.combo_deals d ON d.deal_id = ti.deal_id
            WHERE ti.deal_id IS NOT NULL
              AND t.transaction_dt::date NOT BETWEEN d.valid_from AND d.valid_until
              AND t.transaction_dt::date < CURRENT_DATE;
