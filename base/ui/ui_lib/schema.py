"""
The schema vocabulary: per-industry table docs, table lists, schema docs.

The largest data block in the control panel — column-by-column documentation for
every table in every industry, the schema -> table lists the Table Explorer and
Data Dictionary navigate, and the schema-level blurbs.

Pure data plus two tiny lookups. No Streamlit calls, so it imports (and tests)
without a running app.

Split out of `base/ui/app.py` (t_c2eca5dd).
"""
# Table Explorer — documentation for every table
# ---------------------------------------------------------------------------

GAS_STATION_TABLE_DOCS = {
    "hr.locations": {
        "title": "HR — Locations",
        "description": (
            "Source of truth for all physical store locations. Every transaction, "
            "employee, pump, and stock record links back to a location here."
        ),
        "columns": [
            ("location_id", "UUID PK", "Primary key — referenced by all other schemas"),
            ("name", "VARCHAR(100)", "Human-readable store name (e.g. 'Downtown Express #1')"),
            ("address / city / state / zip", "VARCHAR", "Full mailing address"),
            ("phone", "VARCHAR(20)", "Store phone number (nullable)"),
            ("opened_date", "DATE", "Date the location opened for business"),
            ("type", "VARCHAR(20)", "Store type: store (c-store only), fuel_only, or combo"),
            ("is_active", "BOOLEAN", "FALSE for closed or decommissioned locations"),
            ("created_at", "TIMESTAMPTZ", "Row creation timestamp"),
        ],
        "relationships": [
            "Referenced by hr.employees(location_id)",
            "Referenced by pos.transactions(location_id)",
            "Referenced by fuel.transactions(location_id)",
            "Referenced by fuel.pumps(location_id)",
            "Referenced by inv.stock_levels(location_id)",
        ],
        "notes": "Seeded once at startup. The generator creates 3–5 locations depending on config.",
    },
    "hr.employees": {
        "title": "HR — Employees",
        "description": "Master employee record. All other systems reference a person via this table.",
        "columns": [
            ("employee_id", "UUID PK", "Primary key"),
            ("location_id", "UUID FK → hr.locations", "Which store this employee works at"),
            ("first_name / last_name", "VARCHAR(100)", "Employee name (Faker-generated)"),
            ("email", "VARCHAR(255) UNIQUE", "Work email"),
            ("hire_date", "DATE", "Date hired"),
            ("termination_date", "DATE", "NULL if still employed"),
            ("department", "VARCHAR(50)", "One of: store, fuel, management"),
            ("job_title", "VARCHAR(100)", "Role within the department"),
            ("hourly_rate", "NUMERIC(8,2)", "Hourly pay rate"),
            ("status", "VARCHAR(20)", "active, terminated, or on_leave"),
            ("created_at / updated_at", "TIMESTAMPTZ", "Row audit timestamps"),
        ],
        "relationships": [
            "hr.employees.location_id → hr.locations.location_id",
            "Referenced by pos.transactions(employee_id)",
            "Referenced by fuel.transactions(employee_id)",
        ],
        "notes": "Generator probabilistically hires (~0.1%/tick) and terminates (~0.02%/tick) employees.",
    },
    "pos.transactions": {
        "title": "POS — Transactions",
        "description": "Every in-store purchase. Transaction header — line items in pos.transaction_items.",
        "columns": [
            ("transaction_id", "UUID PK", "Primary key"),
            ("location_id", "UUID FK → hr.locations", "Which store"),
            ("employee_id", "UUID FK → hr.employees", "Cashier (nullable)"),
            ("member_id", "UUID FK → pos.loyalty_members", "Loyalty member (nullable)"),
            ("transaction_dt", "TIMESTAMPTZ", "When the transaction occurred"),
            ("subtotal", "NUMERIC(10,2)", "Sum of line totals before tax"),
            ("tax", "NUMERIC(10,2)", "Tax amount"),
            ("total", "NUMERIC(10,2)", "Final amount charged"),
            ("payment_method", "VARCHAR(30)", "cash, credit, debit, mobile_pay, loyalty_points"),
            ("scenario_tag", "VARCHAR(50)", "Active scenario when generated"),
        ],
        "relationships": [
            "pos.transactions.location_id → hr.locations.location_id",
            "pos.transactions.member_id → pos.loyalty_members.member_id",
            "Referenced by pos.transaction_items(transaction_id)",
        ],
        "notes": "High-volume table — expect 500–2,000 rows/day × number of locations.",
    },
    "pos.transaction_items": {
        "title": "POS — Transaction Items",
        "description": "Line items for each POS transaction. One row per product sold.",
        "columns": [
            ("item_id", "UUID PK", "Primary key"),
            ("transaction_id", "UUID FK → pos.transactions", "Parent transaction"),
            ("product_id", "UUID FK → pos.products", "Which product"),
            ("product_name / category", "VARCHAR (joined)", "Denormalized from pos.products"),
            ("quantity", "INTEGER", "Units sold"),
            ("unit_price", "NUMERIC(8,2)", "Retail price at time of sale"),
            ("discount", "NUMERIC(8,2)", "Per-unit discount applied"),
            ("line_total", "NUMERIC(10,2)", "quantity × (unit_price − discount)"),
        ],
        "relationships": [
            "pos.transaction_items.transaction_id → pos.transactions.transaction_id",
            "pos.transaction_items.product_id → pos.products.product_id",
        ],
        "notes": "Highest-volume table — typically 2–5 items per transaction.",
    },
    "pos.products": {
        "title": "POS — Products",
        "description": "Product catalog — ~200 SKUs seeded at startup across 7 categories.",
        "columns": [
            ("product_id", "UUID PK", "Primary key"),
            ("sku", "VARCHAR(50) UNIQUE", "Stock-keeping unit"),
            ("name", "VARCHAR(200)", "Product display name"),
            ("category", "VARCHAR(100)", "Beverages, Snacks, Tobacco, Automotive, etc."),
            ("subcategory", "VARCHAR(100)", "Sub-category (nullable)"),
            ("cost", "NUMERIC(8,4)", "Supplier cost"),
            ("current_price", "NUMERIC(8,2)", "Current retail price"),
            ("is_active", "BOOLEAN", "FALSE for discontinued products"),
        ],
        "relationships": [
            "Referenced by pos.transaction_items(product_id)",
            "Referenced by pos.price_history(product_id)",
            "Referenced by inv.stock_levels(product_id)",
        ],
        "notes": "Static reference table — seeded once, rarely modified.",
    },
    "pos.loyalty_members": {
        "title": "POS — Loyalty Members",
        "description": "Customer loyalty program members. New members sign up at ~5% of transaction rate.",
        "columns": [
            ("member_id", "UUID PK", "Primary key"),
            ("first_name / last_name", "VARCHAR(100)", "Member name"),
            ("email", "VARCHAR(255) UNIQUE", "Loyalty account email"),
            ("phone", "VARCHAR(20)", "Optional phone number"),
            ("signup_date", "DATE", "Date they joined"),
            ("points_balance", "INTEGER", "Current point balance"),
            ("tier", "VARCHAR(20)", "bronze, silver, gold, or platinum"),
        ],
        "relationships": [
            "Referenced by pos.transactions(member_id)",
            "Referenced by fuel.transactions(member_id)",
        ],
        "notes": "Tier thresholds: silver ≥ 500, gold ≥ 2,000, platinum ≥ 5,000 points.",
    },
    "pos.price_history": {
        "title": "POS — Product Price History",
        "description": "Audit trail of product retail price changes. One row per change event.",
        "columns": [
            ("price_history_id", "UUID PK", "Primary key"),
            ("product_name / category", "VARCHAR (joined)", "Denormalized from pos.products"),
            ("old_price", "NUMERIC(8,2)", "Price before the change"),
            ("new_price", "NUMERIC(8,2)", "Price after the change"),
            ("changed_at", "TIMESTAMPTZ", "When the price was updated"),
        ],
        "relationships": ["pos.price_history.product_id → pos.products.product_id"],
        "notes": "Generator occasionally adjusts product prices to simulate market fluctuations.",
    },
    "fuel.transactions": {
        "title": "Fuel — Transactions",
        "description": "Every fuel dispensing event at a pump.",
        "columns": [
            ("transaction_id", "UUID PK", "Primary key"),
            ("pump_id", "UUID FK → fuel.pumps", "Which pump dispensed fuel"),
            ("location_id", "UUID FK → hr.locations", "Which store"),
            ("transaction_dt", "TIMESTAMPTZ", "When fuel was dispensed"),
            ("grade_id", "UUID FK → fuel.grades", "Fuel grade selected"),
            ("grade_name", "VARCHAR (joined)", "Grade name"),
            ("gallons", "NUMERIC(8,4)", "Volume dispensed"),
            ("price_per_gallon", "NUMERIC(8,4)", "Price at time of fill"),
            ("total_amount", "NUMERIC(10,2)", "gallons × price_per_gallon"),
            ("payment_method", "VARCHAR(30)", "cash, credit, debit, pay_at_pump, etc."),
            ("scenario_tag", "VARCHAR(50)", "Active scenario at generation time"),
        ],
        "relationships": [
            "fuel.transactions.pump_id → fuel.pumps.pump_id",
            "fuel.transactions.grade_id → fuel.grades.grade_id",
        ],
        "notes": "300–1,000 fuel transactions/day × locations.",
    },
    "fuel.grades": {
        "title": "Fuel — Grades",
        "description": "Fuel grade definitions and current prices. 4 grades seeded at startup.",
        "columns": [
            ("grade_id", "UUID PK", "Primary key"),
            ("name", "VARCHAR(50) UNIQUE", "Regular, Plus, Premium, or Diesel"),
            ("octane_rating", "VARCHAR(10)", "Octane rating; NULL for Diesel"),
            ("current_price", "NUMERIC(8,4)", "Current price per gallon"),
            ("is_active", "BOOLEAN", "FALSE to disable a grade"),
            ("updated_at", "TIMESTAMPTZ", "When price was last set"),
        ],
        "relationships": [
            "Referenced by fuel.transactions(grade_id)",
            "Referenced by fuel.price_history(grade_id)",
        ],
        "notes": "Small lookup — 4 rows. Initial prices: Regular $3.2990, Plus $3.5990, Premium $3.8990, Diesel $3.7990.",
    },
    "fuel.price_history": {
        "title": "Fuel — Price History",
        "description": "Audit trail of fuel price changes. One row per grade per change event.",
        "columns": [
            ("price_history_id", "UUID PK", "Primary key"),
            ("grade_name", "VARCHAR (joined)", "Grade name from fuel.grades"),
            ("old_price", "NUMERIC(8,4)", "Price before the change"),
            ("new_price", "NUMERIC(8,4)", "Price after the change"),
            ("changed_at", "TIMESTAMPTZ", "Timestamp of the price change"),
        ],
        "relationships": ["fuel.price_history.grade_id → fuel.grades.grade_id"],
        "notes": "Price changes fire every ~3.5 days. The fuel_spike scenario accelerates upward changes.",
    },
    "fuel.pumps": {
        "title": "Fuel — Pumps",
        "description": "Physical fuel pump hardware. 4–8 pumps per location seeded at startup.",
        "columns": [
            ("pump_id", "UUID PK", "Primary key"),
            ("location_id", "UUID FK → hr.locations", "Which store"),
            ("pump_number", "INTEGER", "Sequential number within location (1-based)"),
            ("num_sides", "INTEGER", "Number of dispensing sides (usually 2)"),
            ("is_active", "BOOLEAN", "FALSE for out-of-service pumps"),
        ],
        "relationships": [
            "fuel.pumps.location_id → hr.locations.location_id",
            "Referenced by fuel.transactions(pump_id)",
        ],
        "notes": "UNIQUE constraint on (location_id, pump_number).",
    },
    "inv.stock_levels": {
        "title": "Inventory — Stock Levels",
        "description": "Current on-hand quantity per product per location.",
        "columns": [
            ("stock_id", "UUID PK", "Primary key"),
            ("product_id", "UUID FK → pos.products", "Which product"),
            ("location_id", "UUID FK → hr.locations", "Which location"),
            ("product_name / category", "VARCHAR (joined)", "Denormalized from pos.products"),
            ("quantity_on_hand", "INTEGER", "Current physical count"),
            ("reorder_point / reorder_qty", "INTEGER (joined)", "Thresholds from inv.products"),
            ("last_updated", "TIMESTAMPTZ", "When this row was last written"),
        ],
        "relationships": [
            "UNIQUE constraint on (product_id, location_id)",
        ],
        "notes": "Updated after every POS transaction (decremented) and every receipt (incremented).",
    },
    "inv.receipts": {
        "title": "Inventory — Receipts",
        "description": "Supplier delivery events — header record for stock replenishment.",
        "columns": [
            ("receipt_id", "UUID PK", "Primary key"),
            ("location_id", "UUID FK → hr.locations", "Which store received the delivery"),
            ("received_by", "UUID FK → hr.employees", "Employee who signed (nullable)"),
            ("received_dt", "TIMESTAMPTZ", "When the delivery arrived"),
            ("supplier_name", "VARCHAR(200)", "Supplier company name"),
            ("po_number", "VARCHAR(50)", "Purchase order reference"),
            ("total_cost", "NUMERIC(12,2)", "Total cost of all items"),
        ],
        "relationships": [
            "inv.receipts.location_id → hr.locations.location_id",
            "Referenced by inv.receipt_items(receipt_id)",
        ],
        "notes": "Generated automatically when any product drops below its reorder point.",
    },
    "inv.receipt_items": {
        "title": "Inventory — Receipt Items",
        "description": "Line items on a restocking receipt. One row per product received.",
        "columns": [
            ("receipt_item_id", "UUID PK", "Primary key"),
            ("receipt_id", "UUID FK → inv.receipts", "Parent receipt header"),
            ("product_id", "UUID FK → pos.products", "Which product"),
            ("product_name / category", "VARCHAR (joined)", "Denormalized from pos.products"),
            ("quantity", "INTEGER", "Units received"),
            ("unit_cost", "NUMERIC(8,4)", "Cost per unit"),
            ("line_total", "NUMERIC(12,2)", "quantity × unit_cost"),
        ],
        "relationships": [
            "inv.receipt_items.receipt_id → inv.receipts.receipt_id",
        ],
        "notes": "After receipt, inv.stock_levels.quantity_on_hand is incremented by the received quantity.",
    },
    "inv.products": {
        "title": "Inventory — Product Config",
        "description": "Inventory management parameters per product. One row per product.",
        "columns": [
            ("inv_product_id", "UUID PK", "Primary key"),
            ("product_id", "UUID FK → pos.products UNIQUE", "One-to-one with pos.products"),
            ("product_name / category / sku", "VARCHAR (joined)", "Denormalized from pos.products"),
            ("reorder_point", "INTEGER", "Trigger restocking below this quantity"),
            ("reorder_qty", "INTEGER", "Units to order per restocking event"),
            ("unit_of_measure", "VARCHAR(20)", "each, case, carton, etc."),
            ("supplier_name", "VARCHAR(200)", "Primary supplier"),
            ("lead_time_days", "INTEGER", "Days from order to delivery (informational)"),
        ],
        "relationships": [
            "inv.products.product_id → pos.products.product_id (UNIQUE)",
        ],
        "notes": "Seeded once at startup alongside inv.stock_levels.",
    },
    "control.generator_state": {
        "title": "Control — Generator State",
        "description": "Single-row control table holding the generator's current operational state.",
        "columns": [
            ("state_id", "SERIAL PK", "Always 1 — single-row table"),
            ("is_running", "BOOLEAN", "TRUE when actively running"),
            ("is_paused", "BOOLEAN", "TRUE when paused"),
            ("mode", "VARCHAR(20)", "realtime, backfill, or stopped"),
            ("active_scenario", "VARCHAR(50)", "Current scenario tag"),
            ("volume_multiplier", "NUMERIC(5,2)", "Scales all transaction counts (0.1–10.0)"),
            ("backfill_start_date / backfill_end_date", "DATE", "Backfill date range (nullable)"),
            ("backfill_current_date", "DATE", "Day currently being processed in backfill"),
            ("tick_interval_seconds", "INTEGER", "Wall-clock seconds between ticks"),
            ("last_tick_at / started_at / updated_at", "TIMESTAMPTZ", "Audit timestamps"),
        ],
        "relationships": [],
        "notes": "The API (PATCH /generator/config, POST /generator/start) writes to this row.",
    },
    "control.generation_stats": {
        "title": "Control — Generation Stats",
        "description": "Append-only log of per-tick generation activity. Powers the Dashboard charts.",
        "columns": [
            ("stat_id", "BIGSERIAL PK", "Auto-incrementing primary key"),
            ("recorded_at", "TIMESTAMPTZ", "Wall-clock time when tick completed"),
            ("pos_transactions_generated", "INTEGER", "POS transactions inserted this tick"),
            ("fuel_transactions_generated", "INTEGER", "Fuel transactions inserted this tick"),
            ("inventory_receipts_generated", "INTEGER", "Restocking receipts created this tick"),
            ("scenario_tag", "VARCHAR(50)", "Active scenario during this tick"),
            ("simulation_dt", "TIMESTAMPTZ", "Simulated timestamp the tick represented"),
            ("wall_clock_ms", "INTEGER", "How long the tick took in milliseconds"),
        ],
        "relationships": [],
        "notes": "Never updated — append-only. Indexed on recorded_at DESC.",
    },
}

GROCERY_TABLE_DOCS = {
    "hr.locations": {
        "title": "HR — Locations",
        "description": "All physical locations: stores and distribution warehouses.",
        "columns": [
            ("location_id", "UUID PK", "Primary key"),
            ("name", "VARCHAR(100)", "Location name (e.g. 'FreshMart #1', 'FreshMart Distribution Center #1')"),
            ("address / city / state / zip", "VARCHAR", "Full mailing address"),
            ("phone", "VARCHAR(20)", "Phone number (nullable)"),
            ("opened_date", "DATE", "Date location opened"),
            ("location_type", "VARCHAR(20)", "store or warehouse"),
            ("store_sqft", "INTEGER", "Square footage (stores only, nullable)"),
            ("num_aisles", "INTEGER", "Number of aisles (stores only, nullable)"),
            ("is_active", "BOOLEAN", "FALSE for closed locations"),
        ],
        "relationships": [
            "Referenced by hr.employees(location_id)",
            "Referenced by pos.transactions(location_id)",
            "Referenced by inv.stock_levels(location_id)",
            "Referenced by ordering.store_orders(location_id)",
        ],
        "notes": "Stores are customer-facing. Warehouses fulfill orders to stores.",
    },
    "hr.employees": {
        "title": "HR — Employees",
        "description": "All employees across stores and warehouses.",
        "columns": [
            ("employee_id", "UUID PK", "Primary key"),
            ("location_id", "UUID FK → hr.locations", "Which location they work at"),
            ("first_name / last_name", "VARCHAR(100)", "Employee name"),
            ("email", "VARCHAR(255) UNIQUE", "Work email"),
            ("hire_date", "DATE", "Date hired"),
            ("department", "VARCHAR(50)", "store, produce, deli, bakery, meat, warehouse, transport, management"),
            ("job_title", "VARCHAR(100)", "Role (e.g. Cashier, Produce Clerk, Warehouse Associate)"),
            ("hourly_rate", "NUMERIC(8,2)", "Hourly pay rate"),
            ("status", "VARCHAR(20)", "active, terminated, or on_leave"),
        ],
        "relationships": [
            "hr.employees.location_id → hr.locations.location_id",
            "Referenced by pos.transactions(employee_id)",
            "Referenced by timeclock.events(employee_id)",
        ],
        "notes": "Warehouse employees are separate from store employees — use department filter to distinguish.",
    },
    "pos.transactions": {
        "title": "POS — Transactions",
        "description": "Every in-store sale. Includes coupon and combo deal savings columns.",
        "columns": [
            ("transaction_id", "UUID PK", "Primary key"),
            ("location_id", "UUID FK → hr.locations", "Which store"),
            ("employee_id", "UUID FK → hr.employees", "Cashier (nullable)"),
            ("member_id", "UUID FK → pos.loyalty_members", "Loyalty member (nullable)"),
            ("transaction_dt", "TIMESTAMPTZ", "When the transaction occurred"),
            ("subtotal", "NUMERIC(10,2)", "Sum of line totals before tax"),
            ("coupon_savings", "NUMERIC(10,2)", "Total coupon discounts applied"),
            ("deal_savings", "NUMERIC(10,2)", "Total combo deal savings"),
            ("tax", "NUMERIC(10,2)", "Tax amount"),
            ("total", "NUMERIC(10,2)", "Final amount charged"),
            ("payment_method", "VARCHAR(30)", "cash, credit, debit, mobile_pay, loyalty_points"),
            ("scenario_tag", "VARCHAR(50)", "Active scenario when generated"),
        ],
        "relationships": [
            "pos.transactions.location_id → hr.locations.location_id",
            "Referenced by pos.transaction_items(transaction_id)",
        ],
        "notes": "coupon_savings and deal_savings are informational — subtotal already reflects discounts.",
    },
    "pos.transaction_items": {
        "title": "POS — Transaction Items",
        "description": "Line items for each POS transaction. Quantity is NUMERIC for weight-based items.",
        "columns": [
            ("item_id", "UUID PK", "Primary key"),
            ("transaction_id", "UUID FK → pos.transactions", "Parent transaction"),
            ("product_id", "UUID FK → pos.products", "Which product"),
            ("product_name / category / department", "VARCHAR (joined)", "Denormalized from pos.products"),
            ("quantity", "NUMERIC(8,3)", "Units sold — decimal for lb-based items (e.g. produce, meat)"),
            ("unit_price", "NUMERIC(8,2)", "Retail price at time of sale"),
            ("discount", "NUMERIC(8,2)", "Per-unit discount applied"),
            ("line_total", "NUMERIC(10,2)", "quantity × (unit_price − discount)"),
        ],
        "relationships": [
            "pos.transaction_items.transaction_id → pos.transactions.transaction_id",
        ],
        "notes": "Highest-volume table. lb-based products (Produce, Meat) use decimal quantities like 0.75 lbs.",
    },
    "pos.products": {
        "title": "POS — Products",
        "description": "Product catalog — ~500 SKUs across 11 grocery departments.",
        "columns": [
            ("product_id", "UUID PK", "Primary key"),
            ("sku", "VARCHAR(50) UNIQUE", "Stock-keeping unit"),
            ("name", "VARCHAR(200)", "Product display name"),
            ("department_id", "UUID FK → pos.departments", "Department assignment"),
            ("category", "VARCHAR(100)", "Category within department"),
            ("subcategory", "VARCHAR(100)", "Sub-category (nullable)"),
            ("cost", "NUMERIC(8,4)", "Supplier cost"),
            ("current_price", "NUMERIC(8,2)", "Current retail price"),
            ("unit_of_measure", "VARCHAR(20)", "each, lb, oz, etc."),
            ("is_active", "BOOLEAN", "FALSE for discontinued products"),
        ],
        "relationships": [
            "pos.products.department_id → pos.departments.department_id",
            "Referenced by pos.transaction_items(product_id)",
            "Referenced by inv.stock_levels(product_id)",
        ],
        "notes": "lb-based products have unit_of_measure = 'lb'. ~500 initial SKUs vs 200 in gas station.",
    },
    "pos.departments": {
        "title": "POS — Departments",
        "description": "Grocery store departments. Products are assigned to departments.",
        "columns": [
            ("department_id", "UUID PK", "Primary key"),
            ("name", "VARCHAR(100) UNIQUE", "Department name (e.g. Produce, Dairy, Meat, Bakery)"),
            ("code", "VARCHAR(10) UNIQUE", "Short code (e.g. PROD, DAIRY, MEAT)"),
            ("is_active", "BOOLEAN", "FALSE for discontinued departments"),
        ],
        "relationships": ["Referenced by pos.products(department_id)"],
        "notes": "Seeded at startup from config. Typical departments: Produce, Dairy, Meat, Bakery, Deli, Frozen, Grocery, Beverage, Snack, Health & Beauty, General Merchandise.",
    },
    "pos.coupons": {
        "title": "POS — Coupons",
        "description": "Active coupons applied during POS transactions. Loyalty members get higher attach rates.",
        "columns": [
            ("coupon_id", "UUID PK", "Primary key"),
            ("code", "VARCHAR(50) UNIQUE", "Coupon code"),
            ("description", "VARCHAR(200)", "Human-readable description"),
            ("coupon_type", "VARCHAR(20)", "percent_off, dollar_off, or bogo"),
            ("discount_value", "NUMERIC(8,4)", "Percent (0–1.0) or dollar amount"),
            ("department_id", "UUID FK → pos.departments", "Department restriction (nullable = all)"),
            ("product_id", "UUID FK → pos.products", "Product restriction (nullable = all in dept)"),
            ("min_purchase", "NUMERIC(8,2)", "Minimum purchase amount to qualify (nullable)"),
            ("valid_from / valid_through", "DATE", "Validity window"),
            ("is_active", "BOOLEAN", "FALSE for expired or disabled coupons"),
        ],
        "relationships": [
            "pos.coupons.department_id → pos.departments.department_id",
        ],
        "notes": "Applied at transaction time. coupon_savings on pos.transactions reflects total coupon value.",
    },
    "pos.combo_deals": {
        "title": "POS — Combo Deals",
        "description": "Combo promotions like '2 for $5' or 'Buy 2 Get 1 Free' applied during checkout.",
        "columns": [
            ("deal_id", "UUID PK", "Primary key"),
            ("name", "VARCHAR(200)", "Deal name (e.g. '2 for $5 Beverages')"),
            ("deal_type", "VARCHAR(20)", "multi_price (2 for $X), bogo, or tiered_discount"),
            ("required_qty", "INTEGER", "Quantity needed to trigger the deal"),
            ("deal_price", "NUMERIC(8,2)", "Total price for required_qty items"),
            ("department_id", "UUID FK → pos.departments", "Department restriction (nullable)"),
            ("is_active", "BOOLEAN", "FALSE for inactive deals"),
        ],
        "relationships": ["pos.combo_deals.department_id → pos.departments.department_id"],
        "notes": "deal_savings on pos.transactions reflects total combo deal savings.",
    },
    "pos.returns": {
        "title": "POS — Customer Returns",
        "description": "Return events referencing an original transaction. Refund amounts are prorated from the transaction total, so SUM(refund_amount) reconciles against pos.transactions.total.",
        "columns": [
            ("return_id", "UUID PK", "Primary key"),
            ("transaction_id", "UUID FK → pos.transactions", "Original sale"),
            ("location_id", "UUID FK → hr.locations", "Store accepting the return"),
            ("member_id", "UUID FK → pos.loyalty_members", "Loyalty member (nullable)"),
            ("return_dt", "TIMESTAMPTZ", "When the return happened"),
            ("reason", "VARCHAR(30)", "defective, wrong_item, changed_mind, damaged_in_transit, price_found_lower, other"),
            ("refund_method", "VARCHAR(20)", "original_payment, cash, or store_credit"),
            ("refund_amount", "NUMERIC(10,2)", "Money returned"),
            ("is_restocked", "BOOLEAN", "TRUE when goods went back on the shelf"),
            ("scenario_tag", "VARCHAR(50)", "Active scenario at generation time"),
        ],
        "relationships": ["pos.returns.transaction_id → pos.transactions", "Referenced by pos.return_items"],
        "notes": "One return per transaction maximum. Defective/damaged goods are written off (is_restocked=FALSE); sellable goods flow back into inv.stock_levels.",
    },
    "pos.return_items": {
        "title": "POS — Return Line Items",
        "description": "Line-level detail of what was returned, referencing the original transaction_items rows.",
        "columns": [
            ("return_item_id", "UUID PK", "Primary key"),
            ("return_id", "UUID FK → pos.returns", "Parent return"),
            ("transaction_item_id", "UUID FK → pos.transaction_items", "Original line sold"),
            ("product_id", "UUID FK → pos.products", "Product returned"),
            ("quantity", "NUMERIC(8,3)", "Units returned (≤ sold quantity)"),
            ("refund_amount", "NUMERIC(10,2)", "Prorated refund for this line"),
        ],
        "relationships": ["pos.return_items.return_id → pos.returns", "pos.return_items.transaction_item_id → pos.transaction_items"],
        "notes": "Return quantity can be a partial of the original line (e.g. 2 of 3 units).",
    },
    "pos.loyalty_members": {
        "title": "POS — Loyalty Members",
        "description": "Customer loyalty program members. Loyalty members get coupon discounts.",
        "columns": [
            ("member_id", "UUID PK", "Primary key"),
            ("first_name / last_name", "VARCHAR(100)", "Member name"),
            ("email", "VARCHAR(255) UNIQUE", "Loyalty account email"),
            ("phone", "VARCHAR(20)", "Optional phone number"),
            ("signup_date", "DATE", "Date they joined"),
            ("points_balance", "INTEGER", "Current point balance"),
            ("tier", "VARCHAR(20)", "bronze, silver, gold, or platinum"),
        ],
        "relationships": ["Referenced by pos.transactions(member_id)"],
        "notes": "Tier thresholds: silver ≥ 500, gold ≥ 2,000, platinum ≥ 5,000 points.",
    },
    "pos.price_history": {
        "title": "POS — Product Price History",
        "description": "Audit trail of product retail price changes.",
        "columns": [
            ("price_history_id", "UUID PK", "Primary key"),
            ("product_name / category", "VARCHAR (joined)", "From pos.products"),
            ("old_price / new_price", "NUMERIC(8,2)", "Price before and after the change"),
            ("changed_at", "TIMESTAMPTZ", "When the price was updated"),
        ],
        "relationships": ["pos.price_history.product_id → pos.products.product_id"],
        "notes": "Generator occasionally adjusts product prices to simulate market fluctuations.",
    },
    "timeclock.events": {
        "title": "Timeclock — Events",
        "description": "Employee clock in/out and break events. Generated based on shift schedules.",
        "columns": [
            ("event_id", "UUID PK", "Primary key"),
            ("employee_id", "UUID FK → hr.employees", "Which employee"),
            ("location_id", "UUID FK → hr.locations", "Which location"),
            ("event_type", "VARCHAR(20)", "clock_in, clock_out, break_start, or break_end"),
            ("event_dt", "TIMESTAMPTZ", "When the event occurred"),
            ("notes", "TEXT", "Optional notes (nullable)"),
        ],
        "relationships": [
            "timeclock.events.employee_id → hr.employees.employee_id",
            "timeclock.events.location_id → hr.locations.location_id",
        ],
        "notes": "Shift windows: morning (clock_in 6–9am), afternoon (2–5pm). ~80% of employees work any given day. Breaks generated at shift midpoint.",
    },
    "ordering.store_orders": {
        "title": "Ordering — Store Orders",
        "description": "Replenishment orders from stores to the warehouse, triggered when stock drops below reorder point.",
        "columns": [
            ("order_id", "UUID PK", "Primary key"),
            ("location_id", "UUID FK → hr.locations", "Which store placed the order"),
            ("requested_by", "UUID FK → hr.employees", "Employee who created the order (nullable)"),
            ("order_dt", "TIMESTAMPTZ", "When the order was created"),
            ("status", "VARCHAR(20)", "pending, approved, shipped, or delivered"),
            ("notes", "TEXT", "Optional notes"),
        ],
        "relationships": [
            "ordering.store_orders.location_id → hr.locations.location_id",
            "Referenced by ordering.store_order_items(order_id)",
            "Referenced by fulfillment.orders(store_order_id)",
        ],
        "notes": "Created once per day when stock drops below reorder_point. Auto-approved in simulation.",
    },
    "ordering.store_order_items": {
        "title": "Ordering — Store Order Items",
        "description": "Line items on a store replenishment order.",
        "columns": [
            ("item_id", "UUID PK", "Primary key"),
            ("order_id", "UUID FK → ordering.store_orders", "Parent order"),
            ("product_id", "UUID FK → pos.products", "Which product"),
            ("requested_qty", "INTEGER", "Quantity requested"),
        ],
        "relationships": [
            "ordering.store_order_items.order_id → ordering.store_orders.order_id",
        ],
        "notes": "One row per product on the order. Quantity is based on reorder_qty from inv.products.",
    },
    "fulfillment.orders": {
        "title": "Fulfillment — Orders",
        "description": "Warehouse fulfillment of store orders. Created when warehouse picks an approved store order.",
        "columns": [
            ("fulfillment_id", "UUID PK", "Primary key"),
            ("store_order_id", "UUID FK → ordering.store_orders", "Which store order is being fulfilled"),
            ("filled_by", "UUID FK → hr.employees", "Warehouse employee who filled (nullable)"),
            ("fulfillment_dt", "TIMESTAMPTZ", "When fulfillment was created"),
            ("status", "VARCHAR(20)", "picking, packed, or dispatched"),
        ],
        "relationships": [
            "fulfillment.orders.store_order_id → ordering.store_orders.order_id",
            "Referenced by fulfillment.items(fulfillment_id)",
            "Referenced by transport.load_items(fulfillment_id)",
        ],
        "notes": "~5% short-fill rate per line — some items may be filled at less than requested qty.",
    },
    "fulfillment.items": {
        "title": "Fulfillment — Items",
        "description": "Line items on a fulfillment order. Reflects what was actually picked.",
        "columns": [
            ("item_id", "UUID PK", "Primary key"),
            ("fulfillment_id", "UUID FK → fulfillment.orders", "Parent fulfillment"),
            ("product_id", "UUID FK → pos.products", "Which product"),
            ("requested_qty", "INTEGER", "What the store asked for"),
            ("fulfilled_qty", "INTEGER", "What the warehouse actually packed"),
        ],
        "relationships": ["fulfillment.items.fulfillment_id → fulfillment.orders.fulfillment_id"],
        "notes": "fulfilled_qty ≤ requested_qty due to short-fill simulation.",
    },
    "transport.trucks": {
        "title": "Transport — Trucks",
        "description": "Truck fleet used to deliver orders from warehouse to stores.",
        "columns": [
            ("truck_id", "UUID PK", "Primary key"),
            ("make / model", "VARCHAR(100)", "Truck make and model (Freightliner, Peterbilt, etc.)"),
            ("license_plate", "VARCHAR(20) UNIQUE", "License plate number"),
            ("capacity_pallets", "INTEGER", "Max pallet capacity"),
            ("is_active", "BOOLEAN", "FALSE for out-of-service trucks"),
        ],
        "relationships": ["Referenced by transport.loads(truck_id)"],
        "notes": "Fleet of 4 trucks seeded at startup. Trucks are assigned loads by the generator.",
    },
    "transport.loads": {
        "title": "Transport — Loads",
        "description": "Delivery loads from warehouse to store. One load per store per day.",
        "columns": [
            ("load_id", "UUID PK", "Primary key"),
            ("truck_id", "UUID FK → transport.trucks", "Which truck carried the load"),
            ("destination_location_id", "UUID FK → hr.locations", "Destination store"),
            ("driver_id", "UUID FK → hr.employees", "Driver (warehouse/transport employee)"),
            ("dispatched_at", "TIMESTAMPTZ", "When the truck left the warehouse"),
            ("delivered_at", "TIMESTAMPTZ", "When the truck arrived at the store (nullable until delivered)"),
            ("status", "VARCHAR(20)", "dispatched or delivered"),
        ],
        "relationships": [
            "transport.loads.truck_id → transport.trucks.truck_id",
            "transport.loads.destination_location_id → hr.locations.location_id",
        ],
        "notes": "Loads are marked delivered after ~18 simulated hours. Triggers inv.receipts creation.",
    },
    "inv.stock_levels": {
        "title": "Inventory — Stock Levels",
        "description": "Current on-hand quantity per product per store location.",
        "columns": [
            ("stock_id", "UUID PK", "Primary key"),
            ("product_id", "UUID FK → pos.products", "Which product"),
            ("location_id", "UUID FK → hr.locations", "Which store (not warehouse)"),
            ("quantity_on_hand", "NUMERIC(8,3)", "Current count — decimal for lb-based items"),
            ("reorder_point", "NUMERIC(8,3)", "Trigger ordering below this level"),
            ("last_updated", "TIMESTAMPTZ", "When last written"),
        ],
        "relationships": ["UNIQUE constraint on (product_id, location_id)"],
        "notes": "Only store locations have stock records. When below reorder_point, ordering.store_orders are created. quantity_on_hand floors at 0 (a physical count); a row sitting at 0 means sold out and awaiting replenishment — the unmet demand is recorded in inv.stockout_events (t_959cd040).",
    },
    "inv.stockout_events": {
        "title": "Inventory — Stockouts (lost sales)",
        "description": "One row per sale line the shelf could not cover. The requested-vs-fulfilled split, and what the store lost by not having the stock.",
        "columns": [
            ("stockout_id", "UUID PK", "Primary key"),
            ("channel", "VARCHAR(10)", "pos | online"),
            ("pos_transaction_id", "UUID FK → pos.transactions", "Set when channel='pos'"),
            ("online_order_id", "UUID FK → online.orders", "Set when channel='online'"),
            ("product_id", "UUID FK → pos.products", "Which product"),
            ("location_id", "UUID FK → hr.locations", "Which store"),
            ("requested_quantity", "NUMERIC(8,3)", "What the shopper asked for"),
            ("fulfilled_quantity", "NUMERIC(8,3)", "What was rung up (matches the sale line)"),
            ("lost_quantity", "NUMERIC(8,3)", "The gap"),
            ("unit_price / lost_value", "NUMERIC(8,2) / NUMERIC(10,2)", "Priced at the price of record"),
            ("event_dt", "TIMESTAMPTZ", "When the shortfall happened"),
            ("scenario_tag", "VARCHAR(50)", "Which scenario was active"),
        ],
        "relationships": [
            "Exactly one of pos_transaction_id / online_order_id is set (CHECK)",
            "lost_quantity = requested_quantity − fulfilled_quantity (CHECK)",
        ],
        "notes": "Added by t_959cd040. Before this table a shortage was absorbed silently: depletion floored at zero while the sale booked the full requested quantity. API: /grocery/inventory/stockout-events.",
    },
    "inv.sku_demand_daily": {
        "title": "Inventory — Daily SKU Demand",
        "description": "Per store-SKU-day requested / fulfilled / lost totals. The input replenishment is sized from.",
        "columns": [
            ("location_id", "UUID FK → hr.locations", "Which store"),
            ("product_id", "UUID FK → pos.products", "Which product"),
            ("demand_date", "DATE", "The simulated day"),
            ("requested_units", "NUMERIC(12,3)", "Demand expressed (POS + online)"),
            ("fulfilled_units", "NUMERIC(12,3)", "What the shelves could cover"),
            ("lost_units", "NUMERIC(12,3)", "The gap (= requested − fulfilled)"),
            ("lost_value", "NUMERIC(14,2)", "The gap priced at the price of record"),
            ("line_count", "INTEGER", "Sale lines behind these totals"),
            ("last_updated", "TIMESTAMPTZ", "When last written"),
        ],
        "relationships": [
            "PRIMARY KEY (location_id, product_id, demand_date)",
        ],
        "notes": "Added by t_959cd040. ordering.check_and_create_orders sizes a reorder from requested_units over reorder_demand_window_days plus restock_threshold_pct safety. API: /grocery/inventory/sku-demand-daily.",
    },
    "inv.receipts": {
        "title": "Inventory — Receipts",
        "description": "Store receipt of a delivered transport load. Created when a load is marked delivered.",
        "columns": [
            ("receipt_id", "UUID PK", "Primary key"),
            ("location_id", "UUID FK → hr.locations", "Which store received the delivery"),
            ("load_id", "UUID FK → transport.loads", "Which transport load was received"),
            ("received_by", "UUID FK → hr.employees", "Employee who received (nullable)"),
            ("received_dt", "TIMESTAMPTZ", "When the delivery arrived"),
            ("total_items", "INTEGER", "Number of product lines received"),
        ],
        "relationships": [
            "inv.receipts.load_id → transport.loads.load_id",
            "Referenced by inv.receipt_items(receipt_id)",
        ],
        "notes": "Creating a receipt increments inv.stock_levels for all items received.",
    },
    "inv.receipt_items": {
        "title": "Inventory — Receipt Items",
        "description": "Line items on an inventory receipt. One row per product received.",
        "columns": [
            ("receipt_item_id", "UUID PK", "Primary key"),
            ("receipt_id", "UUID FK → inv.receipts", "Parent receipt"),
            ("product_id", "UUID FK → pos.products", "Which product"),
            ("quantity_received", "NUMERIC(8,3)", "Units received"),
            ("unit_cost", "NUMERIC(8,4)", "Cost per unit"),
        ],
        "relationships": ["inv.receipt_items.receipt_id → inv.receipts.receipt_id"],
        "notes": "After insert, inv.stock_levels.quantity_on_hand is incremented.",
    },
    "inv.products": {
        "title": "Inventory — Product Config",
        "description": "Inventory management parameters per product.",
        "columns": [
            ("inv_product_id", "UUID PK", "Primary key"),
            ("product_id", "UUID FK → pos.products UNIQUE", "One-to-one with pos.products"),
            ("reorder_point", "NUMERIC(8,3)", "Trigger restocking below this level"),
            ("reorder_qty", "NUMERIC(8,3)", "Quantity to order per event"),
            ("unit_of_measure", "VARCHAR(20)", "each, lb, oz, case, etc."),
            ("supplier_name", "VARCHAR(200)", "Primary supplier"),
        ],
        "relationships": ["inv.products.product_id → pos.products.product_id (UNIQUE)"],
        "notes": "Seeded once at startup alongside inv.stock_levels.",
    },
    "control.generator_state": GAS_STATION_TABLE_DOCS["control.generator_state"],
    "control.generation_stats": {
        "title": "Control — Generation Stats",
        "description": "Append-only log of per-tick generation activity. Powers the Dashboard charts.",
        "columns": [
            ("stat_id", "BIGSERIAL PK", "Auto-incrementing primary key"),
            ("recorded_at", "TIMESTAMPTZ", "Wall-clock time when tick completed"),
            ("pos_transactions_generated", "INTEGER", "POS transactions inserted this tick"),
            ("timeclock_events_generated", "INTEGER", "Timeclock events inserted this tick"),
            ("orders_generated", "INTEGER", "Store orders created this tick"),
            ("inventory_receipts_generated", "INTEGER", "Inventory receipts created this tick"),
            ("scenario_tag", "VARCHAR(50)", "Active scenario during this tick"),
            ("simulation_dt", "TIMESTAMPTZ", "Simulated timestamp the tick represented"),
            ("wall_clock_ms", "INTEGER", "How long the tick took in milliseconds"),
        ],
        "relationships": [],
        "notes": "Never updated — append-only. Indexed on recorded_at DESC.",
    },
}

# Combined lookup
SUPPORT_TABLE_DOCS: dict = {}

TABLE_DOCS_BY_INDUSTRY = {
    "gas-station": GAS_STATION_TABLE_DOCS,
    "grocery": GROCERY_TABLE_DOCS,
    "support": SUPPORT_TABLE_DOCS,
}


# ---------------------------------------------------------------------------
# Schema → table lists per industry
# ---------------------------------------------------------------------------

GAS_STATION_SCHEMA_TABLES = {
    "HR": [
        ("hr.locations", "Locations"),
        ("hr.employees", "Employees"),
    ],
    "POS": [
        ("pos.transactions", "Transactions"),
        ("pos.transaction_items", "Transaction Items"),
        ("pos.products", "Products"),
        ("pos.loyalty_members", "Loyalty Members"),
        ("pos.price_history", "Product Price History"),
    ],
    "Fuel": [
        ("fuel.transactions", "Transactions"),
        ("fuel.grades", "Grades"),
        ("fuel.price_history", "Price History"),
        ("fuel.pumps", "Pumps"),
    ],
    "Inventory": [
        ("inv.stock_levels", "Stock Levels"),
        ("inv.receipts", "Receipts"),
        ("inv.receipt_items", "Receipt Items"),
        ("inv.products", "Product Config"),
    ],
    "Control": [
        ("control.generator_state", "Generator State"),
        ("control.generation_stats", "Generation Stats"),
    ],
}

GROCERY_SCHEMA_TABLES = {
    "HR": [
        ("hr.locations", "Locations"),
        ("hr.employees", "Employees"),
    ],
    "POS": [
        ("pos.transactions", "Transactions"),
        ("pos.transaction_items", "Transaction Items"),
        ("pos.returns", "Customer Returns"),
        ("pos.return_items", "Return Line Items"),
        ("pos.products", "Products"),
        ("pos.departments", "Departments"),
        ("pos.coupons", "Coupons"),
        ("pos.combo_deals", "Combo Deals"),
        ("pos.loyalty_members", "Loyalty Members"),
        ("pos.price_history", "Product Price History"),
    ],
    "Online": [
        ("online.orders", "Online Orders"),
        ("online.order_items", "Order Items"),
        ("online.order_events", "Lifecycle Events"),
    ],
    "Timeclock": [
        ("timeclock.events", "Clock Events"),
    ],
    "Ordering": [
        ("ordering.store_orders", "Store Orders"),
        ("ordering.store_order_items", "Order Items"),
    ],
    "Fulfillment": [
        ("fulfillment.orders", "Fulfillment Orders"),
        ("fulfillment.items", "Fulfillment Items"),
    ],
    "Transport": [
        ("transport.trucks", "Trucks"),
        ("transport.loads", "Loads"),
    ],
    "Inventory": [
        ("inv.stock_levels", "Stock Levels"),
        ("inv.stockout_events", "Stockouts (lost sales)"),
        ("inv.sku_demand_daily", "Daily SKU Demand"),
        ("inv.receipts", "Receipts"),
        ("inv.receipt_items", "Receipt Items"),
        ("inv.products", "Product Config"),
    ],
    "Control": [
        ("control.generator_state", "Generator State"),
        ("control.generation_stats", "Generation Stats"),
    ],
}

SUPPORT_SCHEMA_TABLES = {
    "HR": [
        ("hr.locations", "Contact Centers"),
        ("hr.employees", "Agents & Staff"),
    ],
    "Queues": [
        ("support.queues", "Ticket Queues"),
        ("support.categories", "Categories"),
    ],
    "Customers": [
        ("support.customers", "Customers"),
    ],
    "Tickets": [
        ("support.tickets", "Tickets"),
        ("support.ticket_comments", "Comments"),
        ("support.ticket_actions", "Action Audit Trail"),
    ],
    "Voice ACD": [
        ("voice.calls", "Call Detail Records"),
    ],
    "Chat": [
        ("chat.sessions", "Chat Sessions"),
        ("chat.messages", "Transcripts"),
    ],
    "Surveys": [
        ("survey.surveys", "sNPS Surveys"),
    ],
    "Training": [
        ("training.courses", "Courses"),
        ("training.assignments", "Assignments"),
    ],
    "Control": [
        ("control.generator_state", "Generator State"),
        ("control.generation_stats", "Generation Stats"),
    ],
}

SCHEMA_TABLES_BY_INDUSTRY = {
    "gas-station": GAS_STATION_SCHEMA_TABLES,
    "grocery": GROCERY_SCHEMA_TABLES,
    "support": SUPPORT_SCHEMA_TABLES,
}

# ---------------------------------------------------------------------------
# Schema-level documentation (for Data Dictionary tab)
# ---------------------------------------------------------------------------

GAS_STATION_SCHEMA_DOCS = {
    "HR": {
        "description": "Physical store locations and all employees. The root of the data model — every other schema references HR for location and person context.",
        "tables_summary": [
            ("hr.locations", "Physical store locations."),
            ("hr.employees", "All employees across all locations."),
        ],
        "notes": "Referenced by POS, Fuel, and Inventory schemas via location_id and employee_id foreign keys.",
    },
    "POS": {
        "description": "Convenience store point-of-sale — transactions, line items, product catalog, and loyalty members.",
        "tables_summary": [
            ("pos.transactions", "Every in-store sale. 300–800 transactions/day per location."),
            ("pos.transaction_items", "Line items for each sale."),
            ("pos.products", "Product catalog across store categories."),
            ("pos.loyalty_members", "Loyalty program member records."),
            ("pos.price_history", "Audit trail of retail price changes."),
        ],
        "notes": "The promotion scenario triggers 15% discounts on Snacks and Beverages.",
    },
    "Fuel": {
        "description": "Fuel dispensing operations — pump transactions, grade definitions, price history, and pump hardware at each location.",
        "tables_summary": [
            ("fuel.transactions", "Every fuel dispensing event at the pumps."),
            ("fuel.grades", "Fuel grade definitions and current prices (Regular, Plus, Premium, Diesel)."),
            ("fuel.price_history", "Audit trail of fuel price changes."),
            ("fuel.pumps", "Physical pump hardware per location."),
        ],
        "notes": "The fuel_spike scenario triggers above-average upward price changes. Prices change every ~3.5 days.",
    },
    "Inventory": {
        "description": "Product stock levels and restocking events. Stock decrements after each POS sale and increments after each supplier delivery.",
        "tables_summary": [
            ("inv.stock_levels", "Real-time on-hand quantity per product per location."),
            ("inv.receipts", "Supplier delivery receipt headers."),
            ("inv.receipt_items", "Products and quantities in each delivery."),
            ("inv.products", "Inventory config per product — reorder points, suppliers."),
        ],
        "notes": "Restocking receipts are auto-generated when stock drops below the reorder_point threshold.",
    },
    "Control": {
        "description": "Internal generator control tables. Not a business source system — tracks operational state and per-tick generation activity.",
        "tables_summary": [
            ("control.generator_state", "Single-row control table. Holds mode, scenario, and timing config."),
            ("control.generation_stats", "Append-only per-tick activity log. Powers the Dashboard charts."),
        ],
        "notes": "These tables exist to support the simulation engine, not the business domain.",
    },
}

GROCERY_SCHEMA_DOCS = {
    "HR": {
        "description": "Physical locations (stores and warehouses) and all employees. The root of the data model — every other schema references HR for location and person context.",
        "tables_summary": [
            ("hr.locations", "All physical stores and distribution warehouses."),
            ("hr.employees", "All employees across all locations and departments."),
        ],
        "notes": "Referenced by POS, Timeclock, Ordering, Transport, and Inventory via location_id and employee_id.",
    },
    "POS": {
        "description": "Point-of-sale layer — the highest-volume schema. Captures every customer transaction, line items, the product catalog, loyalty program, active coupons, and combo deal promotions.",
        "tables_summary": [
            ("pos.transactions", "Every in-store sale. Highest-volume event stream."),
            ("pos.transaction_items", "Line items for each transaction."),
            ("pos.products", "Product catalog — ~500 SKUs across 11 departments."),
            ("pos.departments", "Grocery department definitions (Produce, Dairy, Meat, etc.)."),
            ("pos.coupons", "Active discount coupons applied during checkout."),
            ("pos.combo_deals", "Multi-buy promotions (2 for $5, BOGO)."),
            ("pos.loyalty_members", "Loyalty program members with tier and point balance."),
            ("pos.price_history", "Audit trail of retail price changes."),
        ],
        "notes": "coupon_savings and deal_savings on pos.transactions are informational — subtotal already reflects discounts. Formula: total = subtotal + tax - coupon_savings - deal_savings.",
    },
    "Timeclock": {
        "description": "Employee shift tracking. Records clock_in, clock_out, break_start, and break_end events for all store and warehouse employees.",
        "tables_summary": [
            ("timeclock.events", "All employee timeclock events across all locations."),
        ],
        "notes": "~80% of employees work any given day. Morning shifts 6–9am, afternoon shifts 2–5pm. Breaks generated at shift midpoint.",
    },
    "Ordering": {
        "description": "Store replenishment requests to the warehouse. Orders are auto-created when inventory drops below reorder thresholds — one order per store per day when needed.",
        "tables_summary": [
            ("ordering.store_orders", "Order headers placed by stores to the warehouse."),
            ("ordering.store_order_items", "Line items specifying product and quantity requested."),
        ],
        "notes": "Orders are auto-approved in the simulation. Fulfilled by the Fulfillment schema.",
    },
    "Fulfillment": {
        "description": "Warehouse processing of store replenishment orders. Tracks what was actually picked vs. requested — a ~5% short-fill rate is built into the simulation.",
        "tables_summary": [
            ("fulfillment.orders", "Fulfillment headers — warehouse processes an approved store order."),
            ("fulfillment.items", "Actual picked quantities (may be less than requested due to short-fill)."),
        ],
        "notes": "Once dispatched, fulfillment orders trigger transport.loads for delivery.",
    },
    "Transport": {
        "description": "Truck fleet and delivery logistics. Loads are dispatched from the warehouse to stores; marking a load delivered triggers inventory receipt creation at the destination store.",
        "tables_summary": [
            ("transport.trucks", "Fleet of delivery trucks (make, model, capacity)."),
            ("transport.loads", "Delivery loads — one per store per day."),
        ],
        "notes": "Loads are marked delivered after ~18 simulated hours. Delivery triggers inv.receipts and updates inv.stock_levels.",
    },
    "Inventory": {
        "description": "Per-product, per-store stock levels and receipt tracking. Stock decrements on every POS sale and increments when inventory receipts are created from delivered transport loads.",
        "tables_summary": [
            ("inv.stock_levels", "Real-time on-hand quantity per product per store location."),
            ("inv.receipts", "Delivery receipt headers — created when a transport load is delivered."),
            ("inv.receipt_items", "Products and quantities received in each delivery."),
            ("inv.products", "Inventory management config — reorder points, quantities, suppliers."),
        ],
        "notes": "Only store locations have stock records. Warehouses do not. Stock below reorder_point triggers ordering.store_orders.",
    },
    "Control": {
        "description": "Internal generator control tables. Not a business source system — tracks operational state and per-tick generation activity for the simulation engine.",
        "tables_summary": [
            ("control.generator_state", "Single-row control table. Holds mode, scenario, tick interval, and audit timestamps."),
            ("control.generation_stats", "Append-only per-tick activity log. Powers the Dashboard charts."),
        ],
        "notes": "These tables exist to support the simulation engine. Rarely used in analytics models.",
    },
}

SUPPORT_SCHEMA_DOCS = {
    "HR": {
        "description": "Contact-center sites and all staff. Agents carry skill_groups (queue codes) and an aht_factor that personalizes handle times.",
        "tables_summary": [
            ("hr.locations", "Contact centers and remote hubs."),
            ("hr.employees", "Agents, team leads, QA, trainers, managers."),
        ],
        "notes": "Every ticket, call, chat, survey, and training assignment references these people.",
    },
    "Queues": {
        "description": "Ticket destination queues (billing, technical, account, shipping, returns, escalations) with per-queue SLA and average handle profiles.",
        "tables_summary": [
            ("support.queues", "Queue definitions + SLA targets."),
            ("support.categories", "Classifications within each queue."),
        ],
        "notes": "Queue movement is audited in support.ticket_actions.",
    },
    "Customers": {
        "description": "The customer base behind every contact: tier, lifetime value, signup date.",
        "tables_summary": [("support.customers", "Customer master.")],
        "notes": "VIP tier skews tickets to escalations.",
    },
    "Tickets": {
        "description": "The ticketing system: lifecycle (new→open→pending→resolved→closed), assignments, queue movements, agent + customer comments, and a complete action audit trail.",
        "tables_summary": [
            ("support.tickets", "One row per case, with timestamps for each lifecycle stage."),
            ("support.ticket_comments", "Internal + customer-visible comments by agents, customers, system."),
            ("support.ticket_actions", "Immutable audit: assigned, moved, escalated, status_change, sla_breached..."),
        ],
        "notes": "reopen_count and touch_count make first-contact-resolution and effort analysis possible.",
    },
    "Voice ACD": {
        "description": "Phone ACD call detail records: offered→queued→ring→connect→end with wait, talk, hold and after-call-work seconds, abandonment and disposition.",
        "tables_summary": [("voice.calls", "CDR — one row per offered call.")],
        "notes": "has_ticket links calls that produced a follow-up ticket.",
    },
    "Chat": {
        "description": "Live chat sessions with full message transcripts (customer / agent / system senders).",
        "tables_summary": [
            ("chat.sessions", "Session lifecycle + waits + duration."),
            ("chat.messages", "Transcript lines."),
        ],
        "notes": "transferred_to_ticket marks chats converted into cases.",
    },
    "Surveys": {
        "description": "sNPS survey system attached to closed voice/chat/ticket interactions, with 0-10 NPS, 1-5 CSAT, reason tags, and free-text verbatims.",
        "tables_summary": [("survey.surveys", "One row per survey sent; response columns fill later.")],
        "notes": "Detractor clusters per agent drive qa_finding training assignments.",
    },
    "Training": {
        "description": "Training system: course catalog, per-agent assignments (onboarding, QA remedial, launch/recall triggers) with due dates, scores and attempts.",
        "tables_summary": [
            ("training.courses", "Catalog with duration/pass score/mandatory flags."),
            ("training.assignments", "Assignment lifecycle incl. overdue detection."),
        ],
        "notes": "trigger_reason explains WHY each assignment exists.",
    },
    "Control": {
        "description": "Generator bookkeeping — state machine and per-tick stats. Not analytics data.",
        "tables_summary": [
            ("control.generator_state", "Single-row state."),
            ("control.generation_stats", "Per-tick counts of everything generated."),
        ],
        "notes": "These tables exist to support the simulation engine. Rarely used in analytics models.",
    },
}

SCHEMA_DOCS_BY_INDUSTRY = {
    "gas-station": GAS_STATION_SCHEMA_DOCS,
    "grocery": GROCERY_SCHEMA_DOCS,
    "support": SUPPORT_SCHEMA_DOCS,
}

