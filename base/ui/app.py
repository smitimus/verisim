"""
Verisim — Multi-Industry Data Generator Control Panel
5 tabs: Dashboard, Generator Control, Scenarios, Table Explorer, Documentation
"""
import os
import time
from datetime import datetime, date, timedelta

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

API = os.environ.get("API_BASE_URL", "http://localhost:8000")
REFRESH_INTERVAL = 15  # seconds

st.set_page_config(
    page_title="Verisim — Data Generator",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Industry selector — driven by /health; hidden when only one industry is up
# ---------------------------------------------------------------------------

INDUSTRY_META = {
    "gas-station": ("⛽", "Gas Station"),
    "grocery":     ("🛒", "Grocery"),
}


INDUSTRY_ORDER = ["grocery", "gas-station"]  # preferred display order


@st.cache_data(ttl=30)
def get_available_industries():
    """
    Returns all DB-healthy industries from /health.
    Generator running/stopped state does not affect availability.
    """
    try:
        r = requests.get(f"{API}/health", timeout=5)
        r.raise_for_status()
        db_healthy = [slug for slug, ok in r.json().get("industries", {}).items() if ok]
    except Exception:
        return []

    # Return in preferred display order
    return [s for s in INDUSTRY_ORDER if s in db_healthy] + [s for s in db_healthy if s not in INDUSTRY_ORDER]


available = get_available_industries()

if not available:
    st.error(f"Cannot reach API at `{API}`. Is the stack running?")
    st.stop()

if len(available) == 1:
    industry = available[0]
else:
    _labels = [INDUSTRY_META.get(s, ("🏭", s.replace("-", " ").title()))[1] for s in available]
    _sel = st.radio(
        "Industry",
        _labels,
        horizontal=True,
        key="industry_selector",
        label_visibility="collapsed",
    )
    industry = available[_labels.index(_sel)]

industry_icon, industry_label = INDUSTRY_META.get(
    industry, ("🏭", industry.replace("-", " ").title())
)
pfx = f"/{industry}"

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def api_get(path: str, params: dict = None):
    try:
        r = requests.get(f"{API}{path}", params=params, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def api_post(path: str, json: dict = None):
    try:
        r = requests.post(f"{API}{path}", json=json or {}, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


def api_patch(path: str, json: dict):
    try:
        r = requests.patch(f"{API}{path}", json=json, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


def api_delete(path: str):
    try:
        r = requests.delete(f"{API}{path}", timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


# ---------------------------------------------------------------------------
# Auto-refresh logic
# ---------------------------------------------------------------------------

if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()


def maybe_rerun():
    if time.time() - st.session_state.last_refresh > REFRESH_INTERVAL:
        st.session_state.last_refresh = time.time()
        st.rerun()


# ---------------------------------------------------------------------------
# Status badge helper
# ---------------------------------------------------------------------------

def status_badge(state: dict) -> str:
    if not state:
        return "🔴 API Unreachable"
    if state.get("mode") == "stopped" or not state.get("is_running"):
        return "🔴 Stopped"
    if state.get("is_paused"):
        return "🟡 Paused"
    if state.get("mode") == "backfill":
        return "🔵 Backfilling"
    return "🟢 Running"


# ---------------------------------------------------------------------------
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
        "notes": "Only store locations have stock records. When below reorder_point, ordering.store_orders are created.",
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
TABLE_DOCS_BY_INDUSTRY = {
    "gas-station": GAS_STATION_TABLE_DOCS,
    "grocery": GROCERY_TABLE_DOCS,
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
        ("pos.products", "Products"),
        ("pos.departments", "Departments"),
        ("pos.coupons", "Coupons"),
        ("pos.combo_deals", "Combo Deals"),
        ("pos.loyalty_members", "Loyalty Members"),
        ("pos.price_history", "Product Price History"),
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
        ("inv.receipts", "Receipts"),
        ("inv.receipt_items", "Receipt Items"),
        ("inv.products", "Product Config"),
    ],
    "Control": [
        ("control.generator_state", "Generator State"),
        ("control.generation_stats", "Generation Stats"),
    ],
}

SCHEMA_TABLES_BY_INDUSTRY = {
    "gas-station": GAS_STATION_SCHEMA_TABLES,
    "grocery": GROCERY_SCHEMA_TABLES,
}

# Tables that require date filters
NEEDS_DATES = {
    "pos.transactions", "pos.transaction_items", "fuel.transactions",
    "inv.receipts", "inv.receipt_items",
    "timeclock.events", "ordering.store_orders", "transport.loads",
}

# Tables that support location filter
NEEDS_LOCATION = {
    "hr.employees", "pos.transactions", "pos.transaction_items",
    "fuel.transactions", "fuel.pumps", "inv.stock_levels",
    "inv.receipts", "inv.receipt_items",
    "timeclock.events", "ordering.store_orders", "transport.loads",
}


# ---------------------------------------------------------------------------
# Table Explorer — data loader
# ---------------------------------------------------------------------------

def _load_table(table: str, start_date, end_date, loc_id, limit: int, extra: dict, pfx: str):
    sd = f"{start_date}T00:00:00" if start_date else None
    ed = f"{end_date}T23:59:59" if end_date else None

    def paged(path, params):
        r = api_get(path, params)
        if not r:
            return pd.DataFrame(), 0
        return pd.DataFrame(r.get("data", [])), r.get("total", 0)

    def flat(path, params):
        r = api_get(path, params)
        if not r:
            return pd.DataFrame(), 0
        rows = r.get("data", r) if isinstance(r, dict) else r
        if not isinstance(rows, list):
            rows = []
        return pd.DataFrame(rows), len(rows)

    p: dict = {}
    if loc_id:
        p["location_id"] = loc_id
    p["limit"] = limit

    # --- Shared tables ---
    if table == "hr.locations":
        return flat(f"{pfx}/hr/locations", {})
    if table == "hr.employees":
        p.update(extra)
        return flat(f"{pfx}/hr/employees", p)
    if table == "pos.transactions":
        p.update({"start_dt": sd, "end_dt": ed})
        return paged(f"{pfx}/pos/transactions", p)
    if table == "pos.transaction_items":
        p.update({"start_dt": sd, "end_dt": ed})
        return paged(f"{pfx}/pos/transaction-items", p)
    if table == "pos.products":
        p.update(extra)
        return flat(f"{pfx}/pos/products", p)
    if table == "pos.loyalty_members":
        p.update(extra)
        return paged(f"{pfx}/pos/loyalty-members", p)
    if table == "pos.price_history":
        return flat(f"{pfx}/pos/price-history", {"limit": limit})
    if table == "inv.stock_levels":
        p.update(extra)
        return flat(f"{pfx}/inventory/stock-levels", p)
    if table == "inv.receipts":
        p.update({"start_dt": sd, "end_dt": ed})
        return flat(f"{pfx}/inventory/receipts", p)
    if table == "inv.receipt_items":
        p.update({"start_dt": sd, "end_dt": ed})
        return paged(f"{pfx}/inventory/receipt-items", p)
    if table == "inv.products":
        return flat(f"{pfx}/inventory/products", {"limit": limit})
    if table == "control.generator_state":
        r = api_get(f"{pfx}/status")
        if not r:
            return pd.DataFrame(), 0
        return pd.DataFrame([r.get("state", {})]), 1
    if table == "control.generation_stats":
        n = extra.get("last_n_ticks", 100)
        return flat(f"{pfx}/stats/generation", {"last_n_ticks": n})

    # --- Gas-station-only tables ---
    if table == "fuel.transactions":
        p.update({"start_dt": sd, "end_dt": ed})
        return paged(f"{pfx}/fuel/transactions", p)
    if table == "fuel.grades":
        return flat(f"{pfx}/fuel/grades", {})
    if table == "fuel.price_history":
        return flat(f"{pfx}/fuel/price-history", {"limit": limit})
    if table == "fuel.pumps":
        return flat(f"{pfx}/fuel/pumps", p)

    # --- Grocery-only tables ---
    if table == "pos.departments":
        return flat(f"{pfx}/pos/departments", {})
    if table == "pos.coupons":
        return flat(f"{pfx}/pos/coupons", {})
    if table == "pos.combo_deals":
        return flat(f"{pfx}/pos/combo-deals", {})
    if table == "timeclock.events":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("employee_id"):
            p["employee_id"] = extra["employee_id"]
        return paged(f"{pfx}/timeclock/events", p)
    if table == "ordering.store_orders":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("status"):
            p["status"] = extra["status"]
        return paged(f"{pfx}/ordering/orders", p)
    if table == "ordering.store_order_items":
        return flat(f"{pfx}/ordering/orders", {"limit": limit})
    if table == "fulfillment.orders":
        if extra.get("status"):
            p["status"] = extra["status"]
        return paged(f"{pfx}/fulfillment/orders", p)
    if table == "fulfillment.items":
        return paged(f"{pfx}/fulfillment/orders", p)
    if table == "transport.trucks":
        return flat(f"{pfx}/transport/trucks", {})
    if table == "transport.loads":
        p.update({"start_dt": sd, "end_dt": ed})
        if extra.get("status"):
            p["status"] = extra["status"]
        return paged(f"{pfx}/transport/loads", p)

    return pd.DataFrame(), 0


# ---------------------------------------------------------------------------
# Scenarios per industry
# ---------------------------------------------------------------------------

GAS_STATION_SCENARIOS = {
    "normal": {
        "label": "Normal",
        "icon": "🏪",
        "description": "Baseline traffic with realistic time-of-day and day-of-week patterns.",
    },
    "rush_hour": {
        "label": "Rush Hour",
        "icon": "🚗",
        "description": "2.5× volume during morning (7–9am) and evening (4–7pm) commute peaks.",
    },
    "weekend": {
        "label": "Weekend",
        "icon": "🎉",
        "description": "1.3× baseline volume. Simulates higher foot traffic on Friday and Saturday.",
    },
    "promotion": {
        "label": "Promotion",
        "icon": "🏷️",
        "description": "15% discount applied to Snacks and Beverages. Slightly higher volume.",
    },
    "fuel_spike": {
        "label": "Fuel Price Spike",
        "icon": "⬆️",
        "description": "Fuel prices increased by ~12%. Simulates a supply disruption or market event.",
    },
}

GROCERY_SCENARIOS = {
    "normal": {
        "label": "Normal",
        "icon": "🏪",
        "description": "Baseline traffic with grocery shopping patterns (morning and evening peaks).",
    },
    "rush_hour": {
        "label": "Rush Hour",
        "icon": "🚗",
        "description": "2.0× volume during after-work hours (4–7pm) and weekend mornings.",
    },
    "weekend": {
        "label": "Weekend",
        "icon": "🛒",
        "description": "1.3× baseline. Simulates higher weekend grocery shopping traffic.",
    },
    "promotion": {
        "label": "Promotion",
        "icon": "🏷️",
        "description": "15% discount on featured departments. Higher basket sizes.",
    },
    "holiday_week": {
        "label": "Holiday Week",
        "icon": "🦃",
        "description": "1.6× volume, increased produce and meat sales. Simulates Thanksgiving or holiday week shopping.",
    },
    "double_coupons": {
        "label": "Double Coupons",
        "icon": "✂️",
        "description": "Coupon value doubled. Higher loyalty member attach rate. Simulates a double-coupon event.",
    },
}

SCENARIOS_BY_INDUSTRY = {
    "gas-station": GAS_STATION_SCENARIOS,
    "grocery": GROCERY_SCENARIOS,
}


# ---------------------------------------------------------------------------
# Main layout
# ---------------------------------------------------------------------------

st.title(f"{industry_icon} Verisim — {industry_label}")

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Dashboard", "⚙️ Generator Control", "🎭 Scenarios", "🏷️ Promotions",
    "📈 Distributions", "🗄️ Table Explorer", "📖 Documentation"
])


# ===========================================================================
# TAB 1 — Dashboard
# ===========================================================================
with tab1:
    status_data = api_get(f"{pfx}/status")
    today_data = api_get(f"{pfx}/stats/today")
    gen_stats = api_get(f"{pfx}/stats/generation", {"last_n_ticks": 200})

    if status_data:
        state = status_data.get("state", {})
        col_badge, col_scenario, col_tick = st.columns([2, 2, 3])
        col_badge.metric("Generator Status", status_badge(state))
        col_scenario.metric("Active Scenario", state.get("active_scenario", "—").replace("_", " ").title())
        last_tick = state.get("last_tick_at")
        col_tick.metric("Last Tick", last_tick[:19].replace("T", " ") if last_tick else "Never")
    else:
        st.error(f"Cannot reach API at `{pfx}/status`. Is the generator running?")

    st.divider()

    if today_data:
        state = (status_data or {}).get("state", {})
        if industry == "gas-station":
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("POS Transactions Today", f"{today_data.get('pos_transactions', 0):,}")
            c2.metric("Fuel Transactions Today", f"{today_data.get('fuel_transactions', 0):,}")
            c3.metric("Ticks Today", f"{today_data.get('ticks', 0):,}")
            c4.metric("Volume Multiplier", f"{state.get('volume_multiplier', 1.0):.1f}×" if status_data else "—")
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("POS Transactions Today", f"{today_data.get('pos_transactions', 0):,}")
            c2.metric("Timeclock Events Today", f"{today_data.get('timeclock_events', 0):,}")
            c3.metric("Orders Today", f"{today_data.get('orders', 0):,}")
            c4.metric("Volume Multiplier", f"{state.get('volume_multiplier', 1.0):.1f}×" if status_data else "—")

    st.divider()

    if gen_stats:
        df = pd.DataFrame(gen_stats)
        if not df.empty:
            df["recorded_at"] = pd.to_datetime(df["recorded_at"])
            df = df.sort_values("recorded_at")

            col_a, col_b = st.columns(2)
            with col_a:
                st.subheader("POS Transactions per Tick")
                fig = px.line(df, x="recorded_at", y="pos_transactions_generated",
                              color="scenario_tag",
                              labels={"recorded_at": "Time", "pos_transactions_generated": "Count"})
                fig.update_layout(height=300, margin=dict(t=20, b=20))
                st.plotly_chart(fig, use_container_width=True)

            with col_b:
                if industry == "gas-station" and "fuel_transactions_generated" in df.columns:
                    st.subheader("Fuel Transactions per Tick")
                    fig2 = px.line(df, x="recorded_at", y="fuel_transactions_generated",
                                   labels={"recorded_at": "Time", "fuel_transactions_generated": "Count"})
                    fig2.update_layout(height=300, margin=dict(t=20, b=20))
                    st.plotly_chart(fig2, use_container_width=True)
                elif industry == "grocery" and "timeclock_events_generated" in df.columns:
                    st.subheader("Timeclock Events per Tick")
                    fig2 = px.line(df, x="recorded_at", y="timeclock_events_generated",
                                   labels={"recorded_at": "Time", "timeclock_events_generated": "Count"})
                    fig2.update_layout(height=300, margin=dict(t=20, b=20))
                    st.plotly_chart(fig2, use_container_width=True)

            if "wall_clock_ms" in df.columns:
                st.subheader("Tick Duration (ms)")
                fig3 = px.bar(df.tail(50), x="recorded_at", y="wall_clock_ms",
                              labels={"recorded_at": "Time", "wall_clock_ms": "ms"})
                fig3.update_layout(height=200, margin=dict(t=10, b=10))
                st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No generation stats yet. Start the generator to see live data.")

    bf = api_get(f"{pfx}/stats/backfill-progress")
    if bf and bf.get("in_progress"):
        st.subheader("Backfill Progress")
        st.progress(bf["pct_complete"] / 100,
                    text=f"{bf['pct_complete']}% — Day {bf['current']} of {bf['end']} ({bf['days_remaining']} days remaining)")

    st.divider()

    # Last-hour activity
    recent = api_get(f"{pfx}/stats/recent", {"minutes": 60}) if industry == "grocery" else None
    if recent:
        st.subheader("Last Hour")
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Transactions", f"{recent.get('pos_transactions', 0):,}")
        r2.metric("Timeclock Events", f"{recent.get('timeclock_events', 0):,}")
        r3.metric("Orders", f"{recent.get('orders', 0):,}")
        r4.metric("Ticks", f"{recent.get('ticks', 0):,}")

        txns = recent.get("recent_transactions", [])
        if txns:
            df_recent = pd.DataFrame(txns)
            if "transaction_dt" in df_recent.columns:
                df_recent["transaction_dt"] = pd.to_datetime(df_recent["transaction_dt"]).dt.strftime("%H:%M:%S")
            if "total" in df_recent.columns:
                df_recent["total"] = df_recent["total"].apply(lambda x: f"${float(x):.2f}" if x else "—")
            show = [c for c in ["transaction_dt", "store", "total", "payment_method", "scenario_tag"] if c in df_recent.columns]
            st.dataframe(df_recent[show].rename(columns={
                "transaction_dt": "Time", "store": "Store", "total": "Total",
                "payment_method": "Payment", "scenario_tag": "Scenario"
            }), use_container_width=True, hide_index=True)

    st.caption(f"Auto-refreshes every {REFRESH_INTERVAL}s. Last refresh: {datetime.now().strftime('%H:%M:%S')}")
    maybe_rerun()


# ===========================================================================
# TAB 2 — Generator Control
# ===========================================================================
with tab2:
    st.subheader("Generator Control")
    status_data2 = api_get(f"{pfx}/status")
    state2 = status_data2.get("state", {}) if status_data2 else {}

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("#### Start / Stop")
        b1, b2, b3, b4 = st.columns(4)
        if b1.button("▶ Start", type="primary", use_container_width=True):
            mode = st.session_state.get("start_mode", "realtime")
            payload = {"mode": mode}
            if mode == "backfill":
                bf_start = st.session_state.get("bf_start", date.today() - timedelta(days=7))
                bf_end   = st.session_state.get("bf_end",   date.today() - timedelta(days=1))
                if not bf_start or not bf_end:
                    st.error("Backfill requires both start and end dates.")
                    st.stop()
                if bf_end <= bf_start:
                    st.error(f"End date ({bf_end}) must be after start date ({bf_start}).")
                    st.stop()
                payload["backfill_start"] = str(bf_start)
                payload["backfill_end"]   = str(bf_end)
            result = api_post(f"{pfx}/generator/start", payload)
            if result:
                if mode == "backfill":
                    st.toast(f"Backfill queued: {payload['backfill_start']} → {payload['backfill_end']}. Generator will pick it up within {state2.get('tick_interval_seconds', 30)}s.", icon="🔵")
                else:
                    st.toast("Generator started in realtime mode.", icon="🟢")
                st.rerun()
        if b2.button("⏹ Stop", use_container_width=True):
            api_post(f"{pfx}/generator/stop")
            st.rerun()
        if b3.button("⏸ Pause", use_container_width=True):
            api_post(f"{pfx}/generator/pause")
            st.rerun()
        if b4.button("▶▶ Resume", use_container_width=True):
            api_post(f"{pfx}/generator/resume")
            st.rerun()

        st.markdown("#### Mode")
        mode_choice = st.radio("Generation mode", ["realtime", "backfill"], horizontal=True, key="start_mode")
        if mode_choice == "backfill":
            st.date_input("Backfill start date", value=date.today() - timedelta(days=30), key="bf_start")
            st.date_input("Backfill end date", value=date.today() - timedelta(days=1), key="bf_end")
            st.caption("Set dates above, then click ▶ Start to begin backfill.")

        st.markdown("#### Volume & Timing")
        new_multiplier = st.slider(
            "Volume multiplier", min_value=0.1, max_value=5.0, step=0.1,
            value=float(state2.get("volume_multiplier", 1.0))
        )
        tick_options = {15: "15 sec", 30: "30 sec", 60: "1 min", 300: "5 min", 600: "10 min"}
        current_tick = int(state2.get("tick_interval_seconds", 30))
        tick_choice = st.selectbox(
            "Tick interval", options=list(tick_options.keys()),
            format_func=lambda x: tick_options[x],
            index=list(tick_options.keys()).index(current_tick) if current_tick in tick_options else 1
        )
        if st.button("Save Config", type="secondary"):
            api_patch(f"{pfx}/generator/config", {
                "volume_multiplier": new_multiplier,
                "tick_interval_seconds": tick_choice,
            })
            st.success("Config saved.")
            st.rerun()

    with col_right:
        st.markdown("#### Current State")
        if state2:
            st.json(state2)

        bf2 = api_get(f"{pfx}/stats/backfill-progress")
        if bf2 and bf2.get("in_progress"):
            st.markdown("#### Backfill Progress")
            st.progress(bf2["pct_complete"] / 100,
                        text=f"{bf2['pct_complete']}% complete — {bf2['days_remaining']} days remaining")


# ===========================================================================
# TAB 3 — Scenarios
# ===========================================================================
with tab3:
    st.subheader("Scenarios")

    SCENARIOS = SCENARIOS_BY_INDUSTRY[industry]

    if industry == "grocery":
        active_scenarios_list = api_get("/grocery/generator/scenarios") or []
        active_scenario_names = {s["scenario_name"] for s in active_scenarios_list}
    else:
        status_data3 = api_get(f"{pfx}/status")
        active_scenario_names = {(status_data3 or {}).get("state", {}).get("active_scenario", "normal")}

    cols = st.columns(3)
    for i, (key, info) in enumerate(SCENARIOS.items()):
        with cols[i % 3]:
            is_active = key in active_scenario_names
            badge = " ✅ Active" if is_active else ""
            with st.container(border=True):
                st.markdown(f"### {info['icon']} {info['label']}{badge}")
                st.caption(info["description"])
                if industry == "grocery":
                    if is_active:
                        if st.button(f"Deactivate", key=f"sc_off_{key}"):
                            api_delete(f"/grocery/generator/scenarios/{key}")
                            st.rerun()
                    else:
                        if st.button(f"Activate", key=f"sc_on_{key}"):
                            api_post(f"/grocery/generator/scenarios", {"scenario_name": key})
                            st.rerun()
                else:
                    if not is_active:
                        if st.button(f"Activate {info['label']}", key=f"sc_{key}"):
                            api_patch(f"{pfx}/generator/config", {"active_scenario": key})
                            st.rerun()

    if industry == "grocery":
        st.divider()
        st.subheader("Scenario Schedules")
        st.caption("Scheduled scenarios automatically activate during backfill and realtime generation on their date range.")

        schedules = api_get("/grocery/generator/scenario-schedules") or []
        if schedules:
            df_sched = pd.DataFrame(schedules)
            for _, row in df_sched.iterrows():
                c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 3, 1])
                c1.write(row["scenario_name"].replace("_", " ").title())
                c2.write(str(row["start_date"]))
                c3.write(str(row["end_date"]))
                c4.write(row.get("label") or "")
                if c5.button("✕", key=f"del_sched_{row['schedule_id']}"):
                    api_delete(f"/grocery/generator/scenario-schedules/{row['schedule_id']}")
                    st.rerun()
        else:
            st.info("No scenario schedules. Add one below.")

        with st.expander("Add Schedule"):
            sc_col1, sc_col2, sc_col3 = st.columns(3)
            sched_scenario = sc_col1.selectbox("Scenario", [k for k in SCENARIOS if k != "normal"], key="sched_sc")
            sched_start = sc_col2.date_input("Start date", key="sched_start")
            sched_end = sc_col3.date_input("End date", key="sched_end")
            sched_label = st.text_input("Label (optional)", key="sched_label", placeholder="e.g. Summer Sale")
            if st.button("Add Schedule", type="primary"):
                if sched_end < sched_start:
                    st.error("End date must be after start date.")
                else:
                    api_post("/grocery/generator/scenario-schedules", {
                        "scenario_name": sched_scenario,
                        "start_date": str(sched_start),
                        "end_date": str(sched_end),
                        "label": sched_label or None,
                    })
                    st.rerun()

    st.divider()

    if industry == "gas-station":
        st.subheader("Current Fuel Prices")
        grades = api_get(f"{pfx}/fuel/grades")
        if grades:
            df_g = pd.DataFrame(grades)[["name", "octane_rating", "current_price", "updated_at"]]
            df_g.columns = ["Grade", "Octane", "Price/Gallon", "Last Updated"]
            df_g["Price/Gallon"] = df_g["Price/Gallon"].apply(lambda x: f"${float(x):.4f}")
            st.table(df_g)

        price_hist = api_get(f"{pfx}/fuel/price-history", {"limit": 50})
        if price_hist:
            st.subheader("Recent Fuel Price Changes")
            df_ph = pd.DataFrame(price_hist)[["grade_name", "old_price", "new_price", "changed_at"]]
            df_ph.columns = ["Grade", "Old Price", "New Price", "Changed At"]
            st.dataframe(df_ph, use_container_width=True, hide_index=True)

    else:
        st.subheader("Active Combo Deals")
        deals = api_get(f"{pfx}/pos/combo-deals")
        if deals:
            df_d = pd.DataFrame(deals)
            show_cols = [c for c in ["name", "deal_type", "trigger_qty", "deal_price", "valid_from", "valid_until"] if c in df_d.columns]
            if show_cols:
                st.dataframe(df_d[show_cols], use_container_width=True, hide_index=True)
        else:
            st.info("No active combo deals.")


# ===========================================================================
# TAB 4 — Promotions (Grocery only)
# ===========================================================================
with tab4:
    if industry != "grocery":
        st.info("Promotions management is only available for the grocery industry.")
    else:
        st.subheader("Promotions")

        promo_tab_coupons, promo_tab_ads = st.tabs(["🎟️ Coupons", "📰 Weekly Ads"])

        # ── Coupons ──────────────────────────────────────────────────────────
        with promo_tab_coupons:
            st.markdown("#### Active Coupons")
            coupons_all = api_get("/grocery/pos/coupons", {"active_only": False, "limit": 500}) or []

            if coupons_all:
                for coup in coupons_all:
                    with st.container(border=True):
                        cc1, cc2, cc3, cc4, cc5 = st.columns([2, 2, 1, 2, 1])
                        cc1.markdown(f"**{coup['code']}** — {coup['description']}")
                        cc2.write(f"{coup['coupon_type'].replace('_',' ').title()} · ${coup['discount_value']}")
                        cc3.write("✅ Active" if coup.get("is_active") else "❌ Inactive")
                        cc4.write(f"{coup.get('valid_from','?')} → {coup.get('valid_until','?')}")
                        with cc5:
                            if st.button("Deactivate" if coup.get("is_active") else "Activate",
                                         key=f"coup_toggle_{coup['coupon_id']}"):
                                api_patch(f"/grocery/pos/coupons/{coup['coupon_id']}",
                                          {"is_active": not coup.get("is_active")})
                                st.rerun()
                            if st.button("🗑", key=f"coup_del_{coup['coupon_id']}"):
                                api_delete(f"/grocery/pos/coupons/{coup['coupon_id']}")
                                st.rerun()
            else:
                st.info("No coupons found.")

            st.divider()
            with st.expander("➕ Create Coupon"):
                departments = api_get("/grocery/pos/departments") or []
                dept_options = {d["name"]: d["department_id"] for d in departments}
                c1, c2, c3 = st.columns(3)
                new_code = c1.text_input("Code", key="nc_code")
                new_type = c2.selectbox("Type", ["percent_off", "dollar_off", "bogo", "free_item"], key="nc_type")
                new_val = c3.number_input("Discount value", min_value=0.01, value=5.0, key="nc_val")
                new_desc = st.text_input("Description", key="nc_desc")
                c4, c5, c6 = st.columns(3)
                new_dept = c4.selectbox("Department (optional)", ["— None —"] + list(dept_options.keys()), key="nc_dept")
                new_from = c5.date_input("Valid from", value=date.today(), key="nc_from")
                new_until = c6.date_input("Valid until", value=date.today() + timedelta(days=30), key="nc_until")
                new_min = st.number_input("Min purchase ($, 0 = none)", min_value=0.0, value=0.0, key="nc_min")
                new_max_uses = st.number_input("Max uses (0 = unlimited)", min_value=0, value=0, step=1, key="nc_maxu")
                if st.button("Create Coupon", type="primary", key="nc_submit"):
                    if not new_code or not new_desc:
                        st.error("Code and description are required.")
                    else:
                        payload = {
                            "code": new_code, "description": new_desc,
                            "coupon_type": new_type, "discount_value": new_val,
                            "valid_from": str(new_from), "valid_until": str(new_until),
                            "min_purchase": new_min if new_min > 0 else None,
                            "max_uses": int(new_max_uses) if new_max_uses > 0 else None,
                            "department_id": dept_options.get(new_dept) if new_dept != "— None —" else None,
                            "is_active": True,
                        }
                        result = api_post("/grocery/pos/coupons", payload)
                        if result:
                            st.success(f"Coupon '{new_code}' created.")
                            st.rerun()

        # ── Weekly Ads ────────────────────────────────────────────────────────
        with promo_tab_ads:
            st.markdown("#### Weekly Ads")
            ads_resp = api_get("/grocery/pricing/weekly-ads", {"limit": 100})
            ads = (ads_resp.get("data") if isinstance(ads_resp, dict) else ads_resp) or []
            _products_resp = api_get("/grocery/pos/products", {"limit": 2000}) or {}
            products_list = (_products_resp.get("data") if isinstance(_products_resp, dict) else _products_resp) or []
            prod_options = {p["name"]: p["product_id"] for p in products_list}

            if ads:
                for ad in ads:
                    ad_items_resp = api_get("/grocery/pricing/ad-items", {"ad_id": ad["ad_id"], "limit": 200})
                    ad_items = (ad_items_resp.get("data") if isinstance(ad_items_resp, dict) else ad_items_resp) or []
                    with st.expander(f"📰 {ad['ad_name']} ({ad['start_date']} → {ad['end_date']}) — {len(ad_items)} items"):
                        ac1, ac2 = st.columns([5, 1])
                        with ac2:
                            if st.button("Delete Ad", key=f"ad_del_{ad['ad_id']}"):
                                api_delete(f"/grocery/pricing/weekly-ads/{ad['ad_id']}")
                                st.rerun()

                        if ad_items:
                            for item in ad_items:
                                ic1, ic2, ic3, ic4 = st.columns([3, 2, 2, 1])
                                ic1.write(item.get("product_id", "")[:8] + "…")
                                ic2.write(f"${item['promoted_price']}" if item.get("promoted_price") else "—")
                                ic3.write(f"{item['discount_pct']}% off" if item.get("discount_pct") else "—")
                                if ic4.button("✕", key=f"aditem_del_{item['ad_item_id']}"):
                                    api_delete(f"/grocery/pricing/ad-items/{item['ad_item_id']}")
                                    st.rerun()

                        st.markdown("**Add item to this ad:**")
                        ai1, ai2, ai3, ai4 = st.columns([3, 2, 2, 1])
                        sel_prod = ai1.selectbox("Product", ["— select —"] + list(prod_options.keys()), key=f"ai_prod_{ad['ad_id']}")
                        ai_price = ai2.number_input("Promoted price", min_value=0.0, value=0.0, key=f"ai_price_{ad['ad_id']}")
                        ai_pct = ai3.number_input("Discount %", min_value=0.0, max_value=100.0, value=0.0, key=f"ai_pct_{ad['ad_id']}")
                        if ai4.button("Add", key=f"ai_add_{ad['ad_id']}"):
                            if sel_prod == "— select —":
                                st.error("Select a product.")
                            else:
                                api_post("/grocery/pricing/ad-items", {
                                    "ad_id": ad["ad_id"],
                                    "product_id": prod_options[sel_prod],
                                    "promoted_price": ai_price if ai_price > 0 else None,
                                    "discount_pct": ai_pct if ai_pct > 0 else None,
                                })
                                st.rerun()
            else:
                st.info("No weekly ads found.")

            st.divider()
            with st.expander("➕ Create Weekly Ad"):
                na1, na2, na3 = st.columns(3)
                new_ad_name = na1.text_input("Ad name", key="na_name")
                new_ad_start = na2.date_input("Start date", value=date.today(), key="na_start")
                new_ad_end = na3.date_input("End date", value=date.today() + timedelta(days=6), key="na_end")
                if st.button("Create Ad", type="primary", key="na_submit"):
                    if not new_ad_name:
                        st.error("Ad name is required.")
                    elif new_ad_end < new_ad_start:
                        st.error("End date must be after start date.")
                    else:
                        result = api_post("/grocery/pricing/weekly-ads", {
                            "ad_name": new_ad_name,
                            "start_date": str(new_ad_start),
                            "end_date": str(new_ad_end),
                        })
                        if result:
                            st.success(f"Ad '{new_ad_name}' created.")
                            st.rerun()


# ===========================================================================
# TAB 5 — Distributions
# ===========================================================================
with tab5:
    st.subheader("Data Distributions")
    st.caption("Record counts grouped by day and key classifications. Adjust the look-back window to explore different time ranges.")

    _days_opts = {
        "Last 7 days": 7, "Last 14 days": 14, "Last 30 days": 30,
        "Last 60 days": 60, "Last 90 days": 90, "Last 180 days": 180, "Last year": 365
    }
    _days_sel = st.selectbox("Look-back window", list(_days_opts.keys()), index=2, key="dist_days")
    _days = _days_opts[_days_sel]

    dist = api_get(f"{pfx}/stats/distributions", {"days": _days})

    if not dist:
        st.warning("No distribution data available. Run the generator first.")
    else:
        def _bar(data, x, y, title, color=None, labels=None):
            if not data:
                st.info(f"No data for: {title}")
                return
            df = pd.DataFrame(data)
            fig = px.bar(df, x=x, y=y, title=title, color=color, labels=labels or {},
                         color_discrete_sequence=px.colors.qualitative.Safe)
            fig.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=320)
            st.plotly_chart(fig, use_container_width=True)

        # ── Transactions per day (full width) ────────────────────────────────
        st.markdown("#### POS Transactions")
        txn_day = dist.get("transactions_by_day", [])
        if txn_day:
            df_td = pd.DataFrame(txn_day)
            df_td["day"] = pd.to_datetime(df_td["day"])
            fig_td = px.bar(df_td, x="day", y="transaction_count",
                            title="Transactions per Day",
                            labels={"day": "Date", "transaction_count": "Transactions"},
                            color_discrete_sequence=["#4C78A8"])
            fig_td.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=300)
            st.plotly_chart(fig_td, use_container_width=True)
        else:
            st.info("No transaction data in window.")

        dc1, dc2 = st.columns(2)
        with dc1:
            _bar(dist.get("transactions_by_store"), "store_name", "transaction_count",
                 "Transactions by Store", labels={"store_name": "Store", "transaction_count": "Transactions"})
        with dc2:
            _bar(dist.get("transactions_by_payment"), "payment_method", "transaction_count",
                 "Transactions by Payment Method",
                 labels={"payment_method": "Method", "transaction_count": "Transactions"})

        dc3, dc4 = st.columns(2)
        with dc3:
            _bar(dist.get("transactions_by_scenario"), "scenario_tag", "transaction_count",
                 "Transactions by Scenario",
                 labels={"scenario_tag": "Scenario", "transaction_count": "Transactions"})
        with dc4:
            _bar(dist.get("employees_by_department"), "department", "employee_count",
                 "Active Employees by Department",
                 labels={"department": "Department", "employee_count": "Employees"})

        # ── Grocery-only charts ───────────────────────────────────────────────
        if industry == "grocery":
            st.divider()
            st.markdown("#### Timeclock Events")
            tc_day = dist.get("timeclock_by_day", [])
            if tc_day:
                df_tc = pd.DataFrame(tc_day)
                df_tc["day"] = pd.to_datetime(df_tc["day"])
                fig_tc = px.bar(df_tc, x="day", y="event_count",
                                title="Timeclock Events per Day",
                                labels={"day": "Date", "event_count": "Events"},
                                color_discrete_sequence=["#72B7B2"])
                fig_tc.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=300)
                st.plotly_chart(fig_tc, use_container_width=True)
            else:
                st.info("No timeclock data in window.")

            gc1, gc2 = st.columns(2)
            with gc1:
                _bar(dist.get("timeclock_by_type"), "event_type", "event_count",
                     "Events by Type",
                     labels={"event_type": "Type", "event_count": "Events"})
            with gc2:
                _bar(dist.get("products_by_department"), "department_name", "product_count",
                     "Active Products by Department",
                     labels={"department_name": "Department", "product_count": "Products"})

            st.divider()
            st.markdown("#### Supply Chain Orders")
            ord_day = dist.get("orders_by_day", [])
            if ord_day:
                df_od = pd.DataFrame(ord_day)
                df_od["day"] = pd.to_datetime(df_od["day"])
                fig_od = px.bar(df_od, x="day", y="order_count",
                                title="Store Orders per Day",
                                labels={"day": "Date", "order_count": "Orders"},
                                color_discrete_sequence=["#F58518"])
                fig_od.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=300)
                st.plotly_chart(fig_od, use_container_width=True)
            else:
                st.info("No order data in window.")

            _bar(dist.get("orders_by_status"), "status", "order_count",
                 "Orders by Status",
                 labels={"status": "Status", "order_count": "Orders"})

            st.divider()
            st.markdown("#### Shrinkage / Loss")
            shr_day = dist.get("shrinkage_by_day", [])
            if shr_day:
                df_sh = pd.DataFrame(shr_day)
                df_sh["day"] = pd.to_datetime(df_sh["day"])
                fig_sh = px.bar(df_sh, x="day", y="event_count",
                                title="Shrinkage Events per Day",
                                labels={"day": "Date", "event_count": "Events"},
                                color_discrete_sequence=["#E45756"])
                fig_sh.update_layout(margin=dict(t=36, b=0, l=0, r=0), height=300)
                st.plotly_chart(fig_sh, use_container_width=True)
            else:
                st.info("No shrinkage data in window.")

            _bar(dist.get("shrinkage_by_reason"), "reason", "event_count",
                 "Shrinkage by Reason",
                 labels={"reason": "Reason", "event_count": "Events"})

    maybe_rerun()


# ===========================================================================
# TAB 6 — Table Explorer
# ===========================================================================
with tab6:
    st.subheader("Table Explorer")


    SCHEMA_TABLES = SCHEMA_TABLES_BY_INDUSTRY[industry]
    TABLE_DOCS = TABLE_DOCS_BY_INDUSTRY[industry]

    # ── Row 1: schema + table selectors ──────────────────────────────────────
    col_s, col_t = st.columns([2, 5])
    with col_s:
        schema = st.selectbox("Schema", list(SCHEMA_TABLES.keys()), key="ex_schema")
    pairs = SCHEMA_TABLES[schema]
    tkeys = [p[0] for p in pairs]
    tlabels = [p[1] for p in pairs]
    with col_t:
        tidx = st.selectbox(
            "Table", range(len(pairs)),
            format_func=lambda i: f"{tkeys[i]}  —  {tlabels[i]}",
            key="ex_table",
        )
    table = tkeys[tidx]

    # ── Row 2: date range (only for tables that need it) ─────────────────────
    start_date = end_date = None
    if table in NEEDS_DATES:
        cd1, cd2, _ = st.columns([2, 2, 3])
        with cd1:
            start_date = st.date_input("Start date", value=date.today() - timedelta(days=1), key="ex_sd")
        with cd2:
            end_date = st.date_input("End date", value=date.today(), key="ex_ed")

    # ── Row 3: location + table-specific filters + row limit ──────────────────
    filter_slots: list = []
    if table in NEEDS_LOCATION:
        filter_slots.append("location")
    # Table-specific filters
    if table == "hr.employees":
        if industry == "gas-station":
            filter_slots += ["gs_department", "employee_status"]
        else:
            filter_slots += ["gr_department", "employee_status"]
    elif table == "pos.products":
        filter_slots.append("category")
    elif table == "pos.loyalty_members":
        filter_slots.append("loyalty_tier")
    elif table == "inv.stock_levels":
        filter_slots.append("below_reorder")
    elif table == "control.generation_stats":
        filter_slots.append("last_n_ticks")
    elif table in ("ordering.store_orders", "fulfillment.orders", "transport.loads"):
        filter_slots.append("order_status")
    filter_slots.append("row_limit")

    fcols = st.columns(min(len(filter_slots), 4))
    selected_loc_id = None
    extra: dict = {}
    limit = 500

    for fi, slot in enumerate(filter_slots):
        with fcols[fi % 4]:
            if slot == "location":
                locs = api_get(f"{pfx}/hr/locations") or []
                loc_opts = {"All Locations": None}
                for loc in locs:
                    loc_opts[loc["name"]] = loc["location_id"]
                loc_name = st.selectbox("Location", list(loc_opts.keys()), key="ex_loc")
                selected_loc_id = loc_opts[loc_name]

            elif slot == "gs_department":
                dept = st.selectbox("Department", ["All", "store", "fuel", "management"], key="ex_dept")
                if dept != "All":
                    extra["department"] = dept

            elif slot == "gr_department":
                gr_depts = ["All", "store", "produce", "deli", "bakery", "meat", "warehouse", "transport", "management"]
                dept = st.selectbox("Department", gr_depts, key="ex_dept")
                if dept != "All":
                    extra["department"] = dept

            elif slot == "employee_status":
                es = st.selectbox("Status", ["All", "active", "terminated", "on_leave"], key="ex_estatus")
                if es != "All":
                    extra["status"] = es

            elif slot == "category":
                if industry == "gas-station":
                    cats = ["All", "Beverages", "Snacks", "Tobacco", "Automotive", "Health & Beauty", "Food Service", "General Merchandise"]
                else:
                    cats = ["All", "Fresh Produce", "Dairy", "Meat & Poultry", "Bakery", "Deli", "Frozen Foods",
                            "Grocery", "Beverages", "Snacks", "Health & Beauty", "General Merchandise"]
                cat = st.selectbox("Category", cats, key="ex_cat")
                if cat != "All":
                    extra["category"] = cat

            elif slot == "loyalty_tier":
                tier = st.selectbox("Tier", ["All", "bronze", "silver", "gold", "platinum"], key="ex_tier")
                if tier != "All":
                    extra["tier"] = tier

            elif slot == "below_reorder":
                if st.checkbox("Below reorder point only", key="ex_brp"):
                    extra["below_reorder_point"] = True

            elif slot == "last_n_ticks":
                extra["last_n_ticks"] = st.number_input("Last N ticks", 10, 1000, 100, 10, key="ex_nticks")

            elif slot == "order_status":
                if table == "ordering.store_orders":
                    statuses = ["All", "pending", "approved", "shipped", "delivered"]
                elif table == "fulfillment.orders":
                    statuses = ["All", "picking", "packed", "dispatched"]
                else:
                    statuses = ["All", "dispatched", "delivered"]
                os_val = st.selectbox("Status", statuses, key="ex_ostatus")
                if os_val != "All":
                    extra["status"] = os_val

            elif slot == "row_limit":
                limit = st.number_input("Row limit", 50, 5000, 500, 50, key="ex_limit")

    # ── Table documentation (always visible, collapsed by default) ────────────
    doc = TABLE_DOCS.get(table, {})
    no_data_loaded = "ex_df" not in st.session_state or st.session_state.get("ex_table_loaded") != table
    with st.expander("📋 Table Documentation", expanded=no_data_loaded):
        if doc:
            st.markdown(f"#### {doc['title']}")
            st.caption(doc["description"])
            col_info, col_rel = st.columns([3, 2])
            with col_info:
                st.markdown("**Columns**")
                col_df = pd.DataFrame(doc["columns"], columns=["Column", "Type", "Description"])
                st.dataframe(col_df, use_container_width=True, hide_index=True, height=min(35 * len(doc["columns"]) + 38, 340))
            with col_rel:
                if doc.get("relationships"):
                    st.markdown("**Relationships**")
                    for r in doc["relationships"]:
                        st.markdown(f"- `{r}`")
                if doc.get("notes"):
                    st.info(doc["notes"])

    # ── Load button ───────────────────────────────────────────────────────────
    if st.button("⬇ Load Data", type="primary", key="ex_load"):
        with st.spinner(f"Loading {table}…"):
            df_result, total_result = _load_table(table, start_date, end_date, selected_loc_id, limit, extra, pfx)
        st.session_state["ex_df"] = df_result
        st.session_state["ex_total"] = total_result
        st.session_state["ex_table_loaded"] = table

    # ── Results grid ──────────────────────────────────────────────────────────
    if "ex_df" in st.session_state and st.session_state.get("ex_table_loaded") == table:
        df_show = st.session_state["ex_df"]
        total_show = st.session_state.get("ex_total", len(df_show))

        if df_show.empty:
            st.info("No rows returned for the selected filters.")
        else:
            st.caption(f"Showing **{len(df_show):,}** of **{total_show:,}** total rows in `{table}`")
            st.dataframe(df_show, use_container_width=True, hide_index=True)
            csv = df_show.to_csv(index=False)
            st.download_button(
                "⬇️ Download CSV",
                data=csv,
                file_name=f"{table.replace('.', '_')}_{date.today()}.csv",
                mime="text/csv",
                key="ex_csv",
            )


# ===========================================================================
# TAB 7 — Documentation
# ===========================================================================
with tab7:
    if industry == "gas-station":
        st.markdown("""
## Gas Station / Convenience Store

Continuous mock data platform for a **Gas Station / Convenience Store** operation.
Simulates 4 linked enterprise source systems backed by a single PostgreSQL database (`gas_station`).

---

### Source Systems

| System | Schema | Description |
|--------|--------|-------------|
| **HR** | `hr` | Employees and store locations |
| **POS** | `pos` | Point-of-sale transactions, products, loyalty members |
| **Fuel** | `fuel` | Fuel pump transactions, grades, price history |
| **Inventory** | `inv` | Stock levels, restocking events |

### Scenarios

| Scenario | Effect |
|----------|--------|
| `normal` | Baseline traffic with hourly + day-of-week patterns |
| `rush_hour` | 2.5× volume during hours 7–9am and 4–7pm |
| `weekend` | 1.3× baseline volume |
| `promotion` | 15% discount on Snacks & Beverages |
| `fuel_spike` | Fuel prices increased ~12% |

### API Reference

The FastAPI service exposes a full Swagger UI at `/docs`.

**Key endpoints (prefix: `/gas-station/`):**
- `GET /gas-station/status` — generator state
- `POST /gas-station/generator/start` — start realtime or backfill
- `PATCH /gas-station/generator/config` — change volume_multiplier, scenario, tick_interval
- `GET /gas-station/pos/transactions?start_dt=...&end_dt=...`
- `GET /gas-station/fuel/transactions?start_dt=...&end_dt=...`
- `GET /gas-station/fuel/grades` / `GET /gas-station/fuel/price-history`
- `GET /gas-station/hr/employees` / `GET /gas-station/hr/locations`
- `GET /gas-station/inventory/stock-levels`
- `GET /gas-station/stats/generation` — per-tick stats
- `GET /industries` — list all available industries
""")
    else:
        st.markdown("""
## Grocery Store

Continuous mock data platform for a **Grocery Store** operation.
Simulates 8 linked enterprise source systems backed by the `grocery` PostgreSQL database.

---

### Source Systems

| System | Schema | Description |
|--------|--------|-------------|
| **HR** | `hr` | Employees at stores and warehouses |
| **POS** | `pos` | Transactions, products, departments, coupons, combo deals |
| **Timeclock** | `timeclock` | Employee shift clock-in/clock-out events |
| **Ordering** | `ordering` | Store replenishment orders placed to warehouse |
| **Fulfillment** | `fulfillment` | Warehouse picks and packs orders |
| **Transport** | `transport` | Trucks and delivery loads to stores |
| **Inventory** | `inv` | Stock levels per product per store |

### Supply Chain Flow

```
Low stock detected → ordering.store_orders created
    → fulfillment.orders (warehouse picks)
    → transport.loads (truck dispatched)
    → inv.receipts (store receives, stock replenished)
```

### Scenarios

| Scenario | Effect |
|----------|--------|
| `normal` | Baseline grocery shopping patterns |
| `rush_hour` | 2.0× volume after-work and weekend mornings |
| `weekend` | 1.3× baseline volume |
| `promotion` | 15% discount on featured departments |
| `holiday_week` | 1.6× volume, heavy produce and meat |
| `double_coupons` | Coupon values doubled, higher loyalty attach |

### API Reference

The FastAPI service exposes a full Swagger UI at `/docs`.

**Key endpoints (prefix: `/grocery/`):**
- `GET /grocery/status` — generator state
- `POST /grocery/generator/start` — start realtime or backfill
- `PATCH /grocery/generator/config` — change volume_multiplier, scenario
- `GET /grocery/pos/transactions?start_dt=...&end_dt=...`
- `GET /grocery/pos/departments` / `GET /grocery/pos/coupons` / `GET /grocery/pos/combo-deals`
- `GET /grocery/timeclock/events?start_dt=...&end_dt=...`
- `GET /grocery/ordering/orders` / `GET /grocery/fulfillment/orders`
- `GET /grocery/transport/trucks` / `GET /grocery/transport/loads`
- `GET /grocery/hr/employees` / `GET /grocery/hr/locations`
- `GET /grocery/inventory/stock-levels`
- `GET /grocery/stats/generation`
- `GET /industries` — list all available industries
""")
