"""
Verisim Data Generator API — multi-industry FastAPI service.

Industries are determined by INDUSTRY_DBS env var (JSON dict) or individual
GAS_STATION_DB / GROCERY_DB env vars.  Each industry maps to a separate
database on the same postgres host.

Route pattern: /{industry}/...
  Shared endpoints (all industries): status, generator/*, hr/*, pos/*, inventory/*, stats/*
  Gas-station only: fuel/*
  Grocery only: pos/departments, pos/coupons, pos/combo-deals, timeclock/*, ordering/*, fulfillment/*, transport/*
"""
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta
from typing import Optional, List, Any, Dict

import psycopg2
import psycopg2.pool
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

log = logging.getLogger("api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

# ---------------------------------------------------------------------------
# Industry → DB name mapping
# ---------------------------------------------------------------------------

def _load_industry_dbs() -> Dict[str, str]:
    raw = os.environ.get("INDUSTRY_DBS")
    if raw:
        return json.loads(raw)
    dbs: Dict[str, str] = {}
    if os.environ.get("GAS_STATION_DB"):
        dbs["gas-station"] = os.environ["GAS_STATION_DB"]
    if os.environ.get("GROCERY_DB"):
        dbs["grocery"] = os.environ["GROCERY_DB"]
    # Fallback: single-DB legacy mode (POSTGRES_DB)
    if not dbs and os.environ.get("POSTGRES_DB"):
        dbs["gas-station"] = os.environ["POSTGRES_DB"]
    return dbs


INDUSTRY_DBS: Dict[str, str] = {}  # populated at startup

# ---------------------------------------------------------------------------
# Connection pool management
# ---------------------------------------------------------------------------

_pools: Dict[str, psycopg2.pool.SimpleConnectionPool] = {}


def _make_pool(dbname: str) -> psycopg2.pool.SimpleConnectionPool:
    return psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=dbname,
    )


def pool_for(industry: str) -> psycopg2.pool.SimpleConnectionPool:
    if industry not in INDUSTRY_DBS:
        raise HTTPException(404, f"Unknown industry '{industry}'. Available: {list(INDUSTRY_DBS)}")
    if industry not in _pools:
        _pools[industry] = _make_pool(INDUSTRY_DBS[industry])
    return _pools[industry]


def query(sql: str, params, industry: str) -> List[Dict]:
    pool = pool_for(industry)
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        pool.putconn(conn)


def query_write(sql: str, params, industry: str) -> List[Dict]:
    """Execute a write statement with RETURNING and commit."""
    pool = pool_for(industry)
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
        conn.commit()
        return rows
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def execute(sql: str, params, industry: str) -> int:
    pool = pool_for(industry)
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rowcount = cur.rowcount
        conn.commit()
        return rowcount
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global INDUSTRY_DBS
    INDUSTRY_DBS = _load_industry_dbs()
    log.info("Industries configured: %s", list(INDUSTRY_DBS))
    for industry in INDUSTRY_DBS:
        pool_for(industry)
        log.info("Pool ready for '%s' → db '%s'", industry, INDUSTRY_DBS[industry])
    yield
    for pool in _pools.values():
        pool.closeall()


app = FastAPI(
    title="Verisim Data Generator API",
    description="Multi-industry mock data platform — Gas Station, Grocery, and more",
    version="2.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class GeneratorStartRequest(BaseModel):
    mode: str = "realtime"
    backfill_start: Optional[date] = None
    backfill_end: Optional[date] = None
    # force=True: delete all existing data in the requested date range before
    # backfilling, so every day is regenerated from scratch (no skipping).
    # Use this when you want to replace existing data rather than fill gaps.
    force: bool = False


class GeneratorConfigPatch(BaseModel):
    volume_multiplier: Optional[float] = None
    active_scenario: Optional[str] = None
    tick_interval_seconds: Optional[int] = None


class ScenarioActivateRequest(BaseModel):
    scenario_name: str


class ScenarioScheduleRequest(BaseModel):
    scenario_name: str
    start_date: date
    end_date: date
    label: Optional[str] = None


class CouponCreate(BaseModel):
    code: str
    description: str
    coupon_type: str
    discount_value: float
    min_purchase: Optional[float] = None
    department_id: Optional[str] = None
    product_id: Optional[str] = None
    max_uses: Optional[int] = None
    valid_from: date = None
    valid_until: date = None
    is_active: bool = True


class CouponPatch(BaseModel):
    description: Optional[str] = None
    discount_value: Optional[float] = None
    min_purchase: Optional[float] = None
    valid_from: Optional[date] = None
    valid_until: Optional[date] = None
    is_active: Optional[bool] = None
    max_uses: Optional[int] = None


class WeeklyAdCreate(BaseModel):
    ad_name: str
    start_date: date
    end_date: date


class WeeklyAdPatch(BaseModel):
    ad_name: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class AdItemCreate(BaseModel):
    ad_id: str
    product_id: str
    promoted_price: Optional[float] = None
    discount_pct: Optional[float] = None


# ---------------------------------------------------------------------------
# Platform-level endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Platform"])
def health():
    results = {}
    for industry in INDUSTRY_DBS:
        try:
            rows = query("SELECT 1 AS ok", None, industry)
            results[industry] = rows[0]["ok"] == 1
        except Exception:
            results[industry] = False
    all_ok = all(results.values())
    return {"status": "ok" if all_ok else "degraded", "industries": results}


@app.get("/industries", tags=["Platform"])
def industries():
    return [{"industry": k, "db": v} for k, v in INDUSTRY_DBS.items()]


# ---------------------------------------------------------------------------
# Shared: status
# ---------------------------------------------------------------------------

@app.get("/{industry}/status", tags=["Status"])
def status(industry: str):
    state = query("SELECT * FROM control.generator_state WHERE state_id = 1", None, industry)
    if not state:
        raise HTTPException(503, "Generator state not initialised")

    # Stats columns differ per industry
    if industry == "grocery":
        today_stats = query("""
            SELECT
                COALESCE(SUM(pos_transactions_generated), 0)   AS pos_today,
                COALESCE(SUM(timeclock_events_generated), 0)   AS timeclock_today,
                COALESCE(SUM(orders_generated), 0)             AS orders_today,
                COUNT(*) AS ticks_today
            FROM control.generation_stats
            WHERE recorded_at >= CURRENT_DATE
        """, None, industry)
    else:
        today_stats = query("""
            SELECT
                COALESCE(SUM(pos_transactions_generated), 0)  AS pos_today,
                COALESCE(SUM(fuel_transactions_generated), 0) AS fuel_today,
                COUNT(*) AS ticks_today
            FROM control.generation_stats
            WHERE recorded_at >= CURRENT_DATE
        """, None, industry)

    return {"state": state[0], "today": today_stats[0]}


# ---------------------------------------------------------------------------
# Shared: generator control
# ---------------------------------------------------------------------------

VALID_SCENARIOS = {
    "gas-station": {"normal", "rush_hour", "weekend", "promotion", "fuel_spike"},
    "grocery":     {"normal", "rush_hour", "weekend", "promotion", "holiday_week", "double_coupons"},
}


def _clear_date_range(industry: str, start: date, end: date) -> None:
    """
    Delete all generated (time-series) data for the given date range so the
    generator can backfill it fresh.  Reference/seed data (products, employees,
    locations, etc.) is never touched.

    Deletions are ordered to respect FK constraints (children before parents).
    """
    from datetime import datetime as _dt
    start_ts = _dt(start.year, start.month, start.day, 0, 0, 0)
    end_ts   = _dt(end.year,   end.month,   end.day,   23, 59, 59)

    pool = pool_for(industry)
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            # POS: items + loyalty points → transactions
            cur.execute(
                "DELETE FROM pos.transaction_items "
                "WHERE transaction_id IN ("
                "  SELECT transaction_id FROM pos.transactions"
                "  WHERE transaction_dt BETWEEN %s AND %s)",
                (start_ts, end_ts))
            cur.execute(
                "DELETE FROM pos.loyalty_point_transactions "
                "WHERE transaction_id IN ("
                "  SELECT transaction_id FROM pos.transactions"
                "  WHERE transaction_dt BETWEEN %s AND %s)",
                (start_ts, end_ts))
            cur.execute(
                "DELETE FROM pos.transactions "
                "WHERE transaction_dt BETWEEN %s AND %s",
                (start_ts, end_ts))

            # Timeclock
            cur.execute(
                "DELETE FROM timeclock.events WHERE event_dt BETWEEN %s AND %s",
                (start_ts, end_ts))

            # Inventory: receipt_items → receipts; shrinkage
            cur.execute(
                "DELETE FROM inv.receipt_items "
                "WHERE receipt_id IN ("
                "  SELECT receipt_id FROM inv.receipts"
                "  WHERE received_dt BETWEEN %s AND %s)",
                (start_ts, end_ts))
            cur.execute(
                "DELETE FROM inv.receipts WHERE received_dt BETWEEN %s AND %s",
                (start_ts, end_ts))
            cur.execute(
                "DELETE FROM inv.shrinkage_events "
                "WHERE recorded_at BETWEEN %s AND %s",
                (start_ts, end_ts))

            # Transport: load_items → loads (keyed by created_at)
            cur.execute(
                "DELETE FROM transport.load_items "
                "WHERE load_id IN ("
                "  SELECT load_id FROM transport.loads"
                "  WHERE created_at BETWEEN %s AND %s)",
                (start_ts, end_ts))
            cur.execute(
                "DELETE FROM transport.loads WHERE created_at BETWEEN %s AND %s",
                (start_ts, end_ts))

            # Fulfillment: items → orders
            cur.execute(
                "DELETE FROM fulfillment.items "
                "WHERE fulfillment_id IN ("
                "  SELECT fulfillment_id FROM fulfillment.orders"
                "  WHERE created_at BETWEEN %s AND %s)",
                (start_ts, end_ts))
            cur.execute(
                "DELETE FROM fulfillment.orders WHERE created_at BETWEEN %s AND %s",
                (start_ts, end_ts))

            # Ordering: store_order_items → store_orders
            cur.execute(
                "DELETE FROM ordering.store_order_items "
                "WHERE order_id IN ("
                "  SELECT order_id FROM ordering.store_orders"
                "  WHERE created_at BETWEEN %s AND %s)",
                (start_ts, end_ts))
            cur.execute(
                "DELETE FROM ordering.store_orders WHERE created_at BETWEEN %s AND %s",
                (start_ts, end_ts))

            # HR schedules
            cur.execute(
                "DELETE FROM hr.schedules "
                "WHERE scheduled_date BETWEEN %s AND %s",
                (start, end))

        conn.commit()
        log.info("Cleared data for %s → %s in industry '%s'", start, end, industry)
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


@app.post("/{industry}/generator/start", tags=["Generator Control"])
def generator_start(industry: str, req: GeneratorStartRequest):
    pool_for(industry)  # validate
    if req.mode not in ("realtime", "backfill"):
        raise HTTPException(400, "mode must be 'realtime' or 'backfill'")

    if req.mode == "backfill":
        # Default: 30-day backfill ending today when no range is specified
        if not req.backfill_start:
            req.backfill_start = date.today() - timedelta(days=30)
        if not req.backfill_end:
            req.backfill_end = date.today()

        # force=True: clear all existing data in the requested range so every
        # day is regenerated from scratch rather than skipped.
        if req.force:
            _clear_date_range(industry, req.backfill_start, req.backfill_end)

    execute("""
        UPDATE control.generator_state
        SET is_running = TRUE, is_paused = FALSE, mode = %s,
            backfill_start_date = %s, backfill_end_date = %s,
            backfill_current_date = %s, started_at = NOW(), updated_at = NOW()
        WHERE state_id = 1
    """, (req.mode, req.backfill_start, req.backfill_end, req.backfill_start), industry)
    return query("SELECT * FROM control.generator_state WHERE state_id = 1", None, industry)[0]


@app.post("/{industry}/generator/stop", tags=["Generator Control"])
def generator_stop(industry: str):
    pool_for(industry)
    execute("""
        UPDATE control.generator_state
        SET is_running = FALSE, is_paused = FALSE, mode = 'stopped', updated_at = NOW()
        WHERE state_id = 1
    """, None, industry)
    return query("SELECT * FROM control.generator_state WHERE state_id = 1", None, industry)[0]


@app.post("/{industry}/generator/pause", tags=["Generator Control"])
def generator_pause(industry: str):
    pool_for(industry)
    execute("UPDATE control.generator_state SET is_paused = TRUE, updated_at = NOW() WHERE state_id = 1",
            None, industry)
    return query("SELECT * FROM control.generator_state WHERE state_id = 1", None, industry)[0]


@app.post("/{industry}/generator/resume", tags=["Generator Control"])
def generator_resume(industry: str):
    pool_for(industry)
    execute("UPDATE control.generator_state SET is_paused = FALSE, updated_at = NOW() WHERE state_id = 1",
            None, industry)
    return query("SELECT * FROM control.generator_state WHERE state_id = 1", None, industry)[0]


@app.patch("/{industry}/generator/config", tags=["Generator Control"])
def generator_config(industry: str, req: GeneratorConfigPatch):
    pool_for(industry)
    updates, params = [], []
    if req.volume_multiplier is not None:
        if not (0.1 <= req.volume_multiplier <= 10.0):
            raise HTTPException(400, "volume_multiplier must be 0.1–10.0")
        updates.append("volume_multiplier = %s")
        params.append(req.volume_multiplier)
    if req.active_scenario is not None:
        valid = VALID_SCENARIOS.get(industry, set())
        if req.active_scenario not in valid:
            raise HTTPException(400, f"active_scenario must be one of {valid}")
        updates.append("active_scenario = %s")
        params.append(req.active_scenario)
    if req.tick_interval_seconds is not None:
        if not (5 <= req.tick_interval_seconds <= 3600):
            raise HTTPException(400, "tick_interval_seconds must be 5–3600")
        updates.append("tick_interval_seconds = %s")
        params.append(req.tick_interval_seconds)
    if not updates:
        raise HTTPException(400, "No fields to update")
    updates.append("updated_at = NOW()")
    params.append(1)
    execute(f"UPDATE control.generator_state SET {', '.join(updates)} WHERE state_id = %s", params, industry)
    return query("SELECT * FROM control.generator_state WHERE state_id = 1", None, industry)[0]


# ---------------------------------------------------------------------------
# Shared: HR
# ---------------------------------------------------------------------------

@app.get("/{industry}/hr/locations", tags=["HR"])
def hr_locations(industry: str):
    pool_for(industry)
    if industry == "grocery":
        return query("""
            SELECT location_id, name, address, city, state, zip, phone,
                   opened_date, location_type, store_sqft, num_aisles, is_active
            FROM hr.locations ORDER BY location_type, name
        """, None, industry)
    return query("""
        SELECT location_id, name, address, city, state, zip, phone,
               opened_date, type, is_active
        FROM hr.locations ORDER BY name
    """, None, industry)


@app.get("/{industry}/hr/employees", tags=["HR"])
def hr_employees(
    industry: str,
    location_id: Optional[str] = None,
    department: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(500, le=2000),
    offset: int = 0,
):
    pool_for(industry)
    filters, params = ["TRUE"], []
    if location_id:
        filters.append("location_id = %s::uuid")
        params.append(location_id)
    if department:
        filters.append("department = %s")
        params.append(department)
    if status:
        filters.append("status = %s")
        params.append(status)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM hr.employees WHERE {where}", params, industry)[0]["n"]
    rows = query(f"""
        SELECT employee_id, location_id, first_name, last_name, email,
               hire_date, termination_date, department, job_title, hourly_rate, status
        FROM hr.employees WHERE {where}
        ORDER BY last_name, first_name LIMIT %s OFFSET %s
    """, params + [limit, offset], industry)
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Shared: POS
# ---------------------------------------------------------------------------

@app.get("/{industry}/pos/transactions", tags=["POS"])
def pos_transactions(
    industry: str,
    start_dt: datetime = Query(...),
    end_dt: datetime = Query(...),
    location_id: Optional[str] = None,
    scenario: Optional[str] = None,
    limit: int = Query(1000, le=5000),
    offset: int = 0,
):
    pool_for(industry)
    filters = ["t.transaction_dt BETWEEN %s AND %s"]
    params: list = [start_dt, end_dt]
    if location_id:
        filters.append("t.location_id = %s::uuid")
        params.append(location_id)
    if scenario:
        filters.append("t.scenario_tag = %s")
        params.append(scenario)
    where = " AND ".join(filters)

    total = query(f"SELECT COUNT(*) AS n FROM pos.transactions t WHERE {where}", params, industry)[0]["n"]

    if industry == "grocery":
        select = "t.transaction_id, t.location_id, t.employee_id, t.member_id, t.transaction_dt, t.subtotal, t.coupon_savings, t.deal_savings, t.tax, t.total, t.payment_method, t.scenario_tag"
    else:
        select = "t.transaction_id, t.location_id, t.employee_id, t.member_id, t.transaction_dt, t.subtotal, t.tax, t.total, t.payment_method, t.scenario_tag"

    rows = query(f"""
        SELECT {select}
        FROM pos.transactions t WHERE {where}
        ORDER BY t.transaction_dt DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], industry)
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/{industry}/pos/transactions/summary", tags=["POS"])
def pos_transactions_summary(
    industry: str,
    start_dt: datetime = Query(...),
    end_dt: datetime = Query(...),
    group_by: str = Query("hour", pattern="^(hour|day|location)$"),
):
    pool_for(industry)
    if group_by == "hour":
        group_expr, label = "DATE_TRUNC('hour', transaction_dt)", "hour"
    elif group_by == "day":
        group_expr, label = "DATE_TRUNC('day', transaction_dt)", "day"
    else:
        group_expr, label = "location_id::text", "location_id"
    return query(f"""
        SELECT {group_expr} AS {label},
               COUNT(*) AS transaction_count,
               SUM(total) AS total_revenue,
               AVG(total) AS avg_transaction
        FROM pos.transactions
        WHERE transaction_dt BETWEEN %s AND %s
        GROUP BY {group_expr} ORDER BY 1
    """, [start_dt, end_dt], industry)


@app.get("/{industry}/pos/transaction-items", tags=["POS"])
def pos_transaction_items(
    industry: str,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    location_id: Optional[str] = None,
    transaction_id: Optional[str] = None,
    product_id: Optional[str] = None,
    limit: int = Query(1000, le=5000),
    offset: int = 0,
):
    pool_for(industry)
    filters, params = ["TRUE"], []
    if transaction_id:
        filters.append("ti.transaction_id = %s::uuid")
        params.append(transaction_id)
    if start_dt:
        filters.append("t.transaction_dt >= %s")
        params.append(start_dt)
    if end_dt:
        filters.append("t.transaction_dt <= %s")
        params.append(end_dt)
    if location_id:
        filters.append("t.location_id = %s::uuid")
        params.append(location_id)
    if product_id:
        filters.append("ti.product_id = %s::uuid")
        params.append(product_id)
    where = " AND ".join(filters)
    total = query(f"""
        SELECT COUNT(*) AS n FROM pos.transaction_items ti
        JOIN pos.transactions t ON t.transaction_id = ti.transaction_id WHERE {where}
    """, params, industry)[0]["n"]
    rows = query(f"""
        SELECT ti.item_id, ti.transaction_id, ti.product_id,
               p.name AS product_name, p.category,
               ti.quantity, ti.unit_price, ti.discount, ti.line_total,
               t.transaction_dt, t.location_id
        FROM pos.transaction_items ti
        JOIN pos.transactions t ON t.transaction_id = ti.transaction_id
        JOIN pos.products p ON p.product_id = ti.product_id
        WHERE {where}
        ORDER BY t.transaction_dt DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], industry)
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/{industry}/pos/products", tags=["POS"])
def pos_products(
    industry: str,
    category: Optional[str] = None,
    department: Optional[str] = None,
    is_active: Optional[bool] = None,
    limit: int = Query(500, le=2000),
    offset: int = 0,
):
    pool_for(industry)
    filters, params = ["TRUE"], []
    if category:
        filters.append("p.category = %s")
        params.append(category)
    if is_active is not None:
        filters.append("p.is_active = %s")
        params.append(is_active)
    where = " AND ".join(filters)

    if industry == "grocery":
        if department:
            filters.append("d.name = %s")
            params.append(department)
            where = " AND ".join(filters)
        total = query(f"""
            SELECT COUNT(*) AS n FROM pos.products p
            JOIN pos.departments d ON d.department_id = p.department_id
            WHERE {where}
        """, params, industry)[0]["n"]
        rows = query(f"""
            SELECT p.product_id, p.sku, p.upc, p.name, p.brand,
                   d.name AS department, p.category, p.subcategory,
                   p.unit_size, p.unit_of_measure, p.cost, p.current_price,
                   p.is_organic, p.is_local, p.is_active
            FROM pos.products p
            JOIN pos.departments d ON d.department_id = p.department_id
            WHERE {where}
            ORDER BY d.name, p.category, p.name LIMIT %s OFFSET %s
        """, params + [limit, offset], industry)
        return {"data": rows, "total": total, "limit": limit, "offset": offset}

    total = query(f"SELECT COUNT(*) AS n FROM pos.products p WHERE {where}", params, industry)[0]["n"]
    rows = query(f"""
        SELECT product_id, sku, name, category, subcategory, cost, current_price, is_active
        FROM pos.products p WHERE {where}
        ORDER BY category, name LIMIT %s OFFSET %s
    """, params + [limit, offset], industry)
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/{industry}/pos/loyalty-members", tags=["POS"])
def loyalty_members(
    industry: str,
    tier: Optional[str] = None,
    limit: int = Query(500, le=5000),
    offset: int = 0,
):
    pool_for(industry)
    filters, params = ["TRUE"], []
    if tier:
        filters.append("tier = %s")
        params.append(tier)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM pos.loyalty_members WHERE {where}", params, industry)[0]["n"]
    rows = query(f"""
        SELECT member_id, first_name, last_name, email, signup_date, points_balance, tier
        FROM pos.loyalty_members WHERE {where}
        ORDER BY points_balance DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], industry)
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/{industry}/pos/price-history", tags=["POS"])
def pos_price_history(
    industry: str,
    product_id: Optional[str] = None,
    limit: int = Query(200, le=1000),
    offset: int = 0,
):
    pool_for(industry)
    filters, params = ["TRUE"], []
    if product_id:
        filters.append("ph.product_id = %s::uuid")
        params.append(product_id)
    where = " AND ".join(filters)
    total = query(f"""
        SELECT COUNT(*) AS n FROM pos.price_history ph WHERE {where}
    """, params, industry)[0]["n"]
    rows = query(f"""
        SELECT ph.price_history_id, p.name AS product_name, p.category,
               ph.old_price, ph.new_price, ph.changed_at
        FROM pos.price_history ph
        JOIN pos.products p ON p.product_id = ph.product_id
        WHERE {where}
        ORDER BY ph.changed_at DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], industry)
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Gas-station only: Fuel
# ---------------------------------------------------------------------------

@app.get("/gas-station/fuel/transactions", tags=["Fuel"])
def fuel_transactions(
    start_dt: datetime = Query(...),
    end_dt: datetime = Query(...),
    location_id: Optional[str] = None,
    grade_id: Optional[str] = None,
    limit: int = Query(1000, le=5000),
    offset: int = 0,
):
    filters = ["t.transaction_dt BETWEEN %s AND %s"]
    params: list = [start_dt, end_dt]
    if location_id:
        filters.append("t.location_id = %s::uuid")
        params.append(location_id)
    if grade_id:
        filters.append("t.grade_id = %s::uuid")
        params.append(grade_id)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM fuel.transactions t WHERE {where}", params, "gas-station")[0]["n"]
    rows = query(f"""
        SELECT t.transaction_id, t.pump_id, t.location_id, t.grade_id,
               t.transaction_dt, t.gallons, t.price_per_gallon, t.total_amount,
               t.payment_method, t.scenario_tag, g.name AS grade_name
        FROM fuel.transactions t
        JOIN fuel.grades g ON g.grade_id = t.grade_id
        WHERE {where}
        ORDER BY t.transaction_dt DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], "gas-station")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/gas-station/fuel/transactions/summary", tags=["Fuel"])
def fuel_transactions_summary(
    start_dt: datetime = Query(...),
    end_dt: datetime = Query(...),
    group_by: str = Query("day", pattern="^(hour|day|location|grade)$"),
):
    if group_by == "hour":
        group_expr, label = "DATE_TRUNC('hour', t.transaction_dt)", "hour"
    elif group_by == "day":
        group_expr, label = "DATE_TRUNC('day', t.transaction_dt)", "day"
    elif group_by == "location":
        group_expr, label = "t.location_id::text", "location_id"
    else:
        group_expr, label = "g.name", "grade"
    return query(f"""
        SELECT {group_expr} AS {label},
               COUNT(*) AS transaction_count,
               SUM(t.gallons) AS total_gallons,
               SUM(t.total_amount) AS total_revenue,
               AVG(t.price_per_gallon) AS avg_price_per_gallon
        FROM fuel.transactions t
        JOIN fuel.grades g ON g.grade_id = t.grade_id
        WHERE t.transaction_dt BETWEEN %s AND %s
        GROUP BY {group_expr} ORDER BY 1
    """, [start_dt, end_dt], "gas-station")


@app.get("/gas-station/fuel/grades", tags=["Fuel"])
def fuel_grades():
    return query("SELECT grade_id, name, octane_rating, current_price, is_active, updated_at FROM fuel.grades ORDER BY current_price",
                 None, "gas-station")


@app.get("/gas-station/fuel/price-history", tags=["Fuel"])
def fuel_price_history(
    grade_id: Optional[str] = None,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    limit: int = Query(200, le=1000),
):
    filters, params = ["TRUE"], []
    if grade_id:
        filters.append("ph.grade_id = %s::uuid")
        params.append(grade_id)
    if start_dt:
        filters.append("ph.changed_at >= %s")
        params.append(start_dt)
    if end_dt:
        filters.append("ph.changed_at <= %s")
        params.append(end_dt)
    where = " AND ".join(filters)
    return query(f"""
        SELECT ph.price_history_id, g.name AS grade_name, ph.old_price, ph.new_price, ph.changed_at
        FROM fuel.price_history ph
        JOIN fuel.grades g ON g.grade_id = ph.grade_id
        WHERE {where}
        ORDER BY ph.changed_at DESC LIMIT %s
    """, params + [limit], "gas-station")


@app.get("/gas-station/fuel/pumps", tags=["Fuel"])
def fuel_pumps(location_id: Optional[str] = None):
    filters, params = ["TRUE"], []
    if location_id:
        filters.append("p.location_id = %s::uuid")
        params.append(location_id)
    where = " AND ".join(filters)
    return query(f"""
        SELECT p.pump_id, p.location_id, l.name AS location_name,
               p.pump_number, p.num_sides, p.is_active, p.created_at
        FROM fuel.pumps p
        JOIN hr.locations l ON l.location_id = p.location_id
        WHERE {where}
        ORDER BY l.name, p.pump_number
    """, params, "gas-station")


# ---------------------------------------------------------------------------
# Grocery only: Departments, Coupons, Combo Deals
# ---------------------------------------------------------------------------

@app.get("/grocery/pos/departments", tags=["Grocery — POS"])
def pos_departments():
    return query("SELECT department_id, name, code, is_active FROM pos.departments ORDER BY name",
                 None, "grocery")


@app.get("/grocery/pos/coupons", tags=["Grocery — POS"])
def pos_coupons(active_only: bool = True, limit: int = Query(200, le=1000)):
    filters, params = ["TRUE"], []
    if active_only:
        filters.append("c.is_active = TRUE AND c.valid_until >= CURRENT_DATE")
    where = " AND ".join(filters)
    return query(f"""
        SELECT c.coupon_id, c.code, c.description, c.coupon_type, c.discount_value,
               c.min_purchase, d.name AS department, c.uses_count, c.max_uses,
               c.valid_from, c.valid_until, c.is_active
        FROM pos.coupons c
        LEFT JOIN pos.departments d ON d.department_id = c.department_id
        WHERE {where}
        ORDER BY c.valid_until DESC LIMIT %s
    """, params + [limit], "grocery")


@app.get("/grocery/pos/combo-deals", tags=["Grocery — POS"])
def pos_combo_deals(active_only: bool = True):
    filters, params = ["TRUE"], []
    if active_only:
        filters.append("cd.is_active = TRUE AND cd.valid_until >= CURRENT_DATE")
    where = " AND ".join(filters)
    return query(f"""
        SELECT cd.deal_id, cd.name, cd.description, cd.deal_type, cd.trigger_qty,
               d.name AS trigger_department, cd.deal_price, cd.valid_from, cd.valid_until
        FROM pos.combo_deals cd
        LEFT JOIN pos.departments d ON d.department_id = cd.trigger_department_id
        WHERE {where}
        ORDER BY cd.valid_until DESC
    """, params, "grocery")


# ---------------------------------------------------------------------------
# Grocery only: Timeclock
# ---------------------------------------------------------------------------

@app.get("/grocery/timeclock/events", tags=["Grocery — Timeclock"])
def timeclock_events(
    start_dt: datetime = Query(...),
    end_dt: datetime = Query(...),
    location_id: Optional[str] = None,
    employee_id: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: int = Query(1000, le=5000),
    offset: int = 0,
):
    filters = ["e.event_dt BETWEEN %s AND %s"]
    params: list = [start_dt, end_dt]
    if location_id:
        filters.append("e.location_id = %s::uuid")
        params.append(location_id)
    if employee_id:
        filters.append("e.employee_id = %s::uuid")
        params.append(employee_id)
    if event_type:
        filters.append("e.event_type = %s")
        params.append(event_type)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM timeclock.events e WHERE {where}", params, "grocery")[0]["n"]
    rows = query(f"""
        SELECT e.event_id, e.employee_id,
               emp.first_name || ' ' || emp.last_name AS employee_name,
               e.location_id, l.name AS location_name,
               e.event_type, e.event_dt, e.notes
        FROM timeclock.events e
        JOIN hr.employees emp ON emp.employee_id = e.employee_id
        JOIN hr.locations l ON l.location_id = e.location_id
        WHERE {where}
        ORDER BY e.event_dt DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/grocery/timeclock/summary", tags=["Grocery — Timeclock"])
def timeclock_summary(
    start_dt: datetime = Query(...),
    end_dt: datetime = Query(...),
):
    return query("""
        SELECT DATE_TRUNC('day', event_dt) AS day,
               event_type, COUNT(*) AS event_count
        FROM timeclock.events
        WHERE event_dt BETWEEN %s AND %s
        GROUP BY 1, 2 ORDER BY 1, 2
    """, [start_dt, end_dt], "grocery")


# ---------------------------------------------------------------------------
# Grocery only: Ordering
# ---------------------------------------------------------------------------

@app.get("/grocery/ordering/orders", tags=["Grocery — Ordering"])
def ordering_orders(
    status: Optional[str] = None,
    store_location_id: Optional[str] = None,
    limit: int = Query(500, le=2000),
    offset: int = 0,
):
    filters, params = ["TRUE"], []
    if status:
        filters.append("so.status = %s")
        params.append(status)
    if store_location_id:
        filters.append("so.store_location_id = %s::uuid")
        params.append(store_location_id)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM ordering.store_orders so WHERE {where}", params, "grocery")[0]["n"]
    rows = query(f"""
        SELECT so.order_id, so.store_location_id, so.warehouse_location_id,
               so.created_by, so.order_dt, so.requested_delivery_dt,
               so.approved_by, so.approved_dt, so.status, so.notes,
               so.created_at, so.updated_at,
               sl.name AS store_name, wl.name AS warehouse_name,
               COUNT(soi.item_id) AS line_items
        FROM ordering.store_orders so
        JOIN hr.locations sl ON sl.location_id = so.store_location_id
        JOIN hr.locations wl ON wl.location_id = so.warehouse_location_id
        LEFT JOIN ordering.store_order_items soi ON soi.order_id = so.order_id
        WHERE {where}
        GROUP BY so.order_id, sl.name, wl.name
        ORDER BY so.order_dt DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/grocery/ordering/summary", tags=["Grocery — Ordering"])
def ordering_summary():
    return query("""
        SELECT status, COUNT(*) AS order_count
        FROM ordering.store_orders
        GROUP BY status ORDER BY order_count DESC
    """, None, "grocery")


# ---------------------------------------------------------------------------
# Grocery only: Fulfillment
# ---------------------------------------------------------------------------

@app.get("/grocery/fulfillment/orders", tags=["Grocery — Fulfillment"])
def fulfillment_orders(
    status: Optional[str] = None,
    limit: int = Query(500, le=2000),
    offset: int = 0,
):
    filters, params = ["TRUE"], []
    if status:
        filters.append("fo.status = %s")
        params.append(status)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM fulfillment.orders fo WHERE {where}", params, "grocery")[0]["n"]
    rows = query(f"""
        SELECT fo.fulfillment_id, fo.store_order_id, fo.warehouse_location_id,
               fo.assigned_to, fo.status, fo.started_at, fo.completed_at, fo.created_at,
               wl.name AS warehouse_name,
               emp.first_name || ' ' || emp.last_name AS assigned_to_name,
               COUNT(fi.item_id) AS lines_picked
        FROM fulfillment.orders fo
        JOIN hr.locations wl ON wl.location_id = fo.warehouse_location_id
        LEFT JOIN hr.employees emp ON emp.employee_id = fo.assigned_to
        LEFT JOIN fulfillment.items fi ON fi.fulfillment_id = fo.fulfillment_id
        WHERE {where}
        GROUP BY fo.fulfillment_id, wl.name, emp.first_name, emp.last_name
        ORDER BY fo.created_at DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Grocery only: Transport
# ---------------------------------------------------------------------------

@app.get("/grocery/transport/trucks", tags=["Grocery — Transport"])
def transport_trucks():
    return query("""
        SELECT truck_id, license_plate, make, model, year, capacity_pallets, is_active
        FROM transport.trucks ORDER BY license_plate
    """, None, "grocery")


@app.get("/grocery/transport/loads", tags=["Grocery — Transport"])
def transport_loads(
    status: Optional[str] = None,
    limit: int = Query(500, le=2000),
    offset: int = 0,
):
    filters, params = ["TRUE"], []
    if status:
        filters.append("l.status = %s")
        params.append(status)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM transport.loads l WHERE {where}", params, "grocery")[0]["n"]
    rows = query(f"""
        SELECT l.load_id, l.truck_id, l.driver_id,
               l.warehouse_location_id, l.destination_location_id,
               l.status, l.departed_at, l.arrived_at, l.created_at,
               t.license_plate,
               emp.first_name || ' ' || emp.last_name AS driver,
               wl.name AS from_warehouse, dl.name AS to_store
        FROM transport.loads l
        JOIN transport.trucks t ON t.truck_id = l.truck_id
        LEFT JOIN hr.employees emp ON emp.employee_id = l.driver_id
        JOIN hr.locations wl ON wl.location_id = l.warehouse_location_id
        JOIN hr.locations dl ON dl.location_id = l.destination_location_id
        WHERE {where}
        ORDER BY l.created_at DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Shared: Inventory
# ---------------------------------------------------------------------------

@app.get("/{industry}/inventory/stock-levels", tags=["Inventory"])
def inventory_stock_levels(
    industry: str,
    location_id: Optional[str] = None,
    below_reorder_point: bool = False,
    limit: int = Query(500, le=5000),
    offset: int = 0,
):
    pool_for(industry)
    filters, params = ["TRUE"], []
    if location_id:
        filters.append("sl.location_id = %s::uuid")
        params.append(location_id)
    if below_reorder_point:
        filters.append("sl.quantity_on_hand < ip.reorder_point")
    where = " AND ".join(filters)
    total = query(f"""
        SELECT COUNT(*) AS n FROM inv.stock_levels sl
        JOIN inv.products ip ON ip.product_id = sl.product_id
        WHERE {where}
    """, params, industry)[0]["n"]
    rows = query(f"""
        SELECT sl.stock_id, sl.product_id, sl.location_id, p.name AS product_name,
               p.category, sl.quantity_on_hand, sl.quantity_reserved,
               ip.reorder_point, ip.reorder_qty, sl.last_updated
        FROM inv.stock_levels sl
        JOIN pos.products p ON p.product_id = sl.product_id
        JOIN inv.products ip ON ip.product_id = sl.product_id
        WHERE {where}
        ORDER BY sl.quantity_on_hand ASC LIMIT %s OFFSET %s
    """, params + [limit, offset], industry)
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/{industry}/inventory/receipts", tags=["Inventory"])
def inventory_receipts(
    industry: str,
    location_id: Optional[str] = None,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    limit: int = Query(200, le=1000),
):
    pool_for(industry)
    filters, params = ["TRUE"], []
    if location_id:
        filters.append("r.location_id = %s::uuid")
        params.append(location_id)
    if start_dt:
        filters.append("r.received_dt >= %s")
        params.append(start_dt)
    if end_dt:
        filters.append("r.received_dt <= %s")
        params.append(end_dt)
    where = " AND ".join(filters)
    return query(f"""
        SELECT r.receipt_id, r.location_id, r.received_dt, r.supplier_name,
               r.po_number, r.total_cost,
               COUNT(ri.receipt_item_id) AS line_items
        FROM inv.receipts r
        LEFT JOIN inv.receipt_items ri ON ri.receipt_id = r.receipt_id
        WHERE {where}
        GROUP BY r.receipt_id
        ORDER BY r.received_dt DESC LIMIT %s
    """, params + [limit], industry)


@app.get("/{industry}/inventory/products", tags=["Inventory"])
def inventory_products(industry: str, limit: int = Query(500, le=2000), offset: int = 0):
    pool_for(industry)
    total = query("SELECT COUNT(*) AS n FROM inv.products ip", [], industry)[0]["n"]
    rows = query("""
        SELECT ip.inv_product_id, ip.product_id, p.name AS product_name,
               p.category, p.sku, ip.reorder_point, ip.reorder_qty,
               ip.unit_of_measure, ip.supplier_name, ip.lead_time_days
        FROM inv.products ip
        JOIN pos.products p ON p.product_id = ip.product_id
        ORDER BY p.category, p.name LIMIT %s OFFSET %s
    """, [limit, offset], industry)
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/{industry}/inventory/receipt-items", tags=["Inventory"])
def inventory_receipt_items(
    industry: str,
    receipt_id: Optional[str] = None,
    location_id: Optional[str] = None,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    limit: int = Query(500, le=2000),
    offset: int = 0,
):
    pool_for(industry)
    filters, params = ["TRUE"], []
    if receipt_id:
        filters.append("ri.receipt_id = %s::uuid")
        params.append(receipt_id)
    if location_id:
        filters.append("r.location_id = %s::uuid")
        params.append(location_id)
    if start_dt:
        filters.append("r.received_dt >= %s")
        params.append(start_dt)
    if end_dt:
        filters.append("r.received_dt <= %s")
        params.append(end_dt)
    where = " AND ".join(filters)
    total = query(f"""
        SELECT COUNT(*) AS n FROM inv.receipt_items ri
        JOIN inv.receipts r ON r.receipt_id = ri.receipt_id WHERE {where}
    """, params, industry)[0]["n"]
    rows = query(f"""
        SELECT ri.receipt_item_id, ri.receipt_id, ri.product_id,
               p.name AS product_name, p.category,
               ri.quantity, ri.unit_cost, ri.line_total,
               r.received_dt, r.supplier_name, r.location_id
        FROM inv.receipt_items ri
        JOIN inv.receipts r ON r.receipt_id = ri.receipt_id
        JOIN pos.products p ON p.product_id = ri.product_id
        WHERE {where}
        ORDER BY r.received_dt DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], industry)
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Shared: Stats
# ---------------------------------------------------------------------------

@app.get("/{industry}/stats/generation", tags=["Stats"])
def stats_generation(industry: str, last_n_ticks: int = Query(100, le=1000)):
    pool_for(industry)
    if industry == "grocery":
        return query("""
            SELECT stat_id, recorded_at, pos_transactions_generated,
                   timeclock_events_generated, orders_generated,
                   scenario_tag, simulation_dt, wall_clock_ms
            FROM control.generation_stats
            ORDER BY recorded_at DESC LIMIT %s
        """, [last_n_ticks], industry)
    return query("""
        SELECT stat_id, recorded_at, pos_transactions_generated,
               fuel_transactions_generated, inventory_receipts_generated,
               scenario_tag, simulation_dt, wall_clock_ms
        FROM control.generation_stats
        ORDER BY recorded_at DESC LIMIT %s
    """, [last_n_ticks], industry)


@app.get("/{industry}/stats/today", tags=["Stats"])
def stats_today(industry: str):
    pool_for(industry)
    if industry == "grocery":
        return query("""
            SELECT
                COALESCE(SUM(pos_transactions_generated), 0)  AS pos_transactions,
                COALESCE(SUM(timeclock_events_generated), 0)  AS timeclock_events,
                COALESCE(SUM(orders_generated), 0)            AS orders,
                COUNT(*) AS ticks,
                MIN(recorded_at) AS first_tick,
                MAX(recorded_at) AS last_tick
            FROM control.generation_stats
            WHERE recorded_at >= CURRENT_DATE
        """, None, industry)[0]
    return query("""
        SELECT
            COALESCE(SUM(pos_transactions_generated), 0)   AS pos_transactions,
            COALESCE(SUM(fuel_transactions_generated), 0)  AS fuel_transactions,
            COALESCE(SUM(inventory_receipts_generated), 0) AS inventory_receipts,
            COUNT(*) AS ticks,
            MIN(recorded_at) AS first_tick,
            MAX(recorded_at) AS last_tick
        FROM control.generation_stats
        WHERE recorded_at >= CURRENT_DATE
    """, None, industry)[0]


# ---------------------------------------------------------------------------
# Grocery only: HR Schedules
# ---------------------------------------------------------------------------

@app.get("/grocery/hr/schedules", tags=["Grocery — HR"])
def hr_schedules(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    location_id: Optional[str] = None,
    employee_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(2000, le=10000),
    offset: int = 0,
):
    filters, params = ["TRUE"], []
    if start_date:
        filters.append("scheduled_date >= %s")
        params.append(start_date)
    if end_date:
        filters.append("scheduled_date <= %s")
        params.append(end_date)
    if location_id:
        filters.append("location_id = %s::uuid")
        params.append(location_id)
    if employee_id:
        filters.append("employee_id = %s::uuid")
        params.append(employee_id)
    if status:
        filters.append("status = %s")
        params.append(status)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM hr.schedules WHERE {where}", params, "grocery")[0]["n"]
    rows = query(f"""
        SELECT schedule_id, location_id, employee_id, scheduled_date,
               department, shift_start, shift_end, status, created_at
        FROM hr.schedules WHERE {where}
        ORDER BY scheduled_date DESC, location_id LIMIT %s OFFSET %s
    """, params + [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Grocery only: Loyalty Point Transactions
# ---------------------------------------------------------------------------

@app.get("/grocery/pos/loyalty-point-transactions", tags=["Grocery — POS"])
def loyalty_point_transactions(
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    member_id: Optional[str] = None,
    limit: int = Query(2000, le=10000),
    offset: int = 0,
):
    filters, params = ["TRUE"], []
    if start_dt:
        filters.append("created_at >= %s")
        params.append(start_dt)
    if end_dt:
        filters.append("created_at <= %s")
        params.append(end_dt)
    if member_id:
        filters.append("member_id = %s::uuid")
        params.append(member_id)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM pos.loyalty_point_transactions WHERE {where}", params, "grocery")[0]["n"]
    rows = query(f"""
        SELECT pt_id, member_id, transaction_id, points_earned, points_redeemed,
               reason, balance_after, created_at
        FROM pos.loyalty_point_transactions WHERE {where}
        ORDER BY created_at DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Grocery only: Shrinkage Events
# ---------------------------------------------------------------------------

@app.get("/grocery/inventory/shrinkage-events", tags=["Grocery — Inventory"])
def inventory_shrinkage_events(
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
    location_id: Optional[str] = None,
    reason: Optional[str] = None,
    limit: int = Query(2000, le=10000),
    offset: int = 0,
):
    filters, params = ["TRUE"], []
    if start_dt:
        filters.append("recorded_at >= %s")
        params.append(start_dt)
    if end_dt:
        filters.append("recorded_at <= %s")
        params.append(end_dt)
    if location_id:
        filters.append("location_id = %s::uuid")
        params.append(location_id)
    if reason:
        filters.append("reason = %s")
        params.append(reason)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM inv.shrinkage_events WHERE {where}", params, "grocery")[0]["n"]
    rows = query(f"""
        SELECT shrinkage_id, product_id, location_id, quantity, reason,
               estimated_cost, recorded_at, recorded_by
        FROM inv.shrinkage_events WHERE {where}
        ORDER BY recorded_at DESC LIMIT %s OFFSET %s
    """, params + [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Grocery only: Pricing
# ---------------------------------------------------------------------------

@app.get("/grocery/pricing/weekly-ads", tags=["Grocery — Pricing"])
def pricing_weekly_ads(limit: int = Query(500, le=2000), offset: int = 0):
    total = query("SELECT COUNT(*) AS n FROM pricing.weekly_ads", None, "grocery")[0]["n"]
    rows = query("""
        SELECT ad_id, ad_name, start_date, end_date, created_at
        FROM pricing.weekly_ads
        ORDER BY start_date DESC LIMIT %s OFFSET %s
    """, [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


@app.get("/grocery/pricing/ad-items", tags=["Grocery — Pricing"])
def pricing_ad_items(
    ad_id: Optional[str] = None,
    limit: int = Query(2000, le=10000),
    offset: int = 0,
):
    filters, params = ["TRUE"], []
    if ad_id:
        filters.append("ad_id = %s::uuid")
        params.append(ad_id)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM pricing.ad_items WHERE {where}", params, "grocery")[0]["n"]
    rows = query(f"""
        SELECT ad_item_id, ad_id, product_id, promoted_price, discount_pct, created_at
        FROM pricing.ad_items WHERE {where}
        ORDER BY ad_id, product_id LIMIT %s OFFSET %s
    """, params + [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Grocery only: Ordering items
# ---------------------------------------------------------------------------

@app.get("/grocery/ordering/order-items", tags=["Grocery — Ordering"])
def ordering_order_items(
    order_id: Optional[str] = None,
    limit: int = Query(2000, le=10000),
    offset: int = 0,
):
    filters, params = ["TRUE"], []
    if order_id:
        filters.append("order_id = %s::uuid")
        params.append(order_id)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM ordering.store_order_items WHERE {where}", params, "grocery")[0]["n"]
    rows = query(f"""
        SELECT item_id, order_id, product_id, quantity_requested, quantity_approved, notes
        FROM ordering.store_order_items WHERE {where}
        ORDER BY order_id LIMIT %s OFFSET %s
    """, params + [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Grocery only: Fulfillment items
# ---------------------------------------------------------------------------

@app.get("/grocery/fulfillment/items", tags=["Grocery — Fulfillment"])
def fulfillment_items(
    fulfillment_id: Optional[str] = None,
    limit: int = Query(2000, le=10000),
    offset: int = 0,
):
    filters, params = ["TRUE"], []
    if fulfillment_id:
        filters.append("fulfillment_id = %s::uuid")
        params.append(fulfillment_id)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM fulfillment.items WHERE {where}", params, "grocery")[0]["n"]
    rows = query(f"""
        SELECT item_id, fulfillment_id, product_id, quantity_requested,
               quantity_picked, pick_status
        FROM fulfillment.items WHERE {where}
        ORDER BY fulfillment_id LIMIT %s OFFSET %s
    """, params + [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Grocery only: Transport load items
# ---------------------------------------------------------------------------

@app.get("/grocery/transport/load-items", tags=["Grocery — Transport"])
def transport_load_items(
    load_id: Optional[str] = None,
    limit: int = Query(2000, le=10000),
    offset: int = 0,
):
    filters, params = ["TRUE"], []
    if load_id:
        filters.append("load_id = %s::uuid")
        params.append(load_id)
    where = " AND ".join(filters)
    total = query(f"SELECT COUNT(*) AS n FROM transport.load_items WHERE {where}", params, "grocery")[0]["n"]
    rows = query(f"""
        SELECT item_id, load_id, fulfillment_id, store_order_id
        FROM transport.load_items WHERE {where}
        ORDER BY load_id LIMIT %s OFFSET %s
    """, params + [limit, offset], "grocery")
    return {"data": rows, "total": total, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@app.get("/{industry}/stats/backfill-progress", tags=["Stats"])
def stats_backfill_progress(industry: str):
    pool_for(industry)
    state = query("""
        SELECT mode, backfill_start_date, backfill_end_date, backfill_current_date
        FROM control.generator_state WHERE state_id = 1
    """, None, industry)[0]
    if state["mode"] != "backfill" or not state["backfill_start_date"]:
        return {"in_progress": False}
    start = state["backfill_start_date"]
    end = state["backfill_end_date"]
    current = state["backfill_current_date"] or start
    total_days = max(1, (end - start).days)
    done_days = (current - start).days
    pct = round(done_days / total_days * 100, 1)
    return {
        "in_progress": True,
        "start": str(start),
        "end": str(end),
        "current": str(current),
        "pct_complete": pct,
        "days_remaining": total_days - done_days,
    }


@app.get("/{industry}/stats/recent", tags=["Stats"])
def stats_recent(industry: str, minutes: int = Query(60, ge=1, le=1440)):
    pool_for(industry)
    rows = query("""
        SELECT
            COALESCE(SUM(pos_transactions_generated), 0)  AS pos_transactions,
            COALESCE(SUM(timeclock_events_generated), 0)  AS timeclock_events,
            COALESCE(SUM(orders_generated), 0)            AS orders,
            COUNT(*)                                      AS ticks,
            MIN(recorded_at)                              AS first_tick_at,
            MAX(recorded_at)                              AS last_tick_at,
            ARRAY_AGG(DISTINCT scenario_tag) FILTER (WHERE scenario_tag IS NOT NULL) AS scenario_tags
        FROM control.generation_stats
        WHERE recorded_at >= NOW() - (%s * INTERVAL '1 minute')
    """, [minutes], industry)
    result = rows[0]
    result["window_minutes"] = minutes

    if industry == "grocery":
        txn_rows = query("""
            SELECT t.transaction_id, l.name AS store, t.transaction_dt,
                   t.total, t.payment_method, t.scenario_tag
            FROM pos.transactions t
            JOIN hr.locations l ON l.location_id = t.location_id
            WHERE t.transaction_dt >= NOW() - (%s * INTERVAL '1 minute')
            ORDER BY t.transaction_dt DESC LIMIT 20
        """, [minutes], industry)
        result["recent_transactions"] = txn_rows
    return result


# ---------------------------------------------------------------------------
# Stats — Data distributions
# ---------------------------------------------------------------------------

@app.get("/{industry}/stats/distributions", tags=["Stats"])
def stats_distributions(industry: str, days: int = Query(30, ge=1, le=365)):
    """
    Returns record-count distributions grouped by day and key classification
    columns for the given look-back window (default 30 days).
    """
    pool_for(industry)

    result: Dict[str, Any] = {}

    # Transactions per day
    result["transactions_by_day"] = query("""
        SELECT DATE_TRUNC('day', transaction_dt)::date AS day,
               COUNT(*)                               AS transaction_count,
               ROUND(SUM(total)::numeric, 2)          AS total_revenue
        FROM pos.transactions
        WHERE transaction_dt >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
        GROUP BY 1 ORDER BY 1
    """, [days], industry)

    # Transactions by store
    result["transactions_by_store"] = query("""
        SELECT l.name AS store_name,
               COUNT(*) AS transaction_count,
               ROUND(SUM(t.total)::numeric, 2) AS total_revenue
        FROM pos.transactions t
        JOIN hr.locations l ON l.location_id = t.location_id
        WHERE t.transaction_dt >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
        GROUP BY l.name ORDER BY 2 DESC
    """, [days], industry)

    # Transactions by scenario tag
    result["transactions_by_scenario"] = query("""
        SELECT COALESCE(scenario_tag, 'normal') AS scenario_tag,
               COUNT(*) AS transaction_count
        FROM pos.transactions
        WHERE transaction_dt >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
        GROUP BY 1 ORDER BY 2 DESC
    """, [days], industry)

    # Transactions by payment method
    result["transactions_by_payment"] = query("""
        SELECT payment_method, COUNT(*) AS transaction_count
        FROM pos.transactions
        WHERE transaction_dt >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
        GROUP BY 1 ORDER BY 2 DESC
    """, [days], industry)

    # Employees by department (current active)
    result["employees_by_department"] = query("""
        SELECT department, COUNT(*) AS employee_count
        FROM hr.employees
        WHERE status = 'active'
        GROUP BY 1 ORDER BY 2 DESC
    """, None, industry)

    if industry == "grocery":
        # Timeclock events per day
        result["timeclock_by_day"] = query("""
            SELECT DATE_TRUNC('day', event_dt)::date AS day,
                   COUNT(*) AS event_count
            FROM timeclock.events
            WHERE event_dt >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
            GROUP BY 1 ORDER BY 1
        """, [days], industry)

        # Timeclock by event type
        result["timeclock_by_type"] = query("""
            SELECT event_type, COUNT(*) AS event_count
            FROM timeclock.events
            WHERE event_dt >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
            GROUP BY 1 ORDER BY 2 DESC
        """, [days], industry)

        # Store orders per day
        result["orders_by_day"] = query("""
            SELECT DATE_TRUNC('day', order_dt)::date AS day,
                   COUNT(*) AS order_count
            FROM ordering.store_orders
            WHERE order_dt >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
            GROUP BY 1 ORDER BY 1
        """, [days], industry)

        # Orders by status
        result["orders_by_status"] = query("""
            SELECT status, COUNT(*) AS order_count
            FROM ordering.store_orders
            WHERE order_dt >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
            GROUP BY 1 ORDER BY 2 DESC
        """, [days], industry)

        # Products by department (active)
        result["products_by_department"] = query("""
            SELECT d.name AS department_name, COUNT(*) AS product_count
            FROM pos.products p
            JOIN pos.departments d ON d.department_id = p.department_id
            WHERE p.is_active = TRUE
            GROUP BY 1 ORDER BY 2 DESC
        """, None, industry)

        # Shrinkage events per day
        result["shrinkage_by_day"] = query("""
            SELECT DATE_TRUNC('day', recorded_at)::date AS day,
                   COUNT(*) AS event_count,
                   ROUND(COALESCE(SUM(estimated_cost), 0)::numeric, 2) AS total_cost
            FROM inv.shrinkage_events
            WHERE recorded_at >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
            GROUP BY 1 ORDER BY 1
        """, [days], industry)

        # Shrinkage by reason
        result["shrinkage_by_reason"] = query("""
            SELECT reason, COUNT(*) AS event_count,
                   ROUND(COALESCE(SUM(estimated_cost), 0)::numeric, 2) AS total_cost
            FROM inv.shrinkage_events
            WHERE recorded_at >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
            GROUP BY 1 ORDER BY 2 DESC
        """, [days], industry)

    return result


# ---------------------------------------------------------------------------
# Grocery — Multi-scenario management
# ---------------------------------------------------------------------------

@app.get("/grocery/generator/scenarios", tags=["Grocery — Scenarios"])
def list_active_scenarios():
    return query(
        "SELECT scenario_id, scenario_name, activated_at FROM control.active_scenarios ORDER BY activated_at",
        None, "grocery"
    )


@app.post("/grocery/generator/scenarios", tags=["Grocery — Scenarios"])
def activate_scenario(req: ScenarioActivateRequest):
    valid = VALID_SCENARIOS.get("grocery", set())
    if req.scenario_name not in valid:
        raise HTTPException(400, f"scenario_name must be one of {sorted(valid)}")
    execute("""
        INSERT INTO control.active_scenarios (scenario_name)
        VALUES (%s)
        ON CONFLICT (scenario_name) DO NOTHING
    """, [req.scenario_name], "grocery")
    return query(
        "SELECT scenario_id, scenario_name, activated_at FROM control.active_scenarios ORDER BY activated_at",
        None, "grocery"
    )


@app.delete("/grocery/generator/scenarios/{scenario_name}", tags=["Grocery — Scenarios"])
def deactivate_scenario(scenario_name: str):
    execute("DELETE FROM control.active_scenarios WHERE scenario_name = %s", [scenario_name], "grocery")
    return query(
        "SELECT scenario_id, scenario_name, activated_at FROM control.active_scenarios ORDER BY activated_at",
        None, "grocery"
    )


@app.get("/grocery/generator/scenario-schedules", tags=["Grocery — Scenarios"])
def list_scenario_schedules():
    return query("""
        SELECT schedule_id, scenario_name, start_date, end_date, label, created_at
        FROM control.scenario_schedules
        ORDER BY start_date, scenario_name
    """, None, "grocery")


@app.post("/grocery/generator/scenario-schedules", tags=["Grocery — Scenarios"])
def create_scenario_schedule(req: ScenarioScheduleRequest):
    valid = VALID_SCENARIOS.get("grocery", set())
    if req.scenario_name not in valid:
        raise HTTPException(400, f"scenario_name must be one of {sorted(valid)}")
    if req.end_date < req.start_date:
        raise HTTPException(400, "end_date must be >= start_date")
    execute("""
        INSERT INTO control.scenario_schedules (scenario_name, start_date, end_date, label)
        VALUES (%s, %s, %s, %s)
    """, [req.scenario_name, req.start_date, req.end_date, req.label], "grocery")
    return query("""
        SELECT schedule_id, scenario_name, start_date, end_date, label, created_at
        FROM control.scenario_schedules ORDER BY start_date, scenario_name
    """, None, "grocery")


@app.delete("/grocery/generator/scenario-schedules/{schedule_id}", tags=["Grocery — Scenarios"])
def delete_scenario_schedule(schedule_id: str):
    rows = execute(
        "DELETE FROM control.scenario_schedules WHERE schedule_id = %s::uuid",
        [schedule_id], "grocery"
    )
    if rows == 0:
        raise HTTPException(404, "Schedule not found")
    return {"deleted": schedule_id}


# ---------------------------------------------------------------------------
# Grocery — Coupon management (CRUD)
# ---------------------------------------------------------------------------

@app.post("/grocery/pos/coupons", tags=["Grocery — POS"])
def create_coupon(req: CouponCreate):
    valid_types = {'percent_off', 'dollar_off', 'bogo', 'free_item'}
    if req.coupon_type not in valid_types:
        raise HTTPException(400, f"coupon_type must be one of {sorted(valid_types)}")
    rows = query_write("""
        INSERT INTO pos.coupons
            (code, description, coupon_type, discount_value, min_purchase,
             department_id, product_id, max_uses, valid_from, valid_until, is_active)
        VALUES (%s, %s, %s, %s, %s,
                %s::uuid, %s::uuid, %s, %s, %s, %s)
        RETURNING *
    """, [
        req.code, req.description, req.coupon_type, req.discount_value, req.min_purchase,
        req.department_id, req.product_id, req.max_uses, req.valid_from, req.valid_until, req.is_active
    ], "grocery")
    return rows[0]


@app.patch("/grocery/pos/coupons/{coupon_id}", tags=["Grocery — POS"])
def update_coupon(coupon_id: str, req: CouponPatch):
    updates, params = [], []
    for field, col in [
        ("description", "description"), ("discount_value", "discount_value"),
        ("min_purchase", "min_purchase"), ("valid_from", "valid_from"),
        ("valid_until", "valid_until"), ("is_active", "is_active"), ("max_uses", "max_uses"),
    ]:
        val = getattr(req, field)
        if val is not None:
            updates.append(f"{col} = %s")
            params.append(val)
    if not updates:
        raise HTTPException(400, "No fields to update")
    params.append(coupon_id)
    rows = query_write(
        f"UPDATE pos.coupons SET {', '.join(updates)} WHERE coupon_id = %s::uuid RETURNING *",
        params, "grocery"
    )
    if not rows:
        raise HTTPException(404, "Coupon not found")
    return rows[0]


@app.delete("/grocery/pos/coupons/{coupon_id}", tags=["Grocery — POS"])
def delete_coupon(coupon_id: str):
    rows = execute("DELETE FROM pos.coupons WHERE coupon_id = %s::uuid", [coupon_id], "grocery")
    if rows == 0:
        raise HTTPException(404, "Coupon not found")
    return {"deleted": coupon_id}


# ---------------------------------------------------------------------------
# Grocery — Weekly Ad management (CRUD)
# ---------------------------------------------------------------------------

@app.post("/grocery/pricing/weekly-ads", tags=["Grocery — Pricing"])
def create_weekly_ad(req: WeeklyAdCreate):
    if req.end_date < req.start_date:
        raise HTTPException(400, "end_date must be >= start_date")
    rows = query_write("""
        INSERT INTO pricing.weekly_ads (ad_name, start_date, end_date)
        VALUES (%s, %s, %s)
        RETURNING *
    """, [req.ad_name, req.start_date, req.end_date], "grocery")
    return rows[0]


@app.patch("/grocery/pricing/weekly-ads/{ad_id}", tags=["Grocery — Pricing"])
def update_weekly_ad(ad_id: str, req: WeeklyAdPatch):
    updates, params = [], []
    for field, col in [("ad_name", "ad_name"), ("start_date", "start_date"), ("end_date", "end_date")]:
        val = getattr(req, field)
        if val is not None:
            updates.append(f"{col} = %s")
            params.append(val)
    if not updates:
        raise HTTPException(400, "No fields to update")
    params.append(ad_id)
    rows = query_write(
        f"UPDATE pricing.weekly_ads SET {', '.join(updates)} WHERE ad_id = %s::uuid RETURNING *",
        params, "grocery"
    )
    if not rows:
        raise HTTPException(404, "Weekly ad not found")
    return rows[0]


@app.delete("/grocery/pricing/weekly-ads/{ad_id}", tags=["Grocery — Pricing"])
def delete_weekly_ad(ad_id: str):
    rows = execute("DELETE FROM pricing.weekly_ads WHERE ad_id = %s::uuid", [ad_id], "grocery")
    if rows == 0:
        raise HTTPException(404, "Weekly ad not found")
    return {"deleted": ad_id}


@app.post("/grocery/pricing/ad-items", tags=["Grocery — Pricing"])
def create_ad_item(req: AdItemCreate):
    rows = query_write("""
        INSERT INTO pricing.ad_items (ad_id, product_id, promoted_price, discount_pct)
        VALUES (%s::uuid, %s::uuid, %s, %s)
        ON CONFLICT (ad_id, product_id) DO UPDATE
            SET promoted_price = EXCLUDED.promoted_price,
                discount_pct   = EXCLUDED.discount_pct
        RETURNING *
    """, [req.ad_id, req.product_id, req.promoted_price, req.discount_pct], "grocery")
    return rows[0]


@app.delete("/grocery/pricing/ad-items/{ad_item_id}", tags=["Grocery — Pricing"])
def delete_ad_item(ad_item_id: str):
    rows = execute("DELETE FROM pricing.ad_items WHERE ad_item_id = %s::uuid", [ad_item_id], "grocery")
    if rows == 0:
        raise HTTPException(404, "Ad item not found")
    return {"deleted": ad_item_id}
