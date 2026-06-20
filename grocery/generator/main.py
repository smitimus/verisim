"""
Grocery Data Generator — main entry point.

On startup:
  1. Ensures the 'grocery' database and schema exist (self-bootstrapping).
  2. Seeds all reference data (idempotent).
  3. Auto-starts a 30-day backfill when the database is empty, then
     transitions to realtime automatically — no manual configuration needed.
  4. Enters the generation loop (realtime / backfill / stopped).

Backfill behaviour:
  - Skips any date that already has POS transaction data (no duplicates).
  - When backfill_end_date is today, the last day is treated as a partial
    day: generates hours 0 → (current_hour - 1) at their hour boundaries,
    then one final tick at datetime.now() to align exactly with where
    realtime picks up — zero gap between backfill and realtime.
  - Full days (any date before today) use generate_day_events() for
    timeclock; the partial current day uses generate_events() per-hour
    so that open shifts are handled correctly by realtime thereafter.

Supply-chain pipeline (runs once per simulated day):
  POS depletion → low stock → store orders → fulfillment → truck dispatch → delivery receipts
"""
import logging
import os
import random
import time
from datetime import datetime, timedelta, date

import psycopg2
import psycopg2.extras

from config import load_config, reload_config
from models import hr, pos, timeclock, ordering, fulfillment, transport, inventory
from models import shrinkage, promotions, scheduling
from scenarios.scenario_engine import get_scenario_context, get_active_scenario_names

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s — %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
)
log = logging.getLogger('grocery-generator')


# ---------------------------------------------------------------------------
# DB bootstrap — ensures grocery database + schema exist
# ---------------------------------------------------------------------------

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.sql')


def bootstrap_database(cfg):
    """
    Connect to postgres (default db), create grocery DB if missing,
    then run schema.sql if tables don't exist yet.
    """
    # Step 1: create database
    conn = psycopg2.connect(
        host=cfg.db_host, port=cfg.db_port,
        user=cfg.db_user, password=cfg.db_password,
        dbname='postgres',
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (cfg.db_name,))
        if not cur.fetchone():
            log.info("Creating database '%s'...", cfg.db_name)
            cur.execute(f'CREATE DATABASE "{cfg.db_name}"')
    conn.close()

    # Step 2: create schema if tables don't exist
    conn = psycopg2.connect(
        host=cfg.db_host, port=cfg.db_port,
        user=cfg.db_user, password=cfg.db_password,
        dbname=cfg.db_name,
    )
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'control' AND table_name = 'generator_state'
        """)
        if cur.fetchone()[0] == 0:
            log.info("Initializing schema in '%s'...", cfg.db_name)
            with open(SCHEMA_FILE, 'r') as f:
                sql = f.read()
            cur.execute(sql)
            conn.commit()
            log.info("Schema initialized.")
    conn.close()


# ---------------------------------------------------------------------------
# DB connection helpers
# ---------------------------------------------------------------------------

def get_connection(cfg):
    return psycopg2.connect(
        host=cfg.db_host, port=cfg.db_port,
        user=cfg.db_user, password=cfg.db_password,
        dbname=cfg.db_name,
    )


def wait_for_db(cfg, max_retries=30, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=cfg.db_host, port=cfg.db_port,
                user=cfg.db_user, password=cfg.db_password,
                dbname='postgres',
            )
            conn.close()
            log.info("Database server is ready.")
            return
        except psycopg2.OperationalError as e:
            log.warning("DB not ready (attempt %d/%d): %s", attempt, max_retries, e)
            time.sleep(delay)
    raise RuntimeError("Could not connect to database after %d attempts" % max_retries)


# ---------------------------------------------------------------------------
# Control state helpers
# ---------------------------------------------------------------------------

def read_state(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM control.generator_state WHERE state_id = 1")
        return dict(cur.fetchone())


def record_stats(conn, pos_count, timeclock_count, orders_count, scenario_tag, sim_dt, elapsed_ms):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO control.generation_stats
                (pos_transactions_generated, timeclock_events_generated,
                 orders_generated, scenario_tag, simulation_dt, wall_clock_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (pos_count, timeclock_count, orders_count, scenario_tag, sim_dt, elapsed_ms))
        cur.execute("""
            UPDATE control.generator_state
            SET last_tick_at = NOW(), updated_at = NOW()
            WHERE state_id = 1
        """)
    conn.commit()


# ---------------------------------------------------------------------------
# Volume calculation
# ---------------------------------------------------------------------------

def compute_pos_count(cfg, scenario_ctx):
    daily = random.randint(cfg.volumes.pos_transactions_per_day_min,
                           cfg.volumes.pos_transactions_per_day_max)
    ticks_per_day = (24 * 60) / max(1, cfg.generator.simulation_minutes_per_tick)
    per_tick = daily / ticks_per_day
    return max(0, round(per_tick * scenario_ctx.volume_multiplier))


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def seed_all(conn, cfg):
    log.info("Running seed checks...")
    locations = hr.seed_locations(conn, cfg)
    employees = hr.seed_employees(conn, cfg, locations)
    departments = pos.seed_departments(conn, cfg)
    products = pos.seed_products(conn, cfg, departments)
    inventory.seed_inventory(conn, cfg, products, locations['stores'])
    trucks = transport.seed_trucks(conn, truck_count=4)
    pos.seed_named_coupons(conn, departments)
    pos.seed_coupons(conn, cfg, departments, products)
    pos.seed_combo_deals(conn, cfg, departments, products)
    # One-time: mark perishable products + assign shelf_life_days
    shrinkage.mark_perishable_products(conn)
    # Ensure a current weekly ad exists at startup
    promotions.ensure_current_ad(conn, date.today(), products)
    log.info("Seed complete: %d stores, %d warehouses, %d employees, %d products, %d trucks",
             len(locations['stores']), len(locations['warehouses']),
             len(employees), len(products), len(trucks))
    return locations, employees, departments, products, trucks


# ---------------------------------------------------------------------------
# Backfill helpers
# ---------------------------------------------------------------------------

def has_data_for_date(conn, check_date):
    """Return True if POS transactions exist for check_date."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pos.transactions WHERE transaction_dt::date = %s LIMIT 1",
            (check_date,)
        )
        return cur.fetchone() is not None


def auto_backfill_if_fresh(conn, cfg):
    """
    Called once after seed_all(). Handles two scenarios:

    1. Empty DB: configure a 30-day backfill (today-30 → today)
    2. DB has data with gaps in the last 30 days: detect gaps, configure
       a targeted backfill to fill only the missing days, then transition to
       realtime once complete.

    Does nothing if all 30 days are covered — just stays running (or
    transitions to realtime if not already configured).

    Idempotent and safe to call on every startup.
    """
    today = date.today()
    lookback = today - timedelta(days=30)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT min(transaction_dt::date), count(*) FROM pos.transactions"
        )
        row = cur.fetchone()
        if row[1] == 0:
            # Empty DB — full fresh backfill
            log.info("Fresh database — configuring 30-day backfill: %s → %s",
                     lookback, today)
            with conn.cursor() as cur2:
                cur2.execute("""
                    UPDATE control.generator_state SET
                        mode = 'backfill',
                        is_running = TRUE,
                        is_paused = FALSE,
                        backfill_start_date = %s,
                        backfill_end_date = %s,
                        backfill_current_date = %s,
                        started_at = NOW(), updated_at = NOW()
                    WHERE state_id = 1
                """, (lookback, today, lookback))
            conn.commit()
            return

        # Data exists — check for gaps in the last 30 days using max timestamps.
        effective_start = min(row[0] if row[0] else today, lookback)
        effective_end = max(today, row[1] and (today))

        # Get max transaction timestamp per day in the active range.
        # A "complete" day has data reaching the end of the last open hour.
        # The store is now 24/7 — last valid hour is 23.
        cur.execute("""
            SELECT transaction_dt::date AS d, 
                   max(transaction_dt)::timestamp WITHOUT TIME ZONE AS last_txn
            FROM pos.transactions
            WHERE transaction_dt::date BETWEEN %s AND %s
            GROUP BY 1
        """, (effective_start, effective_end))
        by_date = {r[0]: r[1] for r in cur.fetchall() if r}

        # Also get the full date series so we don't miss completely empty days
        cur.execute("""
            SELECT generate_series(%s::date, %s::date, '1 day'::interval)::date AS d
        """, (effective_start, effective_end))
        all_dates = [r[0] for r in cur.fetchall()]

        # A day is "covered" if its last_txn reaches the last open hour (22).
        # Any day with last_txn before that is incomplete — needs resumption.
        missing_dates = []
        for d in all_dates:
            if d not in by_date:
                missing_dates.append(d)
                continue
            last_txn = by_date[d]
            if last_txn.hour >= 23:
                continue  # complete
            missing_dates.append(d)

        if not missing_dates:
            # No gaps — ensure we're in realtime if currently stopped/paused
            already_configured = _ensure_realtime(conn)
            if already_configured:
                return
            with conn.cursor() as cur2:
                cur2.execute("UPDATE control.generator_state SET mode = 'realtime', "
                             "is_running = TRUE, backfill_start_date = NULL, "
                             "backfill_end_date = NULL, backfill_current_date = NULL "
                             "WHERE state_id = 1")
            conn.commit()
            return

        # Gaps found — configure a targeted backfill to fill only missing days
        missing_dates = sorted(missing_dates)
        log.info("Detected %d incomplete/missing day(s) in the last 30 days: %s through %s",
                 len(missing_dates), missing_dates[0], missing_dates[-1])

        with conn.cursor() as cur2:
            # Set backfill range to cover all gaps, even if data is non-contiguous
            cur2.execute("""
                UPDATE control.generator_state SET
                    mode = 'backfill',
                    is_running = TRUE,
                    is_paused = FALSE,
                    backfill_start_date = %s,
                    backfill_end_date = %s,
                    backfill_current_date = %s,
                    started_at = NOW(), updated_at = NOW()
                WHERE state_id = 1
            """, (min(missing_dates), max(missing_dates), min(missing_dates)))
        conn.commit()


def _ensure_realtime(conn):
    """Return True if a backfill was already in progress."""
    with conn.cursor() as cur:
        cur.execute("SELECT mode, is_running FROM control.generator_state WHERE state_id = 1")
        row = cur.fetchone()
        if row and row[0] == 'backfill' and row[1]:
            return True
    return False


# ---------------------------------------------------------------------------
# Main tick (realtime)
# ---------------------------------------------------------------------------

def run_tick(conn, cfg, state, sim_dt, locations, employees, departments,
             products, trucks, members, coupons, deals):
    scenario_names = get_active_scenario_names(conn, sim_dt)
    scenario = get_scenario_context(
        scenario_names,
        float(state['volume_multiplier']),
        sim_dt,
        cfg,
    )
    # Write merged tag back for API display
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE control.generator_state SET active_scenario = %s WHERE state_id = 1",
            (scenario.scenario_tag,)
        )

    pos_count = compute_pos_count(cfg, scenario)
    tick_start = time.monotonic()

    # POS transactions
    depletion = pos.generate_pos_transactions(
        conn, cfg, sim_dt, pos_count, scenario,
        locations['stores'], products, employees, members, coupons, deals
    )
    if depletion:
        inventory.deplete_inventory(conn, depletion)

    # Timeclock events
    tc_count = timeclock.generate_events(conn, sim_dt, employees, locations)

    # Probabilistic events
    pos.maybe_update_product_prices(conn, cfg, products)
    hr.maybe_hire_employee(conn, cfg, locations)
    hr.maybe_terminate_employee(conn)

    # Supply chain + daily models (once per simulated day — check at midnight boundary)
    orders_count = 0
    if sim_dt.hour == 0:
        warehouse_employees = [e for e in employees if e['location_type'] == 'warehouse']
        drivers = [e for e in warehouse_employees if e['department'] == 'transport']
        managers = [e for e in employees if e['department'] == 'management']

        order_ids = ordering.check_and_create_orders(
            conn, locations['stores'], locations['warehouses'], managers, sim_dt)
        orders_count = len(order_ids)

        fulfilled = fulfillment.process_pending_orders(conn, warehouse_employees, sim_dt)

        if fulfilled and locations['warehouses'] and trucks:
            wh_loc_id = locations['warehouses'][0]['location_id']
            transport.dispatch_loads(conn, fulfilled, trucks, drivers, wh_loc_id, sim_dt)

        transport.receive_delivered_loads(conn, sim_dt)

        # Phase 2: perishable expiry dates + shrinkage
        shrinkage.set_expiry_dates(conn, sim_dt)
        shrinkage.generate_shrinkage_events(conn, sim_dt, locations['stores'])

        # Phase 3: weekly ad lifecycle
        promotions.expire_old_ads(conn, sim_dt.date())
        promotions.ensure_current_ad(conn, sim_dt.date(), products)

        # Phase 4: labor scheduling (generate next week) + resolve yesterday's actuals
        scheduling.resolve_schedule_actuals(conn, sim_dt.date())
        scheduling.generate_weekly_schedule(conn, sim_dt.date(), locations, employees)

    elapsed_ms = round((time.monotonic() - tick_start) * 1000)
    record_stats(conn, pos_count, tc_count, orders_count,
                 scenario.scenario_tag, sim_dt, elapsed_ms)
    log.info("Tick done %dms | POS: %d | TC: %d | Orders: %d | Scenario: %s",
             elapsed_ms, pos_count, tc_count, orders_count, scenario.scenario_tag)


# ---------------------------------------------------------------------------
# Backfill mode
# ---------------------------------------------------------------------------

def run_backfill(conn, cfg, state, locations, employees, departments,
                 products, trucks, members, coupons, deals):
    start = state['backfill_start_date']
    end = state['backfill_end_date']
    current = state['backfill_current_date'] or start
    today = date.today()
    now_dt = datetime.now()

    log.info("Starting backfill %s → %s (resuming from %s)", start, end, current)

    warehouse_employees = [e for e in employees if e['location_type'] == 'warehouse']
    drivers = [e for e in warehouse_employees if e['department'] == 'transport']
    managers = [e for e in employees if e['department'] == 'management']

    cur_date = current
    while cur_date <= end:
        # Re-read now_dt each day so the partial-day cutoff stays current
        # for slow backfills that span midnight.
        now_dt = datetime.now()
        today = date.today()

        # For past dates, check if this day is fully generated using max txn timestamp.
        # Resume from the hour AFTER the last recorded transaction instead of hour 0.
        skip_day = False
        start_hour = 0  # default: regenerate full day from hour 0

        if cur_date != today:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT max(transaction_dt)::timestamp FROM pos.transactions "
                    "WHERE transaction_dt::date = %s", (cur_date,)
                )
                row = cur.fetchone()
            last_txn_time = row[0]

            if last_txn_time is None:
                # No data at all — generate the full day (start_hour stays 0)
                pass
            elif last_txn_time.hour >= 23:
                # Fully generated — last open hour (22) has data
                log.info("Skipping %s — already complete (last txn %s)",
                         cur_date, last_txn_time)
                skip_day = True
            else:
                # Partial day — resume from the hour AFTER the last recorded txn
                resuming_from = last_txn_time.replace(minute=0, second=0) + timedelta(hours=1)
                start_hour = min(resuming_from.hour, 24)
                log.info("%s has partial data (last txn %s), regenerating from hour %d",
                         cur_date, last_txn_time, start_hour)

        if skip_day:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE control.generator_state
                    SET backfill_current_date = %s, updated_at = NOW()
                    WHERE state_id = 1
                """, (cur_date + timedelta(days=1),))
            conn.commit()
            cur_date += timedelta(days=1)
            continue

        # Partial day: today gets hours start_hour → current_hour; no end-of-day
        # events (they will run tonight via the realtime midnight handler).
        is_partial = (cur_date == today)
        end_hour = now_dt.hour if is_partial else 23

        log.info("Backfilling %s%s (hours %d–%d)",
                 cur_date, " [partial — up to current hour]" if is_partial else "", start_hour, end_hour)

        for hour in range(start_hour, end_hour + 1):
            # For the last hour of a partial day, use the exact current time so
            # the final backfill tick aligns with where realtime picks up.
            # All previous hours use the hour boundary (e.g. 13:00:00).
            if is_partial and hour == end_hour:
                sim_dt = datetime.now()
            else:
                sim_dt = datetime(cur_date.year, cur_date.month, cur_date.day, hour, 0, 0)
            scenario_names = get_active_scenario_names(conn, sim_dt)
            scenario = get_scenario_context(
                scenario_names,
                float(state['volume_multiplier']),
                sim_dt, cfg,
            )
            daily = random.randint(cfg.volumes.pos_transactions_per_day_min,
                                   cfg.volumes.pos_transactions_per_day_max)
            pos_count = max(0, round((daily / 24) * scenario.volume_multiplier))

            depletion = pos.generate_pos_transactions(
                conn, cfg, sim_dt, pos_count, scenario,
                locations['stores'], products, employees, members, coupons, deals
            )
            if depletion:
                inventory.deplete_inventory(conn, depletion)

            # Partial day: generate timeclock events per-hour using the same
            # idempotent realtime logic (checks existing events before inserting).
            if is_partial:
                timeclock.generate_events(conn, sim_dt, employees, locations)

        if not is_partial:
            # Full day: run all end-of-day events in one pass.
            timeclock.generate_day_events(conn, cur_date, employees)

            order_ids = ordering.check_and_create_orders(
                conn, locations['stores'], locations['warehouses'], managers,
                datetime(cur_date.year, cur_date.month, cur_date.day, 22, 0))
            fulfilled = fulfillment.process_pending_orders(conn, warehouse_employees,
                datetime(cur_date.year, cur_date.month, cur_date.day, 23, 0))
            if fulfilled and locations['warehouses'] and trucks:
                wh_loc_id = locations['warehouses'][0]['location_id']
                transport.dispatch_loads(conn, fulfilled, trucks, drivers, wh_loc_id,
                    datetime(cur_date.year, cur_date.month, cur_date.day, 23, 30))
            transport.receive_delivered_loads(conn,
                datetime(cur_date.year, cur_date.month, cur_date.day, 23, 59))

            pos.maybe_update_product_prices(conn, cfg, products)

            sim_day_end = datetime(cur_date.year, cur_date.month, cur_date.day, 23, 59)
            shrinkage.set_expiry_dates(conn, sim_day_end)
            shrinkage.generate_shrinkage_events(conn, sim_day_end, locations['stores'])
            promotions.expire_old_ads(conn, cur_date)
            promotions.ensure_current_ad(conn, cur_date, products)
            scheduling.resolve_schedule_actuals(conn, cur_date)
            scheduling.generate_weekly_schedule(conn, cur_date, locations, employees)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE control.generator_state
                SET backfill_current_date = %s, updated_at = NOW()
                WHERE state_id = 1
            """, (cur_date + timedelta(days=1),))
        conn.commit()
        cur_date += timedelta(days=1)

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE control.generator_state
            SET mode = 'realtime', is_running = TRUE,
                backfill_start_date = NULL, backfill_end_date = NULL,
                backfill_current_date = NULL, updated_at = NOW()
            WHERE state_id = 1
        """)
    conn.commit()
    log.info("Backfill complete — transitioning to realtime.")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    cfg = load_config()
    wait_for_db(cfg)
    bootstrap_database(cfg)

    conn = get_connection(cfg)
    psycopg2.extras.register_uuid()

    locations, employees, departments, products, trucks = seed_all(conn, cfg)

    # Auto-start a 30-day backfill on a fresh (empty) database.
    auto_backfill_if_fresh(conn, cfg)

    log.info("Generator ready. Entering main loop.")

    REFRESH_EVERY = 20
    tick_count = 0
    members = pos.fetch_loyalty_members(conn)
    coupons = pos.fetch_active_coupons(conn)
    deals = pos.fetch_active_deals(conn)

    while True:
        try:
            cfg = reload_config(cfg)
            state = read_state(conn)

            if state['mode'] == 'stopped' or not state['is_running']:
                time.sleep(state['tick_interval_seconds'])
                continue

            if state['is_paused']:
                time.sleep(state['tick_interval_seconds'])
                continue

            if state['mode'] == 'backfill':
                members = pos.fetch_loyalty_members(conn)
                coupons = pos.fetch_active_coupons(conn)
                deals = pos.fetch_active_deals(conn)
                employees = hr.fetch_active_employees(conn)
                locations = hr.fetch_locations(conn)
                run_backfill(conn, cfg, state, locations, employees, departments,
                             products, trucks, members, coupons, deals)
                continue

            run_tick(conn, cfg, state, datetime.now(), locations, employees,
                     departments, products, trucks, members, coupons, deals)

            tick_count += 1
            if tick_count % REFRESH_EVERY == 0:
                members = pos.fetch_loyalty_members(conn)
                coupons = pos.fetch_active_coupons(conn)
                deals = pos.fetch_active_deals(conn)
                employees = hr.fetch_active_employees(conn)
                locations = hr.fetch_locations(conn)

            time.sleep(state['tick_interval_seconds'])

        except psycopg2.OperationalError as e:
            log.error("DB connection lost: %s — reconnecting...", e)
            time.sleep(10)
            try:
                conn = get_connection(cfg)
            except Exception:
                pass
        except Exception as e:
            log.exception("Unexpected error in main loop: %s", e)
            time.sleep(10)


if __name__ == '__main__':
    main()
