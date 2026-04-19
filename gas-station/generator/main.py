"""
Data Generator — main entry point.

Startup: seeds all reference data (idempotent), then enters the generation loop.
Loop modes:
  - realtime : generates transactions representing the current wall-clock time
  - backfill : iterates a date range, generating full historical data
  - stopped  : sleeps, waiting for a control change via the API
"""
import logging
import math
import random
import time
from datetime import datetime, timedelta, date

import psycopg2
import psycopg2.extras

from config import load_config, reload_config
from models import hr, pos, fuel, inventory
from scenarios.scenario_engine import get_scenario_context

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s — %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
)
log = logging.getLogger('generator')


# ---------------------------------------------------------------------------
# DB connection helpers
# ---------------------------------------------------------------------------

def get_connection(cfg):
    return psycopg2.connect(
        host=cfg.db_host,
        port=cfg.db_port,
        user=cfg.db_user,
        password=cfg.db_password,
        dbname=cfg.db_name,
    )


def wait_for_db(cfg, max_retries=30, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            conn = get_connection(cfg)
            conn.close()
            log.info("Database is ready.")
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


def record_stats(conn, pos_count, fuel_count, inv_count, scenario_tag, simulation_dt, elapsed_ms):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO control.generation_stats
                (pos_transactions_generated, fuel_transactions_generated,
                 inventory_receipts_generated, scenario_tag, simulation_dt, wall_clock_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (pos_count, fuel_count, inv_count, scenario_tag, simulation_dt, elapsed_ms))
        cur.execute("""
            UPDATE control.generator_state
            SET last_tick_at = NOW(), updated_at = NOW()
            WHERE state_id = 1
        """)
    conn.commit()


# ---------------------------------------------------------------------------
# Volume calculation
# ---------------------------------------------------------------------------

def compute_counts(cfg, scenario_ctx, daily_pos_base, daily_fuel_base):
    """
    Compute how many transactions to generate for this tick.
    The scenario context has already applied hourly + DOW multipliers.
    We divide daily volume by ticks-per-day then apply the scenario multiplier.
    """
    ticks_per_day = (24 * 60) / max(1, cfg.generator.simulation_minutes_per_tick)
    pos_per_tick = daily_pos_base / ticks_per_day
    fuel_per_tick = daily_fuel_base / ticks_per_day

    pos_count = max(0, round(pos_per_tick * scenario_ctx.volume_multiplier))
    fuel_count = max(0, round(fuel_per_tick * scenario_ctx.volume_multiplier))
    return pos_count, fuel_count


# ---------------------------------------------------------------------------
# Startup seeding
# ---------------------------------------------------------------------------

def seed_all(conn, cfg):
    log.info("Running seed checks...")
    locations = hr.seed_locations(conn, cfg)
    employees = hr.seed_employees(conn, cfg, locations)
    products = pos.seed_products(conn, cfg)
    fuel.seed_pumps(conn, cfg, locations)
    inventory.seed_inventory(conn, cfg, products, locations)
    log.info("Seed complete: %d locations, %d employees, %d products",
             len(locations), len(employees), len(products))
    return locations, employees, products


# ---------------------------------------------------------------------------
# Main generation tick
# ---------------------------------------------------------------------------

def run_tick(conn, cfg, state, locations, employees, products, pumps, grades, members):
    simulation_dt = datetime.now()
    scenario = get_scenario_context(
        state['active_scenario'],
        float(state['volume_multiplier']),
        simulation_dt,
        cfg,
    )

    daily_pos = random.randint(cfg.volumes.pos_transactions_per_day_min,
                               cfg.volumes.pos_transactions_per_day_max)
    daily_fuel = random.randint(cfg.volumes.fuel_transactions_per_day_min,
                                cfg.volumes.fuel_transactions_per_day_max)
    pos_count, fuel_count = compute_counts(cfg, scenario, daily_pos, daily_fuel)

    tick_start = time.monotonic()

    # POS transactions + inventory depletion
    depletion_info = pos.generate_pos_transactions(
        conn, cfg, simulation_dt, pos_count, scenario,
        locations, products, employees, members
    )
    if depletion_info:
        inventory.deplete_inventory(conn, depletion_info, locations)

    # Fuel transactions
    fuel.generate_fuel_transactions(
        conn, cfg, simulation_dt, fuel_count, scenario,
        locations, pumps, grades, employees, members
    )

    # Probabilistic events
    pos.maybe_update_product_prices(conn, cfg, products)
    fuel.maybe_change_fuel_price(conn, cfg, grades)
    hr.maybe_hire_employee(conn, cfg, locations)
    hr.maybe_terminate_employee(conn)

    elapsed_ms = round((time.monotonic() - tick_start) * 1000)
    record_stats(conn, pos_count, fuel_count, 0, scenario.scenario_tag, simulation_dt, elapsed_ms)

    log.info("Tick done in %dms | POS: %d | Fuel: %d | Scenario: %s",
             elapsed_ms, pos_count, fuel_count, scenario.scenario_tag)
    return pos_count, fuel_count


# ---------------------------------------------------------------------------
# Backfill mode
# ---------------------------------------------------------------------------

def run_backfill(conn, cfg, state, locations, employees, products, pumps, grades, members):
    start = state['backfill_start_date']
    end = state['backfill_end_date']
    current = state['backfill_current_date'] or start

    log.info("Starting backfill from %s to %s (current: %s)", start, end, current)

    cur_date = current
    while cur_date <= end:
        log.info("Backfilling day: %s", cur_date)
        last_restock_hour = -1

        for hour in range(24):
            sim_dt = datetime(cur_date.year, cur_date.month, cur_date.day, hour, 0, 0)
            scenario = get_scenario_context(
                state['active_scenario'],
                float(state['volume_multiplier']),
                sim_dt,
                cfg,
            )
            daily_pos = random.randint(cfg.volumes.pos_transactions_per_day_min,
                                       cfg.volumes.pos_transactions_per_day_max)
            daily_fuel = random.randint(cfg.volumes.fuel_transactions_per_day_min,
                                        cfg.volumes.fuel_transactions_per_day_max)

            # Backfill uses hourly ticks (simulation_minutes_per_tick = 60)
            ticks_per_day = 24
            pos_count = max(0, round((daily_pos / ticks_per_day) * scenario.volume_multiplier))
            fuel_count = max(0, round((daily_fuel / ticks_per_day) * scenario.volume_multiplier))

            depletion_info = pos.generate_pos_transactions(
                conn, cfg, sim_dt, pos_count, scenario,
                locations, products, employees, members
            )
            if depletion_info:
                inventory.deplete_inventory(conn, depletion_info, locations)

            fuel.generate_fuel_transactions(
                conn, cfg, sim_dt, fuel_count, scenario,
                locations, pumps, grades, employees, members
            )

        # End-of-day events
        receipt_count = inventory.check_and_restock(conn, cfg, locations)
        fuel.maybe_change_fuel_price(conn, cfg, grades)

        # Advance backfill pointer
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE control.generator_state
                SET backfill_current_date = %s, updated_at = NOW()
                WHERE state_id = 1
            """, (cur_date + timedelta(days=1),))
        conn.commit()
        cur_date += timedelta(days=1)

    # Mark complete
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE control.generator_state
            SET mode = 'stopped', is_running = FALSE, updated_at = NOW()
            WHERE state_id = 1
        """)
    conn.commit()
    log.info("Backfill complete.")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    cfg = load_config()
    wait_for_db(cfg)

    conn = get_connection(cfg)
    psycopg2.extras.register_uuid()

    # Seed reference data
    locations, employees, products = seed_all(conn, cfg)

    log.info("Generator ready. Entering main loop.")

    # Refresh cadence for in-memory caches (every N ticks)
    REFRESH_EVERY = 20
    tick_count = 0

    pumps = fuel.fetch_active_pumps(conn)
    grades = fuel.fetch_fuel_grades(conn)
    members = pos.fetch_loyalty_members(conn)

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
                # Refresh caches before backfill
                pumps = fuel.fetch_active_pumps(conn)
                grades = fuel.fetch_fuel_grades(conn)
                members = pos.fetch_loyalty_members(conn)
                employees = hr.fetch_active_employees(conn)
                run_backfill(conn, cfg, state, locations, employees, products, pumps, grades, members)
                continue

            # Realtime mode
            run_tick(conn, cfg, state, locations, employees, products, pumps, grades, members)

            tick_count += 1
            if tick_count % REFRESH_EVERY == 0:
                pumps = fuel.fetch_active_pumps(conn)
                grades = fuel.fetch_fuel_grades(conn)
                members = pos.fetch_loyalty_members(conn)
                employees = hr.fetch_active_employees(conn)

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
