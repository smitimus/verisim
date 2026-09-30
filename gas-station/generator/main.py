"""
Gas Station Data Generator — main entry point.

On startup:
  1. Ensures the gas_station database and schema exist (self-bootstrapping).
  2. Seeds all reference data (idempotent).
  3. Auto-starts a 30-day backfill when the database is empty, then
     transitions to realtime automatically.
  4. Enters the generation loop (realtime / backfill / stopped).

Backfill behaviour (grocery parity, t_a6ecb731):
  - Skips any date that already has POS transaction data (no duplicates).
  - Partial days resume from the hour AFTER the last recorded transaction.
  - When backfill_end_date is today, the last day is generated hour-by-hour
    up to the current hour, then realtime takes over seamlessly.

Tick lifecycle (realtime):
  1. reload_config() -> read_state()
  2. Scenario context (volume multiplier, promotions, fuel price modifier)
  3. POS transactions -> inventory depletion
  4. Fuel transactions (ACM-grade pricing with scenario modifier)
  5. Probabilistic events (price changes, hire/terminate)
  6. End-of-day (midnight boundary): restock check + fuel price move
  7. record_stats() -> control.generation_stats
"""
import logging
import os
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
log = logging.getLogger('gas-generator')

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.sql')


# ---------------------------------------------------------------------------
# DB bootstrap — ensures gas_station database + schema exist
# ---------------------------------------------------------------------------

def bootstrap_database(cfg):
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
                cur.execute(f.read())
            conn.commit()
            log.info("Schema initialized.")
    conn.close()


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


def record_stats(conn, pos_count, fuel_count, inv_count, scenario_tag,
                 simulation_dt, elapsed_ms):
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
#
# One law, two channels: a tick carries `simulated_seconds / 3600` of the
# hour's demand, the hour's demand is the date's budget draw shaped by the
# scenario context, and the date's budget is a pure function of the date. `pos`
# and `fuel` differ only in which config band their budget comes from — the two
# private laws are exactly how the 30x realtime defect happened in grocery
# (t_94bbf1ce) and, verbatim, here (t_093fa22c).
# ---------------------------------------------------------------------------

# One simulated hour of demand. The backfill writes exactly one tick per
# simulated hour; realtime writes one tick per `tick_interval_seconds` of wall
# clock, so the same hour arrives in 120 ticks at the default 30 s cadence
# (2880 a day) — that ratio is what the per-tick volume must be divided by.
SIM_HOUR_SECONDS = 3600

DEFAULT_TICK_INTERVAL_SECONDS = 30


def realtime_tick_seconds(cfg, state) -> int:
    """Seconds of simulated time one realtime tick writes.

    The main loop sleeps `control.generator_state.tick_interval_seconds` (seeded
    from config, settable 5-3600 s from the API/UI) — that is the cadence the
    generator really runs at, so it is the one the volume law must use, not
    `simulation_minutes_per_tick`.
    """
    value = (state or {}).get('tick_interval_seconds') or cfg.generator.tick_interval_seconds
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return DEFAULT_TICK_INTERVAL_SECONDS
    return seconds if seconds > 0 else DEFAULT_TICK_INTERVAL_SECONDS


def partial_hour_seconds(now) -> int:
    """Simulated seconds of the hour that have elapsed at `now`.

    The backfill's last tick writes the current hour at `datetime.now()`, and
    realtime continues from there: sizing that tick from the elapsed part of the
    hour is what makes the hour total one hour of demand instead of two.
    """
    return now.minute * 60 + now.second


def daily_volume_target(sim_date, seed_prefix, low, high) -> int:
    """A channel's day budget — a pure function of the date.

    Neither writer may own the day's volume: the backfill replays a day one
    simulated hour at a time (and resumes a partial day after a restart), while
    realtime writes it 120 times an hour. Drawing the band once per call would
    hand the same date a different total depending on who wrote it, and a
    resumed day a different total from an uninterrupted one — the same reasoning
    that made `timeclock.plan_shift()` a pure function of (date, employee id) in
    grocery (t_ca6642e0). The seed is the channel plus the ISO date, so it is
    stable across processes and runs (and the two channels do not draw the same
    number); different dates still draw different budgets from the band.
    """
    rng = random.Random('%s-%s' % (seed_prefix, sim_date.isoformat()))
    return rng.randint(low, high)


def daily_pos_target(cfg, sim_date) -> int:
    """The day's configured convenience-store transaction budget."""
    return daily_volume_target(sim_date, 'verisim-pos',
                               cfg.volumes.pos_transactions_per_day_min,
                               cfg.volumes.pos_transactions_per_day_max)


def daily_fuel_target(cfg, sim_date) -> int:
    """The day's configured fuel transaction budget (see `daily_volume_target`)."""
    return daily_volume_target(sim_date, 'verisim-fuel',
                               cfg.volumes.fuel_transactions_per_day_min,
                               cfg.volumes.fuel_transactions_per_day_max)


def per_tick_volume_expectation(daily, scenario_ctx, simulated_seconds) -> float:
    """Un-rounded volume for ONE tick, before sampling — the shared law.

    `scenario_ctx.volume_multiplier` already carries the hour-of-day weight
    (`hourly_weights[hour] * 24`) and the day-of-week / rush-hour multipliers —
    see `scenario_engine.get_scenario_context` — so one simulated hour's demand is
    `daily * volume_multiplier / 24`, and a tick carries that hour's share:

        per_tick = (daily / 24) * volume_multiplier * simulated_seconds / 3600

    Sizing a tick from anything else is how the defect happened: dividing by
    `(24 * 60) / simulation_minutes_per_tick` — a 96-tick day — while the loop
    sleeps 30 s (2880 ticks a day) made every realtime day exactly 30x the
    configured band (t_093fa22c, same class as grocery's t_94bbf1ce).
    """
    per_hour = (daily / 24.0) * scenario_ctx.volume_multiplier
    return max(0.0, per_hour * simulated_seconds / SIM_HOUR_SECONDS)


def pos_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date, daily=None) -> float:
    """Store volume for one tick, from the date's POS budget.

    `daily` is the date's draw from `volumes.pos_transactions_per_day`; callers
    pass it only to make the arithmetic testable.
    """
    if daily is None:
        daily = daily_pos_target(cfg, sim_date)
    return per_tick_volume_expectation(daily, scenario_ctx, simulated_seconds)


def fuel_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date, daily=None) -> float:
    """Fuel volume for one tick, from the date's fuel budget.

    Sibling of `pos_count_expectation`, same law, its own configured band.
    """
    if daily is None:
        daily = daily_fuel_target(cfg, sim_date)
    return per_tick_volume_expectation(daily, scenario_ctx, simulated_seconds)


def compute_pos_count(cfg, scenario_ctx, simulated_seconds, sim_date) -> int:
    """Integer store transaction count for one tick."""
    return _unbiased_count(
        pos_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date))


def compute_fuel_count(cfg, scenario_ctx, simulated_seconds, sim_date) -> int:
    """Integer fuel transaction count for one tick."""
    return _unbiased_count(
        fuel_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date))


def _unbiased_count(expected) -> int:
    """Round a fractional count to an int without biasing the mean.

    A 30 s tick carries well under one transaction outside the commute peaks, so
    plain `round()` would return 0 for every tick of the quiet half of the night
    and quietly shrink the day. Carrying the fraction as a probability keeps the
    hour's expected volume exact while still writing an integer per tick.
    """
    if expected <= 0:
        return 0
    whole = int(expected)
    return whole + (1 if random.random() < expected - whole else 0)


def compute_counts(cfg, scenario_ctx, simulated_seconds, sim_date):
    """POS + fuel counts for one tick from the date's budgets + scenario context."""
    return (compute_pos_count(cfg, scenario_ctx, simulated_seconds, sim_date),
            compute_fuel_count(cfg, scenario_ctx, simulated_seconds, sim_date))


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
# Fresh-DB auto backfill (gap-aware, idempotent)
# ---------------------------------------------------------------------------

def auto_backfill_if_fresh(conn, cfg):
    today = date.today()
    lookback = today - timedelta(days=30)

    with conn.cursor() as cur:
        cur.execute("SELECT min(transaction_dt::date), count(*) FROM pos.transactions")
        row = cur.fetchone()
        if row[1] == 0:
            log.info("Fresh database — configuring 30-day backfill: %s → %s",
                     lookback, today)
            cur.execute("""
                UPDATE control.generator_state SET
                    mode = 'backfill', is_running = TRUE, is_paused = FALSE,
                    backfill_start_date = %s, backfill_end_date = %s,
                    backfill_current_date = %s, started_at = NOW(), updated_at = NOW()
                WHERE state_id = 1
            """, (lookback, today, lookback))
            conn.commit()
            return

        effective_start = min(row[0], lookback)
        cur.execute("""
            SELECT transaction_dt::date AS d,
                   max(transaction_dt)::timestamp WITHOUT TIME ZONE AS last_txn
            FROM pos.transactions
            WHERE transaction_dt::date BETWEEN %s AND %s
            GROUP BY 1
        """, (effective_start, today))
        by_date = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute("SELECT generate_series(%s::date, %s::date, '1 day'::interval)::date",
                    (effective_start, today))
        all_dates = [r[0] for r in cur.fetchall()]
        missing = [d for d in all_dates if d not in by_date or by_date[d].hour < 23]

        if not missing:
            cur.execute("SELECT mode FROM control.generator_state WHERE state_id = 1")
            if cur.fetchone()[0] != 'backfill':
                cur.execute("""UPDATE control.generator_state SET mode = 'realtime',
                               is_running = TRUE, backfill_start_date = NULL,
                               backfill_end_date = NULL, backfill_current_date = NULL
                               WHERE state_id = 1""")
            conn.commit()
            return

        missing.sort()
        log.info("Detected %d incomplete/missing day(s) — backfilling %s → %s",
                 len(missing), missing[0], missing[-1])
        cur.execute("""
            UPDATE control.generator_state SET
                mode = 'backfill', is_running = TRUE, is_paused = FALSE,
                backfill_start_date = %s, backfill_end_date = %s,
                backfill_current_date = %s, started_at = NOW(), updated_at = NOW()
            WHERE state_id = 1
        """, (missing[0], missing[-1], missing[0]))
        conn.commit()


# ---------------------------------------------------------------------------
# Shared per-tick generation (realtime + backfill)
# ---------------------------------------------------------------------------

def _generate_for_dt(conn, cfg, sim_dt, scenario, simulated_seconds, locations,
                     employees, products, pumps, grades, members):
    """One tick's activity, sized from the simulated time the tick covers.

    `simulated_seconds` is the cadence the caller really runs at: the interval
    the main loop sleeps in realtime, one whole simulated hour in the backfill.
    """
    pos_count, fuel_count = compute_counts(cfg, scenario, simulated_seconds, sim_dt.date())

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
    return pos_count, fuel_count


# ---------------------------------------------------------------------------
# Main tick (realtime)
# ---------------------------------------------------------------------------

def run_tick(conn, cfg, state, locations, employees, products, pumps, grades, members):
    simulation_dt = datetime.now()
    scenario = get_scenario_context(
        state['active_scenario'],
        float(state['volume_multiplier']),
        simulation_dt,
        cfg,
    )
    with conn.cursor() as cur:
        cur.execute("UPDATE control.generator_state SET active_scenario = %s WHERE state_id = 1",
                    (scenario.scenario_tag,))

    tick_start = time.monotonic()
    pos_count, fuel_count = _generate_for_dt(
        conn, cfg, simulation_dt, scenario, realtime_tick_seconds(cfg, state),
        locations, employees, products, pumps, grades, members)

    # Probabilistic events
    pos.maybe_update_product_prices(conn, cfg, products)
    hr.maybe_hire_employee(conn, cfg, locations)
    hr.maybe_terminate_employee(conn)

    # End-of-day models at the midnight boundary (once per simulated day)
    receipts = 0
    if simulation_dt.hour == 0:
        receipts = inventory.check_and_restock(conn, cfg, locations)
        fuel.maybe_change_fuel_price(conn, cfg, grades)

    elapsed_ms = round((time.monotonic() - tick_start) * 1000)
    record_stats(conn, pos_count, fuel_count, receipts,
                 scenario.scenario_tag, simulation_dt, elapsed_ms)
    log.info("Tick done %dms | POS: %d | Fuel: %d | Receipts: %d | Scenario: %s",
             elapsed_ms, pos_count, fuel_count, receipts, scenario.scenario_tag)


# ---------------------------------------------------------------------------
# Backfill mode
# ---------------------------------------------------------------------------

def run_backfill(conn, cfg, state, locations, employees, products, pumps, grades, members):
    start = state['backfill_start_date']
    end = state['backfill_end_date']
    current = state['backfill_current_date'] or start
    today = date.today()
    now_dt = datetime.now()

    log.info("Starting backfill %s → %s (resuming from %s)", start, end, current)

    cur_date = current
    while cur_date <= end:
        now_dt = datetime.now()
        today = date.today()

        skip_day = False
        start_hour = 0
        if cur_date != today:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT max(transaction_dt)::timestamp FROM pos.transactions
                    WHERE transaction_dt::date = %s
                """, (cur_date,))
                last = cur.fetchone()[0]
            if last is not None:
                if last.hour >= 23:
                    log.info("Skipping %s — already complete (last txn %s)", cur_date, last)
                    skip_day = True
                else:
                    start_hour = min(last.hour + 1, 23)
                    log.info("%s partial (last txn %s) — resuming from hour %d",
                             cur_date, last, start_hour)

        if not skip_day:
            is_partial = (cur_date == today)
            end_hour = now_dt.hour if is_partial else 23
            log.info("Backfilling %s%s (hours %d–%d)", cur_date,
                     " [partial — up to current hour]" if is_partial else "",
                     start_hour, end_hour)

            for hour in range(start_hour, end_hour + 1):
                if is_partial and hour == end_hour:
                    sim_dt = datetime.now()
                    tick_seconds = partial_hour_seconds(sim_dt)
                else:
                    sim_dt = datetime(cur_date.year, cur_date.month, cur_date.day, hour, 0, 0)
                    tick_seconds = SIM_HOUR_SECONDS
                scenario = get_scenario_context(
                    state['active_scenario'],
                    float(state['volume_multiplier']), sim_dt, cfg)
                pos_count, fuel_count = _generate_for_dt(
                    conn, cfg, sim_dt, scenario, tick_seconds,
                    locations, employees, products, pumps, grades, members)
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO control.generation_stats
                            (pos_transactions_generated, fuel_transactions_generated,
                             inventory_receipts_generated, scenario_tag, simulation_dt,
                             wall_clock_ms)
                        VALUES (%s,%s,0,%s,%s,0)
                    """, (pos_count, fuel_count, scenario.scenario_tag, sim_dt))
                conn.commit()

            # End-of-day events for full days (partial day handled by realtime)
            if not is_partial:
                inventory.check_and_restock(conn, cfg, locations)
                fuel.maybe_change_fuel_price(conn, cfg, grades)

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

    locations, employees, products = seed_all(conn, cfg)
    auto_backfill_if_fresh(conn, cfg)

    log.info("Generator ready. Entering main loop.")

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
                pumps = fuel.fetch_active_pumps(conn)
                grades = fuel.fetch_fuel_grades(conn)
                members = pos.fetch_loyalty_members(conn)
                employees = hr.fetch_active_employees(conn)
                run_backfill(conn, cfg, state, locations, employees, products,
                             pumps, grades, members)
                continue

            run_tick(conn, cfg, state, locations, employees, products,
                     pumps, grades, members)

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
