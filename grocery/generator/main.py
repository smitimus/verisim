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
from typing import Dict, Optional

import psycopg2
import psycopg2.extras

from config import load_config, reload_config
from models import hr, pos, timeclock, ordering, fulfillment, transport, inventory
from models import shrinkage, promotions, scheduling, returns, online, weather, customers
import schema_reconcile as reconcile
from elasticity import seed_elasticity_columns
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
    fresh = False
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'control' AND table_name = 'generator_state'
        """)
        if cur.fetchone()[0] == 0:
            fresh = True
            log.info("Initializing schema in '%s'...", cfg.db_name)
            with open(SCHEMA_FILE, 'r') as f:
                sql = f.read()
            cur.execute(sql)
            conn.commit()
            log.info("Schema initialized.")

    if fresh:
        conn.close()
        return

    # Step 3: an EXISTING data dir. Applying schema.sql alone would skip it
    # forever (t_b17da778): a volume only ever receives schema.sql on its first
    # bootstrap, so every change since is absent. Measured on a CT106 volume
    # from 2026-09-21: pos.products.{reference_price, price_elasticity},
    # inv.sku_demand_daily, inv.stockout_events, pos.customers and
    # pos.loyalty_members.customer_id were all missing from a volume holding
    # 1.22M transactions. So reconcile it — idempotently, and without a wipe.
    #
    # schema.sql cannot simply be re-run: none of its 40 CREATE TABLE / 58
    # CREATE INDEX statements carry IF NOT EXISTS, so replaying the file aborts
    # on the first relation that already exists. See schema_reconcile.
    try:
        with open(SCHEMA_FILE, 'r') as f:
            schema_sql = f.read()
        reconcile.run_reconcile(
            conn, schema_sql,
            live_tables=_live_relations(conn),
            seed_row_present=True,          # the volume reached this branch,
        )                                   # so control.generator_state is seeded
    except Exception as exc:                           # noqa: BLE001
        # Boot must not die over the reconcile. The generator's own seed path
        # (`seed_all`) carries the per-relation guards, so a volume that still
        # needs something lands degraded and says so, rather than crashing
        # before the main loop — which is the failure this card is about.
        conn.rollback()
        log.warning("Schema reconcile could not run (%s: %s); continuing. The "
                    "data dir may predate schema changes, so relations or "
                    "columns it gained since may be absent. See "
                    "main.bootstrap_database / schema_reconcile.",
                    type(exc).__name__, exc)
    conn.close()


def _live_relations(conn) -> dict:
    """Every schema-qualified table in the database and the columns it has.

    One catalog query rather than 40+2 probes, because the reconcile pass needs
    the whole shape to plan against and a boot is not the place for 40 round
    trips. Views are excluded: they are not in schema.sql, so a view sharing a
    name with a table is not something this pass should reason about.

    `array_agg` returns `text[]`, and psycopg2 hands a Postgres array back as a
    **string** unless told otherwise — `'{a,b,c}'`. Iterating that directly
    yields single characters, so every column looks absent and the planner
    proposes re-adding every column of every table (measured: 347 pointless
    ALTERs against a volume whose 39 tables were otherwise intact). Selecting
    the plain rows avoids the conversion entirely.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_schema || '.' || table_name AS rel, column_name
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """)
        live: Dict[str, set] = {}
        for rel, column in cur.fetchall():
            live.setdefault(rel, set()).add(str(column).lower())
        return live


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


def _safe_rollback(conn, context: str) -> bool:
    """Clear an aborted transaction, and never raise while doing it.

    psycopg2 has no autocommit-by-default: a failed statement leaves the
    connection in a state where every later command fails with
    `InFailedSqlTransaction` until a ROLLBACK. So any handler that swallows an
    error and retries MUST roll back first, or the retry is theatre.

    It is called from the main loop's failure path, where the connection is by
    definition in a bad state — so a rollback that itself raises would escape
    the `except` block and kill the process, converting a recoverable error
    into the outage this exists to prevent. Hence the blanket guard: log and
    report failure, do not propagate. The loop reconnects on its own schedule.

    `context` is the caller's label so the log says which path rolled back.
    """
    try:
        conn.rollback()
        return True
    except Exception as exc:                          # noqa: BLE001
        log.warning("Rollback after %s failed (%s: %s) — the connection is "
                    "being reconnected by the main loop", context,
                    type(exc).__name__, exc)
        return False


def record_stats(conn, pos_count, timeclock_count, orders_count, scenario_tag, sim_dt, elapsed_ms,
                 bump_state_clock=True):
    """Write one tick's ledger row.

    `bump_state_clock` moves `control.generator_state.last_tick_at`, which is what the
    `/status` route and data-lab's readiness sensor read as "the generator is alive". Only
    a realtime tick means that: during a backfill the generator is simulating *yesterday*
    over and over, so stamping the wall clock there would make a backfilled hour look
    like live progress — the sensor would report the generator alive while it was writing
    history, and `last_tick_at` would stop being comparable with `recorded_at` on the
    ledger rows themselves. `run_backfill` therefore passes False.

    Counts are the rows actually written, never the planned volume: a stockout-capped
    tick (t_959cd040) writes strictly fewer rows than it asked for, and the ledger's
    whole purpose is to say what landed.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO control.generation_stats
                (pos_transactions_generated, timeclock_events_generated,
                 orders_generated, scenario_tag, simulation_dt, wall_clock_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (pos_count, timeclock_count, orders_count, scenario_tag, sim_dt, elapsed_ms))
        if bump_state_clock:
            cur.execute("""
                UPDATE control.generator_state
                SET last_tick_at = NOW(), updated_at = NOW()
                WHERE state_id = 1
            """)
    conn.commit()


# ---------------------------------------------------------------------------
# Volume calculation
#
# One law, two channels: a tick carries `simulated_seconds / 3600` of the hour's
# demand, the hour's demand is the date's budget draw shaped by the scenario
# context, and the date's budget is a pure function of the date. `pos` and
# `online` differ only in which config band their budget comes from — the two
# private laws that drifted apart are exactly how t_94bbf1ce (POS, 30x) and
# t_eb31c99f (online, ~120x plus the hour weight twice) happened.
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


def daily_volume_target(sim_date, seed_prefix, low, high) -> int:
    """A channel's day budget — a pure function of the date.

    Neither writer may own the day's volume: the backfill replays a day one
    simulated hour at a time (and resumes a partial day after a restart), while
    realtime writes it 120 times an hour. Drawing the band once per call would
    hand the same date a different total depending on who wrote it, and a resumed
    day a different total from an uninterrupted one — the same reasoning that made
    `timeclock.plan_shift()` a pure function of (date, employee id) (t_ca6642e0).
    The seed is the channel plus the ISO date, so it is stable across processes
    and runs (and the two channels do not draw the same number); different dates
    still draw different budgets from the configured band.
    """
    rng = random.Random('%s-%s' % (seed_prefix, sim_date.isoformat()))
    return rng.randint(low, high)


def daily_pos_target(cfg, sim_date) -> int:
    """The day's configured POS transaction budget (see `daily_volume_target`)."""
    return daily_volume_target(sim_date, 'verisim-pos',
                               cfg.volumes.pos_transactions_per_day_min,
                               cfg.volumes.pos_transactions_per_day_max)


def daily_online_target(cfg, sim_date) -> int:
    """The day's configured online-order budget (see `daily_volume_target`).

    `online.orders_per_day` is the channel's baseline day: the realised day is
    that draw shaped by the same hour weights, day-of-week multiplier and
    automatic rush-hour boost POS uses, so a day for either channel is
    `budget x shape` and neither channel can drift into its own convention.
    """
    return daily_volume_target(sim_date, 'verisim-online',
                               cfg.online.orders_per_day_min,
                               cfg.online.orders_per_day_max)


def per_tick_volume_expectation(daily, scenario_ctx, simulated_seconds) -> float:
    """Un-rounded volume for ONE tick, before sampling — the shared law.

    `scenario_ctx.volume_multiplier` already carries the hour-of-day weight
    (`hourly_weights[hour] * 24`) and the day-of-week / rush-hour multipliers —
    see `scenario_engine.get_scenario_context` — so one simulated hour's demand is
    `daily * volume_multiplier / 24`, and a tick carries that hour's share:

        per_tick = (daily / 24) * volume_multiplier * simulated_seconds / 3600

    Sizing a tick from anything else is how both channel defects happened:
    `simulation_minutes_per_tick` (a 96-tick day) while realtime ticks 2880 times
    a day made every realtime POS day exactly 30x the configured volume
    (t_94bbf1ce); online had no divisor at all *and* multiplied by
    `hourly_weights[hour]` a second time on top of the one already inside the
    multiplier, which inflated its backfill days 1.6x as well as its realtime days
    ~120x (t_eb31c99f).
    """
    per_hour = (daily / 24.0) * scenario_ctx.volume_multiplier
    return max(0.0, per_hour * simulated_seconds / SIM_HOUR_SECONDS)


def pos_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date, daily=None) -> float:
    """POS volume for one tick, from the date's POS budget.

    `daily` is the date's draw from `volumes.pos_transactions_per_day`; callers
    pass it only to make the arithmetic testable.
    """
    if daily is None:
        daily = daily_pos_target(cfg, sim_date)
    return per_tick_volume_expectation(daily, scenario_ctx, simulated_seconds)


def online_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date, daily=None) -> float:
    """Online-order volume for one tick, from the date's online budget.

    Sibling of `pos_count_expectation`, same law, its own configured band.
    """
    if daily is None:
        daily = daily_online_target(cfg, sim_date)
    return per_tick_volume_expectation(daily, scenario_ctx, simulated_seconds)


def compute_pos_count(cfg, scenario_ctx, simulated_seconds, sim_date) -> int:
    """Integer POS transaction count for one tick."""
    return _unbiased_count(
        pos_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date))


def compute_online_count(cfg, scenario_ctx, simulated_seconds, sim_date) -> int:
    """Integer online-order count for one tick.

    Same sampling as POS, and here it is not a nicety: at 30 s an online tick
    carries ~0.0008 orders in the dead hours and ~0.18 at the 18:00 peak against
    the configured 90..170 band, so `round()` returned 0 for every tick of the
    quiet half of the day and shrank the day (t_eb31c99f).
    """
    return _unbiased_count(
        online_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date))


def _unbiased_count(expected) -> int:
    """Round a fractional count to an int without biasing the mean.

    A 30 s tick carries well under one transaction outside the peaks (0.011 in
    the dead hours, 2.6 at the 18:00 peak for POS; 0.0008 and 0.18 for online),
    so plain `round()` would return 0 for every tick of the quiet half of the
    night and quietly shrink the day. Carrying the fraction as a probability keeps
    the hour's expected volume exact while still writing an integer per tick.
    """
    if expected <= 0:
        return 0
    whole = int(expected)
    return whole + (1 if random.random() < expected - whole else 0)


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def seed_all(conn, cfg):
    log.info("Running seed checks...")
    # First: make sure an OLD data dir has the elasticity columns. A
    # schema.sql change only reaches a *fresh* bootstrap (the same trap the
    # AGENTS.md documents for the PG16->PG18 rebuild and for `pos.returns`), so
    # an install generated before t_08deeddf keeps its old `pos.products` and
    # the demand curve has nothing to key on. Idempotent and additive.
    seed_elasticity_columns(conn, cfg)
    # Same trap, same remedy, for the customer dimension: a `schema.sql` change
    # only reaches a fresh bootstrap, so an install generated before this card
    # has no `pos.customers` and no `loyalty_members.customer_id`. Idempotent and
    # additive, and it must run BEFORE seed_loyalty_members so the seeded cards
    # are dimensioned in the same pass.
    customers.ensure_tables(conn)
    # Same trap, same remedy, for `weather.daily` (t_2ab1fb0a): a schema.sql
    # change only reaches a fresh bootstrap, so an install generated before this
    # card has no `weather` schema at all. Idempotent and additive.
    weather.ensure_table(conn)
    locations = hr.seed_locations(conn, cfg)
    employees = hr.seed_employees(conn, cfg, locations)
    departments = pos.seed_departments(conn, cfg)
    products = pos.seed_products(conn, cfg, departments)
    pos.seed_price_history(conn, cfg, products)
    inventory.seed_inventory(conn, cfg, products, locations['stores'])
    trucks = transport.seed_trucks(conn, truck_count=4)
    # Promotions are seeded with a window that reaches back over the backfill
    # horizon: the back-dated transactions reference them, so a window opening
    # "today" would leave every back-dated redemption outside it.
    history_days = getattr(cfg.generator, 'backfill_lookback_days', 30)
    pos.seed_named_coupons(conn, departments, history_days)
    pos.seed_coupons(conn, cfg, departments, products, history_days)
    pos.seed_combo_deals(conn, cfg, departments, products, history_days)
    pos.seed_loyalty_members(conn, cfg)
    # Every loyalty card gets a household. Idempotent: the candidate set is
    # "cards with a NULL customer_id" and the write sets it, so this also
    # picks up members that predate the dimension and nothing on a re-run.
    customers.backfill_customers(conn, cfg)
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
    lookback_days = getattr(cfg.generator, 'backfill_lookback_days', 30)
    lookback = today - timedelta(days=lookback_days)

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

def get_ad_product_prices(conn, sim_date: date) -> Dict[str, float]:
    """product_id -> `promoted_price` for every item on this week's ad.

    The price OF RECORD for an ad item this week: what the shopper actually
    pays, which is what the demand curve must be keyed on. Before
    t_08deeddf this was never read during generation — `ensure_current_ad`
    returned the ad and nobody used it, so a 30%-off item sat at the same
    odds of reaching a basket as a full-price one and the whole weekly-ad
    signal in `mart_promotion_effectiveness` was decoration.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ai.product_id::text, ai.promoted_price
            FROM pricing.ad_items ai
            JOIN pricing.weekly_ads a ON a.ad_id = ai.ad_id
            WHERE a.start_date <= %s AND a.end_date >= %s
              AND ai.promoted_price IS NOT NULL
              AND ai.promoted_price > 0
        """, (sim_date, sim_date))
        return {r[0]: float(r[1]) for r in cur.fetchall()}

def _weather_for_tick(conn, cfg, sim_date: date, weather_cache: Dict[str, object]) -> Optional[dict]:
    """The day's weather effect, generated once and cached (t_2ab1fb0a).

    Realtime writes 2880 ticks a day at the default 30 s cadence, and
    `ensure_day` is a per-store upsert — running it every tick would be 2880
    redundant round trips. The day's row set does not change, so it is written
    once when the date changes and read back from the table for every tick after
    that, which is also what makes the modifier applied to a tick provably the
    same row a forecasting model would join on.

    `weather_cache` is a single mutable dict holding the cached date and effect
    rather than two arguments plus a return value, so the three call sites stay
    one line each. The DB read is cheap and deliberately NOT cached across a
    date change: a fresh day must be re-read after `ensure_day` writes it.
    """
    if not cfg.weather.enabled:
        return None
    if weather_cache.get('date') != sim_date:
        weather.ensure_day(conn, cfg, sim_date)
        weather_cache['date'] = sim_date
        weather_cache['effect'] = None  # force a read-back of the rows just written
    if weather_cache.get('effect') is None:
        weather_cache['effect'] = weather.day_effect(conn, cfg, sim_date)
    return weather_cache.get('effect')



def run_tick(conn, cfg, state, sim_dt, locations, employees, departments,
             products, trucks, members, coupons, deals, weather_cache=None):
    scenario_names = get_active_scenario_names(conn, sim_dt)
    if weather_cache is None:
        weather_cache = {}
    scenario = get_scenario_context(
        scenario_names,
        float(state['volume_multiplier']),
        sim_dt,
        cfg,
        # The synthetic weather covariate, read back from weather.daily (None
        # when the series is off or the day has no rows, in which case the
        # context is left exactly as it was pre-t_2ab1fb0a).
        weather_effect=_weather_for_tick(conn, cfg, sim_dt.date(), weather_cache),
    )
    # Write merged tag back for API display
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE control.generator_state SET active_scenario = %s WHERE state_id = 1",
            (scenario.scenario_tag,)
        )

    tick_seconds = realtime_tick_seconds(cfg, state)
    sim_date = sim_dt.date()
    pos_count = compute_pos_count(cfg, scenario, tick_seconds, sim_date)
    online_count = compute_online_count(cfg, scenario, tick_seconds, sim_date)
    tick_start = time.monotonic()

    # This week's ad prices: the price of record for ad items, and therefore
    # the key of the demand curve for them (t_08deeddf). One read per tick,
    # shared by both channels — the same law must see the same prices.
    ad_prices = get_ad_product_prices(conn, sim_date)

    # Stock-aware capping (t_959cd040). ONE allowance for the whole tick, shared
    # by both channels: POS and online sell the same shelf, so they must draw
    # from one budget or each would be told it could have the last unit. Built
    # before either channel runs, and flushed once both are done — that single
    # flush is what writes inv.stockout_events (the lost sales), decrements
    # inv.stock_levels, and accumulates inv.sku_demand_daily. Without it,
    # depletion floors at zero and the sale books full-quantity revenue for
    # stock that is not there, and reorder_point / restock_threshold_pct never
    # affect anything.
    stock_allowance = inventory.StockAllowance.from_lines(conn, [
        {'location_id': loc['location_id'], 'product_id': product['product_id']}
        for loc in locations['stores']
        for product in products
    ])

    # POS transactions
    depletion = pos.generate_pos_transactions(
        conn, cfg, sim_dt, pos_count, scenario,
        locations['stores'], products, employees, members, coupons, deals,
        ad_prices=ad_prices,
        stock_allowance=stock_allowance,
    )

    # Loyalty signups this batch just wrote are dimensioned here, not at the
    # next restart: `backfill_customers` is the one code path that creates a
    # household, and its candidate set is "cards with a NULL customer_id", so
    # this is a no-op on every tick that signed nobody up. Without it, a card
    # created on day 30 of a run would carry no segment until someone rebooted
    # the generator — a dimension that fills in behind the mart's back.
    customers.backfill_customers(conn, cfg)

    # Timeclock events
    tc_count = timeclock.generate_events(conn, sim_dt, employees, locations)

    # Online orders (e-commerce pickup + delivery) — same shelves as POS, same
    # volume law, its own configured band (`online.orders_per_day`), and the
    # same stock allowance: an online order is filled from the same shelf a POS
    # sale just drew on.
    online_depletion = online.generate_online_orders(
        conn, cfg, sim_dt, online_count, scenario, locations['stores'], products,
        members, ad_prices=ad_prices, stock_allowance=stock_allowance)
    online.advance_online_lifecycle(conn, cfg, sim_dt)

    # Persist the tick's stock outcome: decrement, stockout rows, daily ledger.
    # The written transaction/order ids are passed so a stockout only points at
    # a sale that exists — a basket the shelf could not cover at all writes no
    # sale row, and its walk-away is recorded with no parent.
    stockouts = inventory.flush_sales(
        conn, stock_allowance, sim_dt, scenario.scenario_tag,
        fulfilled_parents=[d['transaction_id'] for d in depletion]
                          + [d['transaction_id'] for d in online_depletion])

    # Probabilistic events
    pos.maybe_update_product_prices(conn, cfg, products, scenario)
    hr.maybe_hire_employee(conn, cfg, locations)
    hr.maybe_terminate_employee(conn)

    # Supply chain + daily models (once per simulated day — check at midnight boundary)
    orders_count = 0
    if sim_dt.hour == 0:
        warehouse_employees = [e for e in employees if e['location_type'] == 'warehouse']
        drivers = [e for e in warehouse_employees if e['department'] == 'transport']
        managers = [e for e in employees if e['department'] == 'management']

        order_ids = ordering.check_and_create_orders(
            conn, locations['stores'], locations['warehouses'], managers, sim_dt,
            scenario, inventory_cfg=cfg.inventory)
        orders_count = len(order_ids)

        fulfilled = fulfillment.process_pending_orders(conn, warehouse_employees, sim_dt)

        if fulfilled and locations['warehouses'] and trucks:
            wh_loc_id = locations['warehouses'][0]['location_id']
            transport.dispatch_loads(conn, fulfilled, trucks, drivers, wh_loc_id, sim_dt, scenario)

        transport.receive_delivered_loads(conn, sim_dt, scenario)

        # Phase 2: perishable expiry dates + shrinkage
        shrinkage.set_expiry_dates(conn, sim_dt)
        shrinkage.generate_shrinkage_events(conn, sim_dt, locations['stores'], scenario)

        # Phase 3: weekly ad lifecycle
        promotions.expire_old_ads(conn, sim_dt.date())
        promotions.ensure_current_ad(conn, sim_dt.date(), products)

        # Phase 4: labor scheduling (generate next week) + resolve yesterday's actuals
        scheduling.resolve_schedule_actuals(conn, sim_dt.date(), scenario)
        scheduling.generate_weekly_schedule(conn, sim_dt.date(), locations, employees, scenario)
        # Yesterday's shifts, so they are resolved under YESTERDAY's weather —
        # `attendance_modifier` is a per-day covariate now (t_2ab1fb0a), and
        # scoring yesterday's attendance with today's storm would write the
        # call-outs on the wrong shift rows. Read yesterday's own effect rather
        # than reusing this tick's; a day with no rows (the series was off, or
        # the row predates the backfill) leaves the shift resolution exactly as
        # it was pre-t_2ab1fb0a.
        yesterday_ctx = scenario
        if cfg.weather.enabled:
            yesterday = sim_dt.date() - timedelta(days=1)
            yesterday_effect = weather.day_effect(conn, cfg, yesterday)
            if yesterday_effect:
                yesterday_ctx = get_scenario_context(
                    get_active_scenario_names(conn, sim_dt),
                    1.0, sim_dt, cfg,
                    weather_effect=yesterday_effect,
                )
        scheduling.resolve_schedule_actuals(conn, sim_dt.date(), yesterday_ctx)

        # Phase 5: coupon + combo deal lifecycle — deactivate expired, top up
        # active set so the API always serves a current batch (freshness
        # contract with data-lab: raw_pos.combo_deals went STALE when deals
        # expired 2026-08-09 and nothing re-seeded them). Then reconcile the
        # promo rows against the redemptions actually on disk: windows that
        # pass a transaction outside them and a uses_count nobody maintained
        # are both source bugs data-lab asserts against.
        history_days = getattr(cfg.generator, 'backfill_lookback_days', 30)
        pos.seed_coupons(conn, cfg, departments, products, history_days)
        pos.seed_combo_deals(conn, cfg, departments, products, history_days)
        pos.reconcile_promotions(conn)

        # Phase 6: customer returns & refunds for transactions aged 2-14 days
        # (reversals + stock reintegration)
        restock = returns.generate_returns(conn, cfg, sim_dt, scenario)
        if restock:
            returns.restock_returns(conn, restock)

    elapsed_ms = round((time.monotonic() - tick_start) * 1000)
    record_stats(conn, pos_count, tc_count, orders_count,
                 scenario.scenario_tag, sim_dt, elapsed_ms)
    log.info("Tick done %dms | POS: %d | Online: %d | TC: %d | Orders: %d | Stockouts: %d | Scenario: %s",
             elapsed_ms, pos_count, online_count, tc_count, orders_count,
             stockouts, scenario.scenario_tag)


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

        # The day's weather, written BEFORE any of its hours (t_2ab1fb0a).
        # One upsert per day rather than one per simulated hour, and a re-run
        # of a resumed day rewrites the identical values because the series is a
        # pure function of (store, date) — so a gap-filled backfill converges
        # instead of accumulating a second, different weather history.
        weather_cache = {}
        if cfg.weather.enabled:
            weather.ensure_day(conn, cfg, cur_date)

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
                # Read back from the rows `ensure_day` wrote for cur_date, so
                # a backfilled hour sees the same weather a realtime hour on
                # that date would — the whole reason the series is pure.
                weather_effect=_weather_for_tick(conn, cfg, cur_date, weather_cache),
            )
            # One tick per simulated hour here, so the whole hour's demand is
            # written at once — the same law realtime applies 120 times an hour,
            # against the same date-keyed daily budget, so both paths write the
            # same day at the same volume (t_94bbf1ce, t_eb31c99f).
            pos_count = compute_pos_count(cfg, scenario, SIM_HOUR_SECONDS, cur_date)
            online_count = compute_online_count(cfg, scenario, SIM_HOUR_SECONDS, cur_date)
            # The ad in force for the BACKFILLED date, not today's: a backfill
            # hour must see the price of record of the day it is writing, or
            # the ad signal lands on the wrong dates (t_08deeddf).
            ad_prices = get_ad_product_prices(conn, cur_date)

            # Same one-allowance-per-tick rule as the realtime path: the whole
            # simulated hour's POS + online demand is resolved against one
            # snapshot of the shelf, then flushed together (t_959cd040).
            stock_allowance = inventory.StockAllowance.from_lines(conn, [
                {'location_id': loc['location_id'],
                 'product_id': product['product_id']}
                for loc in locations['stores']
                for product in products
            ])

            depletion = pos.generate_pos_transactions(
                conn, cfg, sim_dt, pos_count, scenario,
                locations['stores'], products, employees, members, coupons, deals,
                ad_prices=ad_prices,
                stock_allowance=stock_allowance,
            )

            # Online orders for this hour (placed + lifecycle advanced in
            # time order so statuses converge realistically).
            online_depletion = online.generate_online_orders(
                conn, cfg, sim_dt, online_count, scenario, locations['stores'],
                products, members, ad_prices=ad_prices,
                stock_allowance=stock_allowance)
            online.advance_online_lifecycle(conn, cfg, sim_dt)

            # Decrement the shelf, write the lost sales, update the ledger.
            inventory.flush_sales(
                conn, stock_allowance, sim_dt, scenario.scenario_tag,
                fulfilled_parents=[d['transaction_id'] for d in depletion]
                                  + [d['transaction_id'] for d in online_depletion])

            # Partial day: generate timeclock events per-hour using the same
            # idempotent realtime logic (checks existing events before inserting).
            if is_partial:
                tc_count = timeclock.generate_events(conn, sim_dt, employees, locations)
            else:
                # A full day writes its whole shift schedule in one pass after the
                # hourly loop, so the per-hour timeclock count is only meaningful on a
                # partial day. Report 0 rather than a planned number it never wrote.
                tc_count = 0

            # The tick ledger (t_ac80c514). The backfill writes one row per simulated
            # hour, exactly as the realtime tick loop does, so a consumer asking "was the
            # generator up, and under what regime, when these sales were written?" has
            # an answer for every day the backfill produced — including the 30-day
            # window of a fresh install, which used to exist in the fact tables with no
            # telemetry at all. The holiday/rush-hour tag is `scenario.scenario_tag`,
            # the same context this hour's transactions were generated under, so the tag
            # cannot drift from the rows it describes.
            #
            # Counts are what landed (`len(depletion)`), not `pos_count`: a
            # stockout-capped hour (t_959cd040) writes fewer rows than it planned for.
            record_stats(conn, len(depletion), tc_count, len(online_depletion),
                         scenario.scenario_tag, sim_dt, 0, bump_state_clock=False)

        if not is_partial:
            # Full day: run all end-of-day events in one pass.
            timeclock.generate_day_events(conn, cur_date, employees)

            # Compute scenario context for end-of-day models using a
            # representative time (11pm of the current backfill day).
            eod_dt = datetime(cur_date.year, cur_date.month, cur_date.day, 23, 0)
            eod_scenario_names = get_active_scenario_names(conn, eod_dt)
            eod_scenario = get_scenario_context(
                eod_scenario_names,
                1.0,
                eod_dt, cfg,
                # The SAME day's weather the hourly loop above sampled, so
                # end-of-day models (scheduling, shrinkage, transport) see the
                # covariate the day's transactions were written under.
                weather_effect=_weather_for_tick(conn, cfg, cur_date, weather_cache),
            )

            order_ids = ordering.check_and_create_orders(
                conn, locations['stores'], locations['warehouses'], managers,
                datetime(cur_date.year, cur_date.month, cur_date.day, 22, 0),
                eod_scenario, inventory_cfg=cfg.inventory)
            fulfilled = fulfillment.process_pending_orders(conn, warehouse_employees,
                datetime(cur_date.year, cur_date.month, cur_date.day, 23, 0))
            if fulfilled and locations['warehouses'] and trucks:
                wh_loc_id = locations['warehouses'][0]['location_id']
                transport.dispatch_loads(conn, fulfilled, trucks, drivers, wh_loc_id,
                    datetime(cur_date.year, cur_date.month, cur_date.day, 23, 30),
                    eod_scenario)
            transport.receive_delivered_loads(conn,
                datetime(cur_date.year, cur_date.month, cur_date.day, 23, 59),
                eod_scenario)

            pos.maybe_update_product_prices(conn, cfg, products, eod_scenario)

            sim_day_end = datetime(cur_date.year, cur_date.month, cur_date.day, 23, 59)
            shrinkage.set_expiry_dates(conn, sim_day_end)
            shrinkage.generate_shrinkage_events(conn, sim_day_end, locations['stores'], eod_scenario)
            promotions.expire_old_ads(conn, cur_date)
            promotions.ensure_current_ad(conn, cur_date, products)
            scheduling.resolve_schedule_actuals(conn, cur_date, eod_scenario)
            scheduling.generate_weekly_schedule(conn, cur_date, locations, employees, eod_scenario)
            # Yesterday's shifts resolved under YESTERDAY's weather, exactly as
            # the realtime midnight path does (t_2ab1fb0a). Inside a backfilled
            # day the wrong day's storm here would be invisible in the day's own
            # transactions, and still put the call-outs on the wrong rows.
            prev_effect = (weather.day_effect(conn, cfg, cur_date - timedelta(days=1))
                           if cfg.weather.enabled else None)
            prev_ctx = get_scenario_context(
                eod_scenario_names, 1.0, eod_dt, cfg,
                weather_effect=prev_effect or weather_cache.get('effect'),
            )
            scheduling.resolve_schedule_actuals(conn, cur_date, prev_ctx)

            # Coupon + combo deal lifecycle (same as realtime Phase 5) —
            # a 30-day backfill must not leave deals expired at the end.
            history_days = getattr(cfg.generator, 'backfill_lookback_days', 30)
            pos.seed_coupons(conn, cfg, departments, products, history_days)
            pos.seed_combo_deals(conn, cfg, departments, products, history_days)
            pos.reconcile_promotions(conn)

            # Phase 6: returns for transactions aged 2-14 days relative to
            # this backfill day (same contract as realtime).
            restock = returns.generate_returns(conn, cfg,
                datetime(cur_date.year, cur_date.month, cur_date.day, 23, 30),
                eod_scenario)
            if restock:
                returns.restock_returns(conn, restock)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE control.generator_state
                SET backfill_current_date = %s, updated_at = NOW()
                WHERE state_id = 1
            """, (cur_date + timedelta(days=1),))
        conn.commit()
        cur_date += timedelta(days=1)

    # The last (partial) backfill day skips the day-end block above, so its
    # redemptions would stay uncounted until the next simulated midnight.
    # Reconcile once here so a freshly loaded database reads exactly correct.
    pos.reconcile_promotions(conn)

    # Dimension every loyalty card the backfill just wrote, in ONE pass over
    # the whole set — deliberately NOT per day. `plan_households` groups cards
    # by walking them in signup order and deciding, per card, whether it joins
    # the household already open; run per backfill day instead, the walk would
    # restart at each day's boundary and the second adult's card — signed up on
    # a later day than the first — would never be considered as sitting next to
    # its partner. A 30-day backfill would then produce ~30 single-card
    # households instead of the intended mix, and the segment mix would depend
    # on where the backfill happened to be cut. It is idempotent and cheap (it
    # no-ops the moment nothing has a NULL customer_id), so running it here as
    # well as per-tick in realtime is safe.
    customers.backfill_customers(conn, cfg)

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

    # Repair promo windows/counters against whatever history is already on
    # disk — an install generated before the windowing fix would otherwise
    # keep its out-of-window redemptions (card t_01b4fe4f). Idempotent.
    pos.reconcile_promotions(conn)

    log.info("Generator ready. Entering main loop.")

    REFRESH_EVERY = 20
    tick_count = 0
    members = pos.fetch_loyalty_members(conn)
    coupons = pos.fetch_active_coupons(conn)
    deals = pos.fetch_active_deals(conn)
    # Survives across ticks, not across a restart: the day's weather rows are
    # written once when the date changes and then read back from the table, so
    # a 30 s tick does not re-upsert 2880 times a day (t_2ab1fb0a). A restart
    # re-derives it from the same pure seed, so the values are unchanged.
    weather_cache = {}

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
                     departments, products, trucks, members, coupons, deals,
                     weather_cache=weather_cache)

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
            _safe_rollback(conn, "OperationalError")
            time.sleep(10)
            try:
                conn = get_connection(cfg)
            except Exception:
                pass
        except Exception as e:
            log.exception("Unexpected error in main loop: %s", e)
            # Roll back or the loop is dead for the life of the container
            # (t_f963eeb1). This is not a defensive nicety — it is the whole
            # bug. Measured on CT106 2026-10-04: one FK violation at 08:11:05
            # was followed by 446 consecutive failures that all read
            # "current transaction is aborted, commands ignored until end of
            # transaction block", because psycopg2 leaves a connection aborted
            # after a failed statement and every later command inherits that
            # state. The original error appeared exactly once; the noise after
            # it hid it. Nothing was written for the remaining 74 minutes of
            # the container's life while `docker ps` stayed healthy, because
            # the healthcheck probes Postgres and never asks whether the
            # generator is still generating.
            #
            # The rollback must not itself be able to kill the loop: it runs
            # on the failure path, so a connection that is already unusable is
            # the expected case, not an exception.
            _safe_rollback(conn, "main-loop exception")

            # Re-read the caches immediately, or the rollback above only buys
            # one more failing tick. The in-memory promo/member sets are a
            # snapshot taken at boot and refreshed every REFRESH_EVERY ticks —
            # and `tick_count` is only incremented AFTER a successful tick, so
            # while a bad id is causing failures the counter never reaches the
            # next refresh and the stale id is never dropped. That is how the
            # delete of a coupon the generator had cached turns into repeated
            # FK violations rather than a single one. Re-reading here drops the
            # vanished row on the very next tick, deterministically, whether or
            # not the loop happens to reach a refresh multiple.
            #
            # A failure here must not mask the original error or end the loop,
            # so it is best-effort: the caches stay as they were and the next
            # tick retries with the old snapshot.
            try:
                members = pos.fetch_loyalty_members(conn)
                coupons = pos.fetch_active_coupons(conn)
                deals = pos.fetch_active_deals(conn)
                employees = hr.fetch_active_employees(conn)
                locations = hr.fetch_locations(conn)
            except Exception as refresh_exc:              # noqa: BLE001
                log.warning("Cache refresh after error failed (%s: %s) — "
                            "retrying the next tick with the previous snapshot",
                            type(refresh_exc).__name__, refresh_exc)
            time.sleep(10)


if __name__ == '__main__':
    main()
