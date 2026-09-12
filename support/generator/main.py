"""
Customer-Support Data Generator — main entry point.

On startup:
  1. Ensures the 'support' database and schema exist (self-bootstrapping).
  2. Seeds reference data: queues, categories, customers, locations,
     agents, courses.
  3. Auto-starts a 30-day backfill on a fresh DB, then realtime.
  4. Runs the tick loop.

Tick lifecycle (realtime):
  1. Scenario context (contact surge, sentiment shift)
  2. Voice ACD calls
  3. Chat sessions + transcripts
  4. Tickets (creation + lifecycle advancement: queue moves, escalation,
     resolve/close/reopen) — phone/chat contacts seed follow-up tickets
  5. sNPS surveys (send + respond)
  6. Training assignments/completions
  7. Probabilistic HR (hire/terminate) + new customers
  8. record_stats -> control.generation_stats

Backfill mirrors realtime per simulated day and is idempotent (a day is
skipped when its tickets already cover the full day).
"""
import logging
import os
import random
import time
from datetime import datetime, timedelta, date

import psycopg2
import psycopg2.extras

from config import load_config, reload_config
from models import hr, customers as cust_model, tickets, voice, chat, survey, training
from scenarios.scenario_engine import get_scenario_context, get_active_scenario_names

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s — %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
)
log = logging.getLogger('support-generator')

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.sql')


# ---------------------------------------------------------------------------
# DB bootstrap
# ---------------------------------------------------------------------------

def bootstrap_database(cfg):
    conn = psycopg2.connect(
        host=cfg.db_host, port=cfg.db_port,
        user=cfg.db_user, password=cfg.db_password, dbname='postgres')
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (cfg.db_name,))
        if not cur.fetchone():
            log.info("Creating database '%s'...", cfg.db_name)
            cur.execute(f'CREATE DATABASE "{cfg.db_name}"')
    conn.close()

    conn = psycopg2.connect(
        host=cfg.db_host, port=cfg.db_port,
        user=cfg.db_user, password=cfg.db_password, dbname=cfg.db_name)
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


def get_connection(cfg):
    return psycopg2.connect(
        host=cfg.db_host, port=cfg.db_port,
        user=cfg.db_user, password=cfg.db_password, dbname=cfg.db_name)


def wait_for_db(cfg, max_retries=30, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=cfg.db_host, port=cfg.db_port,
                user=cfg.db_user, password=cfg.db_password, dbname='postgres')
            conn.close()
            log.info("Database server is ready.")
            return
        except psycopg2.OperationalError as e:
            log.warning("DB not ready (attempt %d/%d): %s", attempt, max_retries, e)
            time.sleep(delay)
    raise RuntimeError("Could not connect to database after %d attempts" % max_retries)


# ---------------------------------------------------------------------------
# Control state
# ---------------------------------------------------------------------------

def read_state(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM control.generator_state WHERE state_id = 1")
        return dict(cur.fetchone())


def record_stats(conn, tickets_n, calls_n, chats_n, surveys_n, scenario_tag,
                 sim_dt, elapsed_ms):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO control.generation_stats
                (tickets_generated, calls_generated, chat_sessions_generated,
                 surveys_generated, scenario_tag, simulation_dt, wall_clock_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (tickets_n, calls_n, chats_n, surveys_n, scenario_tag, sim_dt, elapsed_ms))
        cur.execute("""
            UPDATE control.generator_state
            SET last_tick_at = NOW(), updated_at = NOW() WHERE state_id = 1
        """)
    conn.commit()


# ---------------------------------------------------------------------------
# Volume calculation
# ---------------------------------------------------------------------------

def _daily_volumes(cfg, ctx):
    """(tickets, calls, chats) for one tick given scenario context."""
    ticks_per_day = (24 * 60) / max(1, cfg.generator.simulation_minutes_per_tick)
    t = random.randint(cfg.volumes.tickets_per_day_min, cfg.volumes.tickets_per_day_max)
    c = random.randint(cfg.volumes.calls_per_day_min, cfg.volumes.calls_per_day_max)
    h = random.randint(cfg.volumes.chats_per_day_min, cfg.volumes.chats_per_day_max)
    return (max(0, round(t / ticks_per_day * ctx.volume_multiplier)),
            max(0, round(c / ticks_per_day * ctx.volume_multiplier)),
            max(0, round(h / ticks_per_day * ctx.volume_multiplier)))


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def seed_all(conn, cfg):
    log.info("Running seed checks...")
    queues = cust_model.seed_queues(conn, cfg)
    locations = hr.seed_locations(conn, cfg)
    employees = hr.seed_employees(conn, cfg, locations, queues)
    cust_model.seed_customers(conn, cfg)
    courses = training.seed_courses(conn, cfg)
    # Every agent gets mandatory onboarding (idempotent; also covers later hires)
    from datetime import datetime as _dt
    training.assign_missing_onboarding(conn, cfg, _dt.now())
    log.info("Seed complete: %d queues, %d locations, %d employees, %d courses",
             len(queues), len(locations['all']), len(employees), len(courses))
    return queues, locations, employees, courses


# ---------------------------------------------------------------------------
# Fresh-DB auto backfill (gap-aware)
# ---------------------------------------------------------------------------

def auto_backfill_if_fresh(conn, cfg):
    today = date.today()
    lookback = today - timedelta(days=30)
    with conn.cursor() as cur:
        cur.execute("SELECT min(created_dt::date), count(*) FROM support.tickets")
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

        cur.execute("""
            SELECT created_dt::date AS d, max(created_dt)::timestamp WITHOUT TIME ZONE AS last_t
            FROM support.tickets
            WHERE created_dt::date BETWEEN %s AND %s
            GROUP BY 1
        """, (lookback, today))
        by_date = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute("SELECT generate_series(%s::date, %s::date, '1 day'::interval)::date",
                    (lookback, today))
        all_dates = [r[0] for r in cur.fetchall()]
        missing = [d for d in all_dates
                   if d not in by_date or by_date[d].hour < 22]
        if not missing:
            cur.execute("SELECT mode FROM control.generator_state WHERE state_id = 1")
            if cur.fetchone()[0] != 'backfill':
                cur.execute("""UPDATE control.generator_state SET mode = 'realtime',
                               is_running = TRUE WHERE state_id = 1""")
            conn.commit()
            return
        missing.sort()
        log.info("Detected %d incomplete day(s) — backfilling %s → %s",
                 len(missing), missing[0], missing[-1])
        cur.execute("""
            UPDATE control.generator_state SET
                mode = 'backfill', is_running = TRUE, is_paused = FALSE,
                backfill_start_date = %s, backfill_end_date = %s,
                backfill_current_date = %s, updated_at = NOW()
            WHERE state_id = 1
        """, (missing[0], missing[-1], missing[0]))
        conn.commit()


# ---------------------------------------------------------------------------
# Per-tick generation
# ---------------------------------------------------------------------------

def generate_for_dt(conn, cfg, sim_dt, ctx, caches, seed_followups=False):
    """Generate one tick's worth of activity. Shared by realtime + backfill."""
    queues = caches['queues']
    customers = caches['customers']
    agents = caches['agents']

    n_tickets, n_calls, n_chats = _daily_volumes(cfg, ctx)

    calls_n, call_followups = voice.generate_calls(
        conn, cfg, sim_dt, n_calls, ctx, queues, customers, agents)
    chats_n, chat_conversions = chat.generate_chats(
        conn, cfg, sim_dt, n_chats, ctx, queues, customers, agents)

    # tickets: independent volume + phone follow-ups + chat conversions
    total_tickets = n_tickets + call_followups + chat_conversions
    created, advanced = tickets.generate_tickets(
        conn, cfg, sim_dt, total_tickets, ctx, queues, customers)

    surveys_sent, surveys_resp = survey.generate_surveys(conn, cfg, sim_dt, ctx)

    detractors = survey.detractor_agents_recent(conn, cfg, sim_dt) \
        if sim_dt.hour in (9, 15) else []
    training.generate_training(conn, cfg, sim_dt, ctx,
                               caches['courses'], agents, detractors)

    return created, calls_n, chats_n, surveys_resp


# ---------------------------------------------------------------------------
# Realtime tick
# ---------------------------------------------------------------------------

def run_tick(conn, cfg, state, sim_dt, caches):
    scenario_names = get_active_scenario_names(conn, sim_dt)
    ctx = get_scenario_context(scenario_names, float(state['volume_multiplier']),
                               sim_dt, cfg)
    with conn.cursor() as cur:
        cur.execute("UPDATE control.generator_state SET active_scenario = %s WHERE state_id = 1",
                    (ctx.scenario_tag[:50],))

    tick_start = time.monotonic()
    created, calls_n, chats_n, surveys_n = generate_for_dt(conn, cfg, sim_dt, ctx, caches)

    # Probabilistic HR churn + new customers each simulated day
    if sim_dt.hour == 0:
        cust_model.new_customers_today(conn, cfg, sim_dt)
        # Give any agents hired since the last sweep their onboarding due date
        training.assign_missing_onboarding(conn, cfg, sim_dt)
    hr.maybe_hire_employee(conn, cfg, caches['locations'],
                           [q['code'] for q in caches['queues']])
    hr.maybe_terminate_employee(conn)
    tickets.maybe_reopen_tickets(conn, cfg, sim_dt, caches['queues'])

    elapsed_ms = round((time.monotonic() - tick_start) * 1000)
    record_stats(conn, created, calls_n, chats_n, surveys_n,
                 ctx.scenario_tag, sim_dt, elapsed_ms)
    log.info("Tick done %dms | Tickets: %d | Calls: %d | Chats: %d | Surveys: %d | Scenario: %s",
             elapsed_ms, created, calls_n, chats_n, surveys_n, ctx.scenario_tag)


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

def run_backfill(conn, cfg, state, caches):
    start = state['backfill_start_date']
    end = state['backfill_end_date']
    current = state['backfill_current_date'] or start
    today = date.today()

    log.info("Starting support backfill %s → %s (resuming %s)", start, end, current)

    cur_date = current
    while cur_date <= end:
        now_dt = datetime.now()
        today = date.today()

        skip_day = False
        start_hour = 0
        if cur_date != today:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT max(created_dt)::timestamp FROM support.tickets
                    WHERE created_dt::date = %s
                """, (cur_date,))
                last = cur.fetchone()[0]
            if last is not None:
                if last.hour >= 22:
                    log.info("Skipping %s — complete (last ticket %s)", cur_date, last)
                    skip_day = True
                else:
                    start_hour = min(last.hour + 1, 23)
                    log.info("%s partial (last %s) — resuming from hour %d",
                             cur_date, last, start_hour)

        if skip_day:
            _advance_pointer(conn, cur_date + timedelta(days=1))
            cur_date += timedelta(days=1)
            continue

        is_partial = (cur_date == today)
        end_hour = now_dt.hour if is_partial else 23
        log.info("Backfilling %s%s (hours %d–%d)", cur_date,
                 " [partial]" if is_partial else "", start_hour, end_hour)

        cust_model.new_customers_today(conn, cfg,
                                       datetime(cur_date.year, cur_date.month, cur_date.day, 1))

        for hour in range(start_hour, end_hour + 1):
            if is_partial and hour == end_hour:
                sim_dt = datetime.now()
            else:
                sim_dt = datetime(cur_date.year, cur_date.month, cur_date.day, hour)
            names = get_active_scenario_names(conn, sim_dt)
            ctx = get_scenario_context(names, float(state['volume_multiplier']),
                                       sim_dt, cfg)
            created, calls_n, chats_n, surveys_n = generate_for_dt(
                conn, cfg, sim_dt, ctx, caches)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO control.generation_stats
                        (tickets_generated, calls_generated, chat_sessions_generated,
                         surveys_generated, scenario_tag, simulation_dt, wall_clock_ms)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                """, (created, calls_n, chats_n, surveys_n, ctx.scenario_tag,
                      sim_dt, 0))
            conn.commit()

        _advance_pointer(conn, cur_date + timedelta(days=1))
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


def _advance_pointer(conn, next_date):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE control.generator_state
            SET backfill_current_date = %s, updated_at = NOW() WHERE state_id = 1
        """, (next_date,))
    conn.commit()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def refresh_caches(conn, queues, courses):
    return {
        'queues': queues,
        'courses': courses,
        'locations': hr.fetch_locations(conn),
        'agents': hr.fetch_active_employees(conn),
        'customers': cust_model.fetch_customer_ids(conn),
    }


def main():
    cfg = load_config()
    wait_for_db(cfg)
    bootstrap_database(cfg)

    conn = get_connection(cfg)
    psycopg2.extras.register_uuid()

    queues, locations, employees, courses = seed_all(conn, cfg)
    auto_backfill_if_fresh(conn, cfg)
    log.info("Generator ready. Entering main loop.")

    REFRESH_EVERY = 20
    tick_count = 0
    caches = refresh_caches(conn, queues, courses)

    while True:
        try:
            cfg = reload_config(cfg)
            state = read_state(conn)

            if state['mode'] == 'stopped' or not state['is_running'] or state['is_paused']:
                time.sleep(state['tick_interval_seconds'])
                continue

            if state['mode'] == 'backfill':
                caches = refresh_caches(conn, queues, courses)
                run_backfill(conn, cfg, state, caches)
                continue

            run_tick(conn, cfg, state, datetime.now(), caches)

            tick_count += 1
            if tick_count % REFRESH_EVERY == 0:
                caches = refresh_caches(conn, queues, courses)

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
