"""
Database bootstrap, connection handling, and the control-state ledger.

Everything the generator needs before it can generate: create the database and
apply `schema.sql` if the target is empty (`bootstrap_database`), open and
retry a connection (`get_connection`, `wait_for_db`), and read/write the single
`control.generator_state` row the loop polls (`read_state`, `record_stats`).

Split out of `generator/main.py` (t_c2eca5dd). `main.py` re-exports every name,
so `main.bootstrap_database(...)` and the tests importing
`grocery.generator.main.has_data_for_date` are unaffected.
"""
import logging
import os
import time

import psycopg2
import psycopg2.extras

log = logging.getLogger('grocery-generator')


# ---------------------------------------------------------------------------
# DB bootstrap, connection, control state
# ---------------------------------------------------------------------------

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

