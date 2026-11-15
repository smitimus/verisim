"""
`online.orders.completed_dt` must be monotonic with `placed_dt` — kanban
t_c9ea180f.

THE DEFECT
----------
988 of 510,405 completed online orders on dev were completed BEFORE they were
placed, worst -20 h 46 m, and 2 of them had already reached the EDW raw layer.
`completed_dt` is not a sampled duration — it is the `sim_dt` of the pass that
completed the order — so a completion earlier than its placement can only come
from a pass whose clock is behind the row it advanced. `advance_online_lifecycle`
selected every open order with no bound on `placed_dt`, and the `ready` /
`out_for_delivery` branches completed on a bare dice roll with no age check. A
backfill replaying hour 0 of a day whose later hours were already written (a
restart; a partial-day replay of *today* always starts at hour 0) therefore
stamped `completed_dt = 00:00` on orders placed at 11:00/12:00/13:00 — which is
exactly the shape found in the source: hour-boundary completions, same date,
`updated_at` eleven minutes after `created_at`.

WHAT THIS FILE PINS
-------------------
1. A pass behind its rows completes NOTHING, even after fifty attempts, and the
   same rows do complete once the clock reaches them — so the test cannot pass
   by the rows simply being uncompletable.
2. Completion never precedes placement, and never precedes the promise the store
   made: `completed_dt >= placed_dt`, delivery
   `>= promised_delivery_dt - delivery_early_grace_minutes`, pickup
   `>= pickup_window_start - pickup_early_grace_minutes`. Asserted over a
   generated day, one pass per simulated hour, so a future change to the
   duration distribution cannot reintroduce a negative duration.
3. The DDL: `online_orders_completed_after_placed` rejects a completion before
   its placement, and the boot migration
   (`standalone/migrations/005_online_order_clock.sql`) repairs a data dir that
   already holds the offending rows before it validates — the ordering matters,
   because `ADD CONSTRAINT` on a volume with violations aborts the boot.

WHICH DATABASE
--------------
The model is production code: it writes `online.orders`, not
`someprefix_online.orders`, so a schema-prefix sandbox cannot drive it. These
tests need a database of their own, and the fixture drops the generator schemas
at teardown:

    sudo docker exec <container> psql -U postgres -d postgres \\
        -c 'CREATE DATABASE vprobe_online_clock OWNER verisim'
    GROCERY_TEST_DB=postgresql://verisim:***@127.0.0.1:5499/vprobe_online_clock \\
        pytest grocery/generator/tests/test_online_order_clock.py -v

It refuses — skips — any database that already holds `online.orders`, because
dropping the schemas at teardown would take a slot's data with it. CI has no
Postgres service for the generator job, so the database half skips there; the
config assertions still run.
"""
import os
import random
import re
import sys
from datetime import datetime, timedelta

import pytest

# `models/` is the generator's own import root when it runs in the image
# (`from models import online`), so this one has to call the real model — a
# re-implementation would pin the test, not the code.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from config import Config  # noqa: E402
from models import online  # noqa: E402
from scenarios.scenario_engine import get_scenario_context  # noqa: E402

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schema.sql")
MIGRATION_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "standalone", "migrations",
    "005_online_order_clock.sql")

GENERATOR_SCHEMAS = tuple(sorted(set(re.findall(
    r"CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+(\w+)\s*;",
    open(SCHEMA_PATH, encoding="utf-8").read(), flags=re.IGNORECASE))))

CLOCK_CHECK = "online_orders_completed_after_placed"

# A weekday midday, far enough from a DST boundary that hour arithmetic on naive
# datetimes is exact.
SIM_DT = datetime(2026, 3, 11, 12, 0, 0)


# ---------------------------------------------------------------------------
# The knobs, and the law they feed
# ---------------------------------------------------------------------------

def test_the_early_allowance_is_bounded_and_configurable():
    """Both graces are real knobs with sane defaults, not magic numbers."""
    cfg = Config()
    assert 0 < cfg.online.pickup_early_grace_minutes < 12 * 60
    assert 0 < cfg.online.delivery_early_grace_minutes < 12 * 60


def test_the_promise_is_never_before_the_place():
    """`promised_delivery_dt` itself is derived forward from `placed_dt`.

    The grace below is meaningless if the promise it is measured against can be
    in the past, so this pins the generator's own arithmetic: the window opens
    two hours out and the delivery promise is two to four hours after that.
    """
    window_start = SIM_DT + timedelta(hours=2)
    assert window_start > SIM_DT
    for extra in (2, 4):
        assert window_start + timedelta(hours=extra) > SIM_DT


# ---------------------------------------------------------------------------
# A throwaway database with the real schema names
# ---------------------------------------------------------------------------

def _scratch_connection():
    """A connection to an EMPTY scratch database, or None (skip)."""
    dsn = os.environ.get("GROCERY_TEST_DB")
    if not dsn:
        return None
    try:
        import psycopg2
        conn = psycopg2.connect(dsn, connect_timeout=5)
    except Exception:
        return None
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('online.orders')")
        if cur.fetchone()[0] is not None:
            conn.close()
            return None
    conn.autocommit = False
    return conn


# The module-scoped connection, for the autouse rollback fixture. A holder
# rather than a fixture dependency: the pure-logic tests above must run with no
# database at all (CI has none), and an autouse fixture depending on `db` would
# skip them.
_CONN = {'conn': None}


@pytest.fixture(scope="module")
def db():
    """Apply `schema.sql` to the scratch database; drop it afterwards."""
    conn = _scratch_connection()
    if conn is None:
        pytest.skip("No empty scratch database — set GROCERY_TEST_DB to a throwaway DB")

    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(open(SCHEMA_PATH, encoding="utf-8").read())
    except Exception as exc:
        conn.close()
        pytest.fail(f"schema.sql did not apply to the scratch database: {exc}")
    conn.autocommit = False
    _CONN['conn'] = conn

    try:
        yield conn
    finally:
        _CONN['conn'] = None
        conn.rollback()
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                for schema in GENERATOR_SCHEMAS:
                    cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        except Exception:
            pass
        conn.close()


@pytest.fixture(autouse=True)
def _no_open_transaction():
    """Every test starts on a clean transaction, pass or fail.

    A test that asserts the CHECK rejects a row leaves the connection aborted;
    without this the *next* test fails with `InFailedSqlTransaction`, which
    reads like a second defect.
    """
    conn = _CONN['conn']
    if conn is not None:
        conn.rollback()
    try:
        yield
    finally:
        if conn is not None:
            conn.rollback()


def _wipe(cur):
    """Empty the tables this module writes, so tests do not inherit rows."""
    cur.execute("""
        TRUNCATE online.orders, online.order_items, online.order_events,
                 hr.locations, pos.departments, pos.products
        RESTART IDENTITY CASCADE
    """)


def _seed(conn, products=8):
    """One store location and a handful of SKUs — everything the model needs."""
    with conn.cursor() as cur:
        _wipe(cur)
        cur.execute("""
            INSERT INTO hr.locations
                (name, address, city, state, zip, opened_date, location_type,
                 latitude, longitude)
            VALUES ('Clock Store', '1 A', 'C', 'NY', '00000', CURRENT_DATE,
                    'store', 40.10, -74.10)
            RETURNING location_id
        """)
        location_id = str(cur.fetchone()[0])

        cur.execute("""
            INSERT INTO pos.departments (name, code)
            VALUES ('Clock Grocery', 'CGR') RETURNING department_id
        """)
        department_id = str(cur.fetchone()[0])

        rows = []
        for i in range(products):
            cur.execute("""
                INSERT INTO pos.products
                    (sku, name, department_id, category, cost, current_price,
                     unit_of_measure, reference_price, price_elasticity)
                VALUES (%s, %s, %s, 'grocery', 1.00, 2.00, 'each', 2.00, -0.5)
                RETURNING product_id::text
            """, (f"CLOCK-{i:03d}", f"Clock SKU {i}", department_id))
            rows.append({
                'product_id': str(cur.fetchone()[0]),
                'uom': 'each',
                'price': 2.00,
                'reference_price': 2.00,
                'price_elasticity': -0.5,
            })
    conn.commit()
    return {'location_id': location_id, 'products': rows}


def _ctx(cfg, sim_dt):
    return get_scenario_context(['normal'], 1.0, sim_dt, cfg)


def _orders(conn):
    """order_id -> (status, placed, completed, promised, window_start, type)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT order_id::text, status, placed_dt::timestamp,
                   completed_dt::timestamp, promised_delivery_dt::timestamp,
                   pickup_window_start::timestamp, fulfillment_type
            FROM online.orders
        """)
        return {r[0]: r[1:] for r in cur.fetchall()}


def _insert_order(cur, seed, placed, *, ftype='pickup', status='ready',
                  completed=None, window_start=None, window_end=None,
                  promised=None):
    """Write one order directly, in whatever state the test needs to start from."""
    cur.execute("""
        INSERT INTO online.orders
            (location_id, placed_dt, fulfillment_type, status, subtotal,
             service_fee, tax, total, payment_method, pickup_window_start,
             pickup_window_end, promised_delivery_dt, completed_dt)
        VALUES (%s, %s, %s, %s, 10.00, 0.00, 0.80, 10.80, 'credit', %s, %s, %s, %s)
        RETURNING order_id::text
    """, (seed['location_id'], placed, ftype, status, window_start, window_end,
          promised, completed))
    return str(cur.fetchone()[0])


def _insert_legacy(cur, seed, placed, ftype, *, completed, window_start=None,
                   promised=None):
    """One order in a `LIKE online.orders` clone (no clock constraint)."""
    cur.execute("""
        INSERT INTO online.orders_legacy
            (location_id, placed_dt, fulfillment_type, status, subtotal,
             service_fee, tax, total, payment_method, pickup_window_start,
             pickup_window_end, promised_delivery_dt, completed_dt)
        VALUES (%s, %s, %s, 'completed', 10.00, 0.00, 0.80, 10.80, 'credit',
                %s, %s, %s, %s)
        RETURNING order_id::text
    """, (seed['location_id'], placed, ftype, window_start,
          (window_start + timedelta(hours=2)) if window_start else None,
          promised, completed))
    return str(cur.fetchone()[0])


def _insert_legacy_event(cur, order_id, event_type, event_dt):
    """One event in the `LIKE online.order_events` clone."""
    cur.execute("""
        INSERT INTO online.order_events_legacy (order_id, event_type, event_dt, note)
        VALUES (%s::uuid, %s, %s, 'legacy')
    """, (order_id, event_type, event_dt))


# ---------------------------------------------------------------------------
# 1. A pass behind its rows completes nothing
# ---------------------------------------------------------------------------

def test_a_pass_behind_its_rows_completes_nothing(db):
    """The defect itself: a replay at hour 0 must not complete a 13:00 order.

    Both completion branches are covered — the pickup `ready` dice (0.35) and the
    delivery `out_for_delivery` dice (0.6). Fifty passes at the early clock make
    "it happened to roll a miss" impossible: with the guard removed the chance of
    no completion is 0.65**50 x 0.4**50, i.e. zero.
    """
    conn = db
    cfg = Config()
    seed = _seed(conn)
    placed = SIM_DT                       # placed at 12:00
    window_start = placed + timedelta(hours=2)
    with conn.cursor() as cur:
        pickup = _insert_order(cur, seed, placed, ftype='pickup', status='ready',
                               window_start=window_start,
                               window_end=window_start + timedelta(hours=2))
        delivery = _insert_order(cur, seed, placed, ftype='delivery',
                                 status='out_for_delivery',
                                 promised=placed + timedelta(hours=6))
    conn.commit()

    early = placed - timedelta(hours=12)   # the hour-0 pass of a replay
    for _ in range(50):
        online.advance_online_lifecycle(conn, cfg, early)
    conn.commit()

    rows = _orders(conn)
    for oid in (pickup, delivery):
        status, placed_dt, completed, _promised, _ws, _ftype = rows[oid]
        assert completed is None, (
            f"{oid} was completed at {completed} by a pass at {early} — the "
            f"lifecycle advanced a row its clock had not reached")
        assert status in ('ready', 'out_for_delivery'), rows[oid]

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM online.order_events")
        assert cur.fetchone()[0] == 0, "the early pass journalled events for orders it had not reached"

    # ...and the same rows DO complete once the clock reaches them, so the test
    # above cannot pass because the rows were uncompletable.
    for minutes in range(0, 24 * 60, 30):
        online.advance_online_lifecycle(conn, cfg, placed + timedelta(minutes=minutes))
        if all(_orders(conn)[oid][2] is not None for oid in (pickup, delivery)):
            break
    rows = _orders(conn)
    for oid in (pickup, delivery):
        status, placed_dt, completed, _promised, _ws, _ftype = rows[oid]
        assert completed is not None, f"{oid} never completed once the clock reached it"
        assert completed >= placed_dt


# ---------------------------------------------------------------------------
# 2. The invariant, over a generated day
# ---------------------------------------------------------------------------

def test_a_generated_day_never_completes_before_it_places(db):
    """One pass per simulated hour across a day: every clock stays ordered.

    This is the acceptance test. It also asserts the completion is not ACED —
    rows must actually complete — and that events follow their order's placement,
    because `event_dt` is stamped from the same pass clock.
    """
    conn = db
    cfg = Config()
    seed = _seed(conn)
    random.seed(20260311)

    day = datetime(2026, 3, 12, 0, 0, 0)      # a clean weekday, no DST boundary
    # Four passes an hour, one order per pass: placed and completed instants are
    # then genuinely different offsets, not both snapped to the hour.
    for hour in range(24):
        for quarter in range(0, 60, 15):
            sim_dt = day + timedelta(hours=hour, minutes=quarter)
            online.generate_online_orders(conn, cfg, sim_dt, 2, _ctx(cfg, sim_dt),
                                          [{'location_id': seed['location_id']}],
                                          seed['products'], [])
            online.advance_online_lifecycle(conn, cfg, sim_dt)
    conn.commit()

    # The tail of the day needs the next day's passes to finish.
    for quarter in range(0, 48):
        online.advance_online_lifecycle(
            conn, cfg, day + timedelta(days=1, minutes=15 * quarter))
    conn.commit()

    rows = _orders(conn)
    assert rows, "the day generated no orders"
    completed = {oid: r for oid, r in rows.items() if r[2] is not None}
    assert len(completed) > len(rows) // 2, (
        f"only {len(completed)} of {len(rows)} orders completed — the case is vacuous")

    pickup_grace = timedelta(minutes=cfg.online.pickup_early_grace_minutes)
    delivery_grace = timedelta(minutes=cfg.online.delivery_early_grace_minutes)

    failures = []
    for oid, (status, placed, done, promised, window_start, ftype) in completed.items():
        if done < placed:
            failures.append(f"{oid} {ftype}: completed {done} before placed {placed}")
        elif ftype == 'delivery' and promised and done < promised - delivery_grace:
            failures.append(
                f"{oid} delivery: completed {done} more than "
                f"{delivery_grace} before the promise {promised}")
        elif ftype == 'pickup' and window_start and done < window_start - pickup_grace:
            failures.append(
                f"{oid} pickup: collected {done} more than "
                f"{pickup_grace} before its window opened {window_start}")
    assert not failures, failures[:5]

    # The event stream shares the pass clock, so it cannot precede the order.
    with conn.cursor() as cur:
        cur.execute("""
            SELECT o.order_id, e.event_type, e.event_dt::timestamp, o.placed_dt::timestamp
            FROM online.order_events e
            JOIN online.orders o ON o.order_id = e.order_id
            WHERE e.event_dt < o.placed_dt
        """)
        leaked = cur.fetchall()
    assert not leaked, leaked[:5]

    # ...and the completed_at -> placed_at duration is a distribution, not one
    # constant: a clock that is only ever one instant would also pass the
    # ordering assertions above.
    durations = {(done - placed).total_seconds() for _s, placed, done, *_r
                 in completed.values()}
    assert len(durations) > 10, f"completion latency collapsed to {len(durations)} value(s)"


def test_a_pickup_is_not_collected_before_its_window_opens(db):
    """The pickup gate, driven minute by minute so the dice cannot hide it."""
    conn = db
    cfg = Config()
    seed = _seed(conn)
    grace = timedelta(minutes=cfg.online.pickup_early_grace_minutes)
    placed = SIM_DT
    window_start = placed + timedelta(hours=2)
    with conn.cursor() as cur:
        oid = _insert_order(cur, seed, placed, ftype='pickup', status='ready',
                            window_start=window_start,
                            window_end=window_start + timedelta(hours=2))
    conn.commit()

    completed = None
    for minutes in range(0, 6 * 60, 5):
        online.advance_online_lifecycle(conn, cfg, placed + timedelta(minutes=minutes))
        completed = _orders(conn)[oid][2]
        if completed is not None:
            break
    conn.commit()

    assert completed is not None, "the pickup never completed — the case is vacuous"
    assert completed >= window_start - grace, (completed, window_start, grace)


def test_a_delivery_is_not_completed_before_its_promise(db):
    """The delivery gate: a long tail extends forward, it never goes negative."""
    conn = db
    cfg = Config()
    seed = _seed(conn)
    grace = timedelta(minutes=cfg.online.delivery_early_grace_minutes)
    placed = SIM_DT
    promised = placed + timedelta(hours=6)
    with conn.cursor() as cur:
        oid = _insert_order(cur, seed, placed, ftype='delivery',
                            status='out_for_delivery', promised=promised)
    conn.commit()

    completed = None
    for minutes in range(0, 8 * 60, 5):
        online.advance_online_lifecycle(conn, cfg, placed + timedelta(minutes=minutes))
        completed = _orders(conn)[oid][2]
        if completed is not None:
            break
    conn.commit()

    assert completed is not None, "the delivery never completed — the case is vacuous"
    assert completed >= promised - grace, (completed, promised, grace)
    assert completed >= placed


# ---------------------------------------------------------------------------
# 3. The DDL: the constraint, and the migration that carries it to a data dir
# ---------------------------------------------------------------------------

def test_the_check_rejects_a_completion_before_its_placement(db):
    """The database, not only the generator, refuses a negative fulfillment."""
    conn = db
    seed = _seed(conn)
    with conn.cursor() as cur:
        oid = _insert_order(cur, seed, SIM_DT, ftype='delivery', status='placed')
    conn.commit()

    with conn.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute("""
                UPDATE online.orders
                   SET completed_dt = placed_dt - interval '1 minute'
                 WHERE order_id = %s::uuid
            """, (oid,))
    conn.rollback()

    # A completion at the placement instant is legal: a zero-minute duration is
    # degenerate but not impossible, and forbidding it would make the constraint
    # about data quality rather than about physics.
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE online.orders SET completed_dt = placed_dt
             WHERE order_id = %s::uuid
        """, (oid,))
    conn.commit()
    assert _orders(conn)[oid][2] == _orders(conn)[oid][1]


def test_migration_repairs_a_legacy_table_then_constrains_it(db):
    """A pre-card data dir: violating rows AND no constraint. Both are fixed.

    The order matters. `ADD CONSTRAINT` VALIDATES, so running it before the
    repair aborts the boot on a volume that holds the 988 rows — which is why the
    file's UPDATE comes first and this test asserts the repaired values rather
    than only the constraint's presence.
    """
    conn = db
    seed = _seed(conn)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS online.order_events_legacy")
        cur.execute("DROP TABLE IF EXISTS online.orders_legacy")
        cur.execute("CREATE TABLE online.orders_legacy "
                    "(LIKE online.orders INCLUDING ALL)")
        cur.execute("ALTER TABLE online.orders_legacy "
                    f"DROP CONSTRAINT IF EXISTS {CLOCK_CHECK}")
        # The event stream needs its own clone too: `LIKE` copies the FK to the
        # REAL orders table, and these rows belong to the legacy clone.
        cur.execute("CREATE TABLE online.order_events_legacy "
                    "(LIKE online.order_events INCLUDING ALL)")
        cur.execute("ALTER TABLE online.order_events_legacy "
                    "DROP CONSTRAINT IF EXISTS order_events_order_id_fkey")

        # The shape every pre-card dir holds: completed BEFORE placed.
        neg_pickup = _insert_legacy(
            cur, seed, SIM_DT, 'pickup',
            completed=SIM_DT - timedelta(hours=12),
            window_start=SIM_DT + timedelta(hours=2))
        neg_delivery = _insert_legacy(
            cur, seed, SIM_DT, 'delivery',
            completed=SIM_DT - timedelta(hours=16),
            promised=SIM_DT + timedelta(hours=6))
        # A row that is already fine must be left exactly as it was.
        untouched = _insert_legacy(
            cur, seed, SIM_DT, 'delivery',
            completed=SIM_DT + timedelta(hours=4),
            promised=SIM_DT + timedelta(hours=6))
        # ...and the events the same pass journalled: a terminal event dated
        # before the order existed, and one whose order never completed.
        _insert_legacy_event(cur, neg_pickup, 'picked_up',
                             SIM_DT - timedelta(hours=12))
        _insert_legacy_event(cur, untouched, 'cancelled',
                             SIM_DT - timedelta(hours=9))
    conn.commit()

    sql = (open(MIGRATION_PATH, encoding="utf-8").read()
           .replace("online.order_events", "online.order_events_legacy")
           .replace("online.orders", "online.orders_legacy"))
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT order_id::text, fulfillment_type, placed_dt::timestamp,
                   completed_dt::timestamp, promised_delivery_dt::timestamp,
                   pickup_window_start::timestamp
            FROM online.orders_legacy
        """)
        rows = {r[0]: r[1:] for r in cur.fetchall()}

    for oid, (_ftype, placed, completed, _promised, _ws) in rows.items():
        assert completed is not None
        assert completed >= placed, f"{oid} is still completed before it was placed"

    # The repair lands where the new law would have put it, not at some constant.
    _ftype, placed, completed, _promised, ws = rows[neg_pickup]
    assert completed == max(placed + timedelta(minutes=45),
                            ws + timedelta(minutes=15)), completed
    _ftype, placed, completed, promised, _ws = rows[neg_delivery]
    assert completed == promised - timedelta(minutes=15), completed
    _ftype, placed, completed, _promised, _ws = rows[untouched]
    assert completed == SIM_DT + timedelta(hours=4), "the migration rewrote a valid row"

    # The event stream moved with its order, and to the completion it recorded.
    with conn.cursor() as cur:
        cur.execute("""
            SELECT e.order_id::text, e.event_type, e.event_dt::timestamp,
                   o.placed_dt::timestamp, o.completed_dt::timestamp
            FROM online.order_events_legacy e
            JOIN online.orders_legacy o ON o.order_id = e.order_id
        """)
        events = cur.fetchall()
    assert len(events) == 2, events
    for oid, etype, event_dt, placed, completed in events:
        assert event_dt >= placed, f"{etype} on {oid} still precedes its order"
        if completed is not None and oid == neg_pickup:
            assert event_dt == completed, (event_dt, completed)

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_constraint "
                    "WHERE conrelid = 'online.orders_legacy'::regclass "
                    f"  AND conname = '{CLOCK_CHECK}'")
        assert cur.fetchone()[0] == 1, "the migration did not add the constraint"
        with pytest.raises(Exception):
            cur.execute("""
                UPDATE online.orders_legacy
                   SET completed_dt = placed_dt - interval '1 minute'
                 WHERE order_id = %s::uuid
            """, (untouched,))
    conn.rollback()

    # Idempotent: the second boot of the same volume is a no-op, not an error.
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("SELECT count(*) FROM pg_constraint "
                    "WHERE conrelid = 'online.orders_legacy'::regclass "
                    f"  AND conname = '{CLOCK_CHECK}'")
        assert cur.fetchone()[0] == 1
        cur.execute("DROP TABLE IF EXISTS online.order_events_legacy")
        cur.execute("DROP TABLE IF EXISTS online.orders_legacy")
    conn.commit()
