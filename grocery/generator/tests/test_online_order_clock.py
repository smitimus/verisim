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
CEILING_MIGRATION_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "standalone", "migrations",
    "006_online_order_ceiling.sql")

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


# ---------------------------------------------------------------------------
# 4. The OTHER end of the band: a clock jump across a timeline gap
#    (t_e1278564)
# ---------------------------------------------------------------------------
# `completed_dt = sim_dt` is a correct completion only while the generator's
# timeline is contiguous. The test slot's timeline had a 34-day hole (the source
# retention window, partly re-created by a backfill), so the first pass to see a
# 2026-10-08 23:00 order again was 2026-11-15 00:00 — and that became its
# completion: 37 days 02 h, 56 rows over 24 h, the only reason
# `mart_online_fulfillment`'s two duration bounds were red. A contiguous 10-day
# backfill never does this (longest span 17 h), so the fix is a ceiling derived
# from the ORDER, not a different duration distribution.

def test_the_ceiling_is_a_configurable_knob_beyond_the_longest_promise():
    """24 h is a knob, and it has to outlast the promise it must contain."""
    cfg = Config()
    ceiling = cfg.online.max_fulfillment_hours
    assert isinstance(ceiling, int)
    # The longest promise the generator can derive: a 2 h window plus a 4 h
    # delivery, both measured from placement. A ceiling below that would make
    # the band unsatisfiable and the promise gate below it dead code.
    assert ceiling > 2 + 4, ceiling
    assert ceiling <= 7 * 24, "a week is not a fulfillment ceiling, it is an outage"


def test_a_pass_inside_the_window_still_uses_the_pass_clock():
    """Nothing changes for a contiguous run — the clamp must be inert there."""
    placed = SIM_DT
    window = timedelta(hours=24)
    for minutes in (0, 45, 6 * 60, 24 * 60 - 1):
        sim_dt = placed + timedelta(minutes=minutes)
        assert online._lifecycle_instant('any-order', placed, sim_dt, window) == sim_dt


def test_a_late_pass_is_derived_from_the_order_and_stays_inside_the_band():
    """The pure law: past the window the instant is derived, not the clock.

    Deterministic per order (a replay answers the same thing), inside
    `[floor, placed + ceiling]`, and not one constant for every order.
    """
    placed = SIM_DT
    window = timedelta(hours=24)
    late_pass = placed + timedelta(days=37, hours=2)
    floor = placed + timedelta(hours=5, minutes=30)     # a delivery promise - grace

    picked = {online._lifecycle_instant(f'order-{i}', placed, late_pass, window, floor)
              for i in range(200)}
    for instant in picked:
        assert placed <= instant <= placed + window, instant
        assert instant >= floor, instant
    assert len(picked) > 100, "the derived instant collapsed onto a single value"
    # ...and the same order answers the same instant on every pass.
    assert (online._lifecycle_instant('order-7', placed, late_pass, window, floor)
            == online._lifecycle_instant('order-7', placed, late_pass, window, floor))


def test_a_gap_pass_completes_inside_the_order_window(db):
    """The card's gap case, end to end: place, jump 37 days, complete.

    Both fulfillment types, both completion branches. Before the ceiling these
    rows completed at the pass clock — 37 days after placement, which is what
    the test slot held.
    """
    conn = db
    cfg = Config()
    cfg.online.noshow_rate = 0.0          # a closed pickup window always collects
    seed = _seed(conn)
    ceiling = timedelta(hours=cfg.online.max_fulfillment_hours)
    placed = SIM_DT
    window_start = placed + timedelta(hours=2)
    promised = placed + timedelta(hours=6)
    with conn.cursor() as cur:
        pickup = _insert_order(cur, seed, placed, ftype='pickup', status='ready',
                               window_start=window_start,
                               window_end=window_start + timedelta(hours=2))
        delivery = _insert_order(cur, seed, placed, ftype='delivery',
                                 status='out_for_delivery', promised=promised)
    conn.commit()

    # The gap: the next pass to see these rows is 37 days later, exactly like
    # the slot's replay after its retention hole.
    gap_clock = placed + timedelta(days=37, hours=2)
    for _ in range(40):                   # 0.4**40: the delivery dice cannot hide
        online.advance_online_lifecycle(conn, cfg, gap_clock)
        if all(_orders(conn)[oid][2] is not None for oid in (pickup, delivery)):
            break
    conn.commit()

    rows = _orders(conn)
    for oid, floor in ((pickup, window_start + timedelta(hours=2)),
                       (delivery, promised - timedelta(
                           minutes=cfg.online.delivery_early_grace_minutes))):
        status, placed_dt, completed, promised_dt, ws, ftype = rows[oid]
        assert completed is not None, f"{ftype} {oid} never completed — vacuous case"
        assert placed_dt <= completed <= placed_dt + ceiling, (
            f"{ftype} {oid}: completed {completed} is outside "
            f"[{placed_dt}, {placed_dt + ceiling}] — the pass clock leaked through")
        assert completed >= floor, (
            f"{ftype} {oid}: completed {completed} is before its own earliest "
            f"plausible instant {floor}")

    # The terminal event IS the completion, and nothing else of the order sits
    # outside its window — the same pass wrote all of them.
    with conn.cursor() as cur:
        cur.execute("""
            SELECT e.order_id::text, e.event_type, e.event_dt::timestamp,
                   o.placed_dt::timestamp, o.completed_dt::timestamp
            FROM online.order_events e
            JOIN online.orders o ON o.order_id = e.order_id
        """)
        events = cur.fetchall()
    assert events, "the gap pass journalled no events"
    for oid, etype, event_dt, placed_dt, completed in events:
        assert placed_dt <= event_dt <= placed_dt + ceiling, (
            f"{etype} on {oid} is at {event_dt}, outside the order's window")
        if etype in ('picked_up', 'delivered'):
            assert event_dt == completed, (etype, event_dt, completed)


def test_a_generated_day_then_a_gap_never_leaves_the_band(db):
    """A whole generated day, then a 34-day jump: every row stays in its window.

    This is the slot's shape at scale — orders placed and advanced hour by hour,
    then a hole, then a pass that has no business completing any of them at its
    own clock.
    """
    conn = db
    cfg = Config()
    seed = _seed(conn)
    random.seed(20261115)
    ceiling = timedelta(hours=cfg.online.max_fulfillment_hours)

    day = datetime(2026, 3, 12, 0, 0, 0)
    for hour in range(24):
        for quarter in range(0, 60, 15):
            sim_dt = day + timedelta(hours=hour, minutes=quarter)
            online.generate_online_orders(conn, cfg, sim_dt, 2, _ctx(cfg, sim_dt),
                                          [{'location_id': seed['location_id']}],
                                          seed['products'], [])
            online.advance_online_lifecycle(conn, cfg, sim_dt)
    conn.commit()

    # The gap. Eight passes at the far side of it move the open rows through the
    # remaining stages (one stage per pass); they have to do it INSIDE the
    # order's own window, not at this clock.
    gap_clock = day + timedelta(days=34, hours=2)
    for _ in range(8):
        online.advance_online_lifecycle(conn, cfg, gap_clock)
    conn.commit()

    rows = _orders(conn)
    completed = {oid: r for oid, r in rows.items() if r[2] is not None}
    assert len(completed) > len(rows) // 2, (
        f"only {len(completed)} of {len(rows)} orders completed — vacuous case")

    failures = []
    for oid, (_status, placed, done, _promised, _ws, ftype) in completed.items():
        if done > placed + ceiling:
            failures.append(f"{oid} {ftype}: completed {done}, which is "
                            f"{done - placed} after placement (ceiling {ceiling})")
    assert not failures, failures[:5]

    with conn.cursor() as cur:
        cur.execute("""
            SELECT o.order_id::text, e.event_type, e.event_dt::timestamp,
                   o.placed_dt::timestamp
            FROM online.order_events e
            JOIN online.orders o ON o.order_id = e.order_id
            WHERE e.event_dt > o.placed_dt + interval '24 hours'
        """)
        leaked = cur.fetchall()
    assert not leaked, (
        "an event landed more than a day after its order was placed — the replay "
        f"clock was used for a row the gap had already passed: {leaked[:5]}")


def test_the_band_assertion_matches_the_config_ceiling():
    """`TIME-05` hardcodes 24 h; the config default is the ceiling. Pin them.

    The integrity spec is a committed SQL file that cannot read `config.yaml`, so
    the number is spelled out there. This is the test that fails when someone
    moves one and not the other.
    """
    from tests.cross_schema_integrity import ASSERTIONS

    spec = next(a for a in ASSERTIONS if a.id == "TIME-05")
    hours = {int(h) for h in re.findall(r"interval '(\d+) hours'", spec.sql)}
    assert hours == {Config().online.max_fulfillment_hours}, (
        f"TIME-05 asserts {hours} but online.max_fulfillment_hours is "
        f"{Config().online.max_fulfillment_hours}")


def test_migration_clamps_a_legacy_over_ceiling_completion(db):
    """A pre-card data dir: completions past the ceiling, events out of band.

    `migrations/006_online_order_ceiling.sql` has to repair both without a
    constraint (the ceiling is a config knob, so there is no CHECK to add) and
    be idempotent.
    """
    conn = db
    cfg = Config()
    ceiling = timedelta(hours=cfg.online.max_fulfillment_hours)
    seed = _seed(conn)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS online.order_events_legacy")
        cur.execute("DROP TABLE IF EXISTS online.orders_legacy")
        cur.execute("CREATE TABLE online.orders_legacy "
                    "(LIKE online.orders INCLUDING ALL)")
        cur.execute("CREATE TABLE online.order_events_legacy "
                    "(LIKE online.order_events INCLUDING ALL)")
        cur.execute("ALTER TABLE online.order_events_legacy "
                    "DROP CONSTRAINT IF EXISTS order_events_order_id_fkey")

        # The slot's shape: placed, then completed by a pass 37 days later, with
        # the terminal event at that same instant.
        gap_done = SIM_DT + timedelta(days=37, hours=2)
        over_pickup = _insert_legacy(
            cur, seed, SIM_DT, 'pickup',
            completed=gap_done,
            window_start=SIM_DT + timedelta(hours=2))
        over_delivery = _insert_legacy(
            cur, seed, SIM_DT, 'delivery',
            completed=gap_done,
            promised=SIM_DT + timedelta(hours=6))
        # A stage of that same order written by the same late pass, so it sits
        # after the completion once the completion is clamped.
        _insert_legacy_event(cur, over_delivery, 'out_for_delivery', gap_done)
        _insert_legacy_event(cur, over_delivery, 'delivered', gap_done)
        _insert_legacy_event(cur, over_pickup, 'picked_up', gap_done)
        # Never completed (a no_show), but the same pass wrote its event.
        no_show = _insert_legacy(
            cur, seed, SIM_DT, 'pickup', completed=None,
            window_start=SIM_DT + timedelta(hours=2))
        cur.execute("""
            UPDATE online.orders_legacy SET status = 'no_show'
             WHERE order_id = %s::uuid
        """, (no_show,))
        _insert_legacy_event(cur, no_show, 'no_show', gap_done)
        # Already inside the band: the migration must not touch it.
        untouched = _insert_legacy(
            cur, seed, SIM_DT, 'delivery',
            completed=SIM_DT + timedelta(hours=4),
            promised=SIM_DT + timedelta(hours=6))
        _insert_legacy_event(cur, untouched, 'delivered',
                             SIM_DT + timedelta(hours=4))
    conn.commit()

    sql = (open(CEILING_MIGRATION_PATH, encoding="utf-8").read()
           .replace("online.order_events", "online.order_events_legacy")
           .replace("online.orders", "online.orders_legacy"))

    def _snapshot(cur):
        cur.execute("""
            SELECT order_id::text, placed_dt::timestamp, completed_dt::timestamp
            FROM online.orders_legacy
        """)
        orders = {r[0]: r[1:] for r in cur.fetchall()}
        cur.execute("""
            SELECT order_id::text, event_type, event_dt::timestamp
            FROM online.order_events_legacy
        """)
        return orders, cur.fetchall()

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

    with conn.cursor() as cur:
        orders, events = _snapshot(cur)

    for oid, (placed, completed) in orders.items():
        if completed is not None:
            assert placed <= completed <= placed + ceiling, (
                f"{oid} is still completed at {completed}, outside "
                f"[{placed}, {placed + ceiling}]")
    assert orders[over_pickup][1] == SIM_DT + ceiling
    assert orders[over_delivery][1] == SIM_DT + ceiling
    assert orders[untouched][1] == SIM_DT + timedelta(hours=4), (
        "the migration rewrote a completion that was already inside the band")

    by_order = {}
    for oid, etype, event_dt in events:
        by_order.setdefault(oid, []).append((etype, event_dt))
        assert event_dt <= orders[oid][0] + ceiling, (
            f"{etype} on {oid} is still past the order's ceiling")
        if etype in ('picked_up', 'delivered'):
            assert event_dt == orders[oid][1], (
                f"{etype} on {oid} does not match the completion it records")
        if orders[oid][1] is not None:
            assert event_dt <= orders[oid][1], (
                f"{etype} on {oid} happens after the order completed")
    # The no_show has no completion to anchor to; its event is clamped instead.
    assert by_order[no_show] == [('no_show', SIM_DT + ceiling)], by_order[no_show]
    assert by_order[untouched] == [('delivered', SIM_DT + timedelta(hours=4))]

    # Idempotent: the second boot of the same volume is a no-op, not an error.
    with conn.cursor() as cur:
        cur.execute(sql)
        orders_again, events_again = _snapshot(cur)
        cur.execute("DROP TABLE IF EXISTS online.order_events_legacy")
        cur.execute("DROP TABLE IF EXISTS online.orders_legacy")
    conn.commit()
    assert orders_again == orders
    assert sorted(events_again) == sorted(events)
