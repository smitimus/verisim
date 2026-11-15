"""
`fulfillment.orders` must carry a real lifecycle — kanban t_5a408514.

THE DEFECT
----------
Every generated fulfillment was the literal 'packed' with
`started_at = completed_at = sim_dt` (16,658/16,658 rows on dev). Six marts
published `hours_to_fulfill` 0 on 6,849/6,849 orders, `fulfillment_status`
constant on 100 % of them, and `hours_to_pick_start` as low as -642 h because
`created_at` was the INSERT wall clock while `started_at` was a simulated day
weeks in the past.

WHAT THIS FILE PINS
-------------------
1. The schedule law (`_pick_state`): every instant inside [approved_dt, sim_dt],
   always ordered, and the pick DURATION is a distribution within one window
   rather than one clamp value. The clamp case is the point — a 100-minute pass
   window gives every order the same 85-minute pick if the duration is clamped,
   which is the same degenerate `hours_to_fulfill` with a different constant.
2. The schedule is DERIVED from the row id, so two passes (and a restart) reach
   the same plan without storing one: the same id must yield the same pick.
3. The model, end to end against a live Postgres: a day-wide backfill pass
   writes ordered timestamps and `created_at` on the order's business clock;
   realtime passes walk one row received → picking → packed across ticks; the
   truck moves it to 'shipped' and the arrival to 'delivered'.
4. The DDL — the widened CHECK accepts the lifecycle values, still accepts the
   legacy 'packed' rows, and rejects an unknown status. `schema.sql` only
   reaches a first run, so a data dir that already exists gets the constraint
   from `standalone/migrations/003_fulfillment_lifecycle.sql`; that file is run
   here against a table in the shape every pre-card data dir has.

WHICH DATABASE
--------------
The model is production code: it writes `fulfillment.orders`, not
`someprefix_fulfillment.orders`. A schema-prefix sandbox (the shape
`test_supplier_schema.py` uses for raw DDL) therefore cannot drive it, so these
tests need a database of their own. `GROCERY_TEST_DB` must point at a THROWAWAY
database — the fixture creates the generator schemas in it and drops them at the
end:

    sudo docker exec <container> psql -U postgres -d postgres \\
        -c 'CREATE DATABASE vprobe_lifecycle OWNER verisim'
    GROCERY_TEST_DB=postgresql://verisim:***@127.0.0.1:5499/vprobe_lifecycle \\
        pytest grocery/generator/tests/test_fulfillment_lifecycle.py -v

It refuses — skips — any database that already holds `fulfillment.orders`,
because dropping the schemas at teardown would take a slot's data with it. CI
has no Postgres service for the generator job, so the whole database half skips
there, exactly as `test_supplier_schema.py` does.
"""
import os
import re
import sys
from datetime import datetime, timedelta

import pytest

# `models/` is the generator's own import root when it runs in the image
# (`from models import suppliers`), so this one has to call the real model —
# a re-implementation would pin the test, not the code.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from models import fulfillment, transport  # noqa: E402

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schema.sql")
MIGRATION_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "standalone", "migrations",
    "003_fulfillment_lifecycle.sql")

GENERATOR_SCHEMAS = tuple(sorted(set(re.findall(
    r"CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+(\w+)\s*;",
    open(SCHEMA_PATH, encoding="utf-8").read(), flags=re.IGNORECASE))))

# The constraint name this card introduces, and the auto-generated name every
# pre-card data dir carries.
LIFECYCLE_CHECK = "fulfillment_orders_status_lifecycle"
LEGACY_CHECK = "orders_status_check"

SIM_DT = datetime(2026, 3, 2, 23, 0, 0)
APPROVED_AT = SIM_DT - timedelta(minutes=90)


# ---------------------------------------------------------------------------
# The schedule law
# ---------------------------------------------------------------------------

def test_day_wide_pick_is_ordered_and_inside_the_pass():
    """approved_dt <= started_at < completed_at <= sim_dt, always."""
    for i in range(400):
        status, started, completed = fulfillment._pick_state(
            f"row-{i}", APPROVED_AT, SIM_DT, 20, day_wide_pass=True)
        assert status in ('packed', 'received'), status
        if status == 'received':
            assert started is None and completed is None
            continue
        assert APPROVED_AT <= started < completed <= SIM_DT


def test_day_wide_pick_duration_is_a_distribution_not_a_clamp():
    """The regression guard: one window must not produce one duration."""
    durations, delays = set(), set()
    for i in range(300):
        status, started, completed = fulfillment._pick_state(
            f"row-{i}", APPROVED_AT, SIM_DT, 20, day_wide_pass=True)
        if status != 'packed':
            continue
        durations.add(int((completed - started).total_seconds()))
        delays.add(int((started - APPROVED_AT).total_seconds()))
    assert len(durations) > 20, f"pick duration collapsed to {len(durations)} values"
    assert len(delays) > 20, f"pick start collapsed to {len(delays)} values"
    assert min(durations) >= 60, "a pick took under a minute"
    assert max(durations) <= fulfillment.MAX_PICK_MINUTES * 60


def test_pick_never_exceeds_the_shift_ceiling():
    """A row that waited a day must not be given a 20-hour pick."""
    approved = SIM_DT - timedelta(days=1)
    for i in range(200):
        status, started, completed = fulfillment._pick_state(
            f"row-{i}", approved, SIM_DT, 400, day_wide_pass=True)
        if status == 'packed':
            assert (completed - started) <= timedelta(minutes=fulfillment.MAX_PICK_MINUTES)


def test_the_schedule_is_derived_and_reproducible():
    """Same row id, same plan — every pass, every restart, no stored column."""
    for i in range(50):
        fid = f"stable-{i}"
        first = fulfillment._pick_state(fid, APPROVED_AT, SIM_DT, 15, day_wide_pass=True)
        second = fulfillment._pick_state(fid, APPROVED_AT, SIM_DT, 15, day_wide_pass=True)
        assert first == second
        assert fulfillment._pick_schedule(fid, 15) == fulfillment._pick_schedule(fid, 15)


def test_realtime_passes_walk_received_picking_packed():
    """A pass is an instant in realtime, so the dwells are real states."""
    fid = None
    for i in range(200):
        candidate = f"dwell-{i}"
        delay, duration, _share, queued = fulfillment._pick_schedule(candidate, 12)
        if not queued and duration > 20:
            fid, start_delay, pick_minutes = candidate, delay, duration
            break
    assert fid, "no non-queued row with a usable schedule in 200 draws"

    approved = SIM_DT
    # Before the pick is due: the row is on the dock.
    status, started, completed = fulfillment._pick_state(
        fid, approved, approved + timedelta(minutes=start_delay - 1), 12,
        day_wide_pass=False)
    assert status == 'received' and started is None and completed is None

    # Mid-pick: it has started, and it is not finished.
    mid = approved + timedelta(minutes=start_delay + pick_minutes / 2)
    status, started, completed = fulfillment._pick_state(
        fid, approved, mid, 12, day_wide_pass=False)
    assert status == 'picking' and started is not None and completed is None

    # After the pick: complete, and inside the pass.
    after = approved + timedelta(minutes=start_delay + pick_minutes + 1)
    status, started2, completed2 = fulfillment._pick_state(
        fid, approved, after, 12, day_wide_pass=False)
    assert status == 'packed'
    assert completed2 is not None and completed2 <= after
    assert started2 < completed2

    # A later pass does not move the instants it already recorded.
    status, started3, completed3 = fulfillment._pick_state(
        fid, approved, after + timedelta(hours=3), 12,
        day_wide_pass=False, started_at=started2)
    assert started3 == started2 and completed3 == completed2


def test_a_queued_order_outlives_a_day_wide_pass():
    """The 10 % queue is what stops a fresh backfill collapsing to two states."""
    queued = [i for i in range(400)
              if fulfillment._pick_schedule(f"q-{i}", 10)[3]]
    assert len(queued) > 10, "no queued rows in 400 draws"
    for i in queued[:10]:
        status, started, completed = fulfillment._pick_state(
            f"q-{i}", APPROVED_AT, SIM_DT, 10, day_wide_pass=True)
        assert status == 'received', "a queued order was picked the same pass"


def test_rates_stay_small_enough_to_keep_the_pass_moving():
    assert 0.0 < fulfillment.PICK_BACKLOG_RATE < 0.30


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
    # No transaction open: the fixture below toggles autocommit to apply the
    # DDL, and psycopg2 refuses that mid-transaction.
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('fulfillment.orders')")
        if cur.fetchone()[0] is not None:
            conn.close()
            return None
    conn.autocommit = False
    return conn


# The module-scoped connection, for the autouse rollback fixture. A holder
# rather than a fixture dependency: the schedule tests must run with no database
# at all (CI has none), and an autouse fixture depending on `db` would skip them.
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

    A test that asserts a CHECK rejects a row leaves the connection aborted;
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
    """Empty the tables this module writes, so tests do not inherit rows.

    TRUNCATE ... CASCADE, not a list of DELETEs: the generator's tables hang off
    `hr.locations`, `pos.products` and each other in a dozen FKs, and a
    hand-ordered delete list is a test that breaks the next time a card adds a
    child table.
    """
    cur.execute("""
        TRUNCATE hr.locations, hr.employees, transport.trucks,
                 pos.departments, pos.products,
                 ordering.store_orders, fulfillment.orders, transport.loads,
                 inv.receipts
        RESTART IDENTITY CASCADE
    """)


def _seed(conn, orders=12, approved_ago=timedelta(hours=3),
          order_ago=timedelta(hours=5), qty=40):
    """One store, one DC, a picker, a manager, a SKU and `orders` store orders."""
    with conn.cursor() as cur:
        _wipe(cur)
        cur.execute("""
            INSERT INTO hr.locations
                (name, address, city, state, zip, opened_date, location_type,
                 latitude, longitude)
            VALUES ('Life Store', '1 A', 'C', 'NY', '00000', CURRENT_DATE,
                    'store', 40.10, -74.10),
                   ('Life DC', '2 B', 'C', 'NY', '00000', CURRENT_DATE,
                    'warehouse', 40.60, -74.60)
            RETURNING location_id, location_type
        """)
        locs = {kind: str(loc) for loc, kind in cur.fetchall()}

        cur.execute("""
            INSERT INTO hr.employees
                (location_id, first_name, last_name, email, hire_date,
                 department, job_title, hourly_rate)
            VALUES (%s, 'Pat', 'Picker', 'pat.picker@lifecycle.test',
                    CURRENT_DATE, 'warehouse', 'Picker', 20.00),
                   (%s, 'Mo', 'Manager', 'mo.manager@lifecycle.test',
                    CURRENT_DATE, 'management', 'Manager', 30.00)
            RETURNING employee_id, department
        """, (locs['warehouse'], locs['store']))
        emps = {dept: str(emp) for emp, dept in cur.fetchall()}

        cur.execute("""
            INSERT INTO pos.departments (name, code)
            VALUES ('Lifecycle Grocery', 'LGR') RETURNING department_id
        """)
        dept = str(cur.fetchone()[0])
        cur.execute("""
            INSERT INTO pos.products
                (sku, name, department_id, category, cost, current_price)
            VALUES ('LIFE-001', 'Lifecycle SKU', %s, 'grocery', 1.00, 2.00)
            RETURNING product_id
        """, (dept,))
        product = str(cur.fetchone()[0])

        order_ids = []
        for _ in range(orders):
            cur.execute("""
                INSERT INTO ordering.store_orders
                    (store_location_id, warehouse_location_id, order_dt,
                     approved_dt, status)
                VALUES (%s, %s, %s, %s, 'approved')
                RETURNING order_id
            """, (locs['store'], locs['warehouse'],
                  SIM_DT - order_ago, SIM_DT - approved_ago))
            order_id = str(cur.fetchone()[0])
            order_ids.append(order_id)
            cur.execute("""
                INSERT INTO ordering.store_order_items
                    (order_id, product_id, quantity_requested, quantity_approved)
                VALUES (%s, %s, %s, %s)
            """, (order_id, product, qty, qty))
    conn.commit()
    return {
        'store': locs['store'], 'warehouse': locs['warehouse'],
        'picker': emps['warehouse'], 'manager': emps['management'],
        'product': product, 'orders': order_ids,
    }


def _pickers(seed):
    return [{'employee_id': seed['picker'], 'department': 'warehouse',
             'location_type': 'warehouse'}]


def _trucks(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO transport.trucks
                (license_plate, capacity_pallets, is_active)
            VALUES ('LIFE-1', 24, TRUE)
            RETURNING truck_id
        """)
        truck = str(cur.fetchone()[0])
    conn.commit()
    return [{'truck_id': truck, 'license_plate': 'LIFE-1', 'capacity': 24}]


def _counts(cur):
    cur.execute("SELECT status, count(*) FROM fulfillment.orders GROUP BY 1")
    return dict(cur.fetchall())


def _rows(cur):
    """fulfillment_id -> (status, started_at, completed_at, created_at, approved_dt)."""
    cur.execute("""
        SELECT fo.fulfillment_id::text, fo.status, fo.started_at::timestamp,
               fo.completed_at::timestamp, fo.created_at::timestamp,
               so.approved_dt::timestamp
        FROM fulfillment.orders fo
        JOIN ordering.store_orders so ON so.order_id = fo.store_order_id
    """)
    return {r[0]: r[1:] for r in cur.fetchall()}


# ---------------------------------------------------------------------------
# The model, end to end
# ---------------------------------------------------------------------------

def test_day_wide_pass_writes_a_real_timeline(db):
    """A backfill pass: ordered instants, real durations, business created_at."""
    conn = db
    seed = _seed(conn, orders=24)
    trucks = _trucks(conn)

    fulfilled = fulfillment.process_pending_orders(conn, _pickers(seed), SIM_DT,
                                                   day_wide_pass=True)
    assert fulfilled, "the pass dispatched nothing"

    with conn.cursor() as cur:
        rows = _rows(cur)
        counts = {}
        for status, started, completed, created_at, approved_dt in rows.values():
            counts[status] = counts.get(status, 0) + 1

        # Every row is on the order's own business clock, not the wall clock.
        for fid, (status, started, completed, created_at, approved_dt) in rows.items():
            assert created_at == approved_dt, (fid, created_at, approved_dt)
            assert approved_dt <= SIM_DT

        # The queued share is what keeps a fresh backfill from collapsing to
        # 'shipped' + 'delivered': it must be exactly the rows whose schedule
        # says the dock could not reach them.
        queued = sum(1 for fid in rows
                     if fulfillment._pick_schedule(fid, 1)[3])
        assert counts.get('received', 0) == queued, (counts, queued)

        # Timelines, and variation in them.
        cur.execute("""
            SELECT count(*) FROM fulfillment.orders
            WHERE started_at IS NOT NULL
              AND (started_at >= completed_at OR completed_at > %s)
        """, (SIM_DT,))
        assert cur.fetchone()[0] == 0, "a timeline is unordered or in the future"

        cur.execute("""
            SELECT count(DISTINCT extract(epoch FROM (completed_at - started_at))),
                   min(extract(epoch FROM (completed_at - started_at))),
                   max(extract(epoch FROM (completed_at - started_at)))
            FROM fulfillment.orders WHERE started_at IS NOT NULL
        """)
        distinct, lo, hi = cur.fetchone()
        assert distinct > 1, f"hours_to_fulfill is constant: {distinct} value(s)"
        assert lo >= 60, f"a pick took {lo}s"
        assert hi <= fulfillment.MAX_PICK_MINUTES * 60, hi

        # `hours_to_pick_start` — the column that read -642 h — is a wait, not
        # a negative age.
        cur.execute("""
            SELECT min(extract(epoch FROM (started_at - created_at)))
            FROM fulfillment.orders WHERE started_at IS NOT NULL
        """)
        assert cur.fetchone()[0] >= 0

    # Everything dispatched is on a load and 'shipped'.
    transport.dispatch_loads(conn, fulfilled, trucks, [], seed['warehouse'], SIM_DT)
    with conn.cursor() as cur:
        assert _counts(cur).get('shipped', 0) == len(fulfilled)

    # The next day does not lose the queued rows: they are picked, loaded, and
    # the loads carry the rest to 'delivered'.
    tomorrow = SIM_DT + timedelta(days=1)
    fulfillment.process_pending_orders(conn, _pickers(seed), tomorrow,
                                       day_wide_pass=True)
    with conn.cursor() as cur:
        # The queued orders are picked on the next pass, not dropped: a row
        # left in 'received' would never reach a receipt and would starve the
        # shelf it was ordered for.
        assert _counts(cur).get('received', 0) == 0, _counts(cur)
        # Anything still open has no timestamps, so it cannot pollute the
        # duration columns with a half-recorded pick.
        cur.execute("""
            SELECT count(*) FROM fulfillment.orders
            WHERE status = 'received'
              AND (started_at IS NOT NULL OR completed_at IS NOT NULL)
        """)
        assert cur.fetchone()[0] == 0

    transport.receive_delivered_loads(conn, tomorrow + timedelta(days=1),
                                      vendor_cfg=None)
    with conn.cursor() as cur:
        assert _counts(cur).get('delivered', 0) == len(fulfilled), _counts(cur)
        cur.execute("SELECT count(*) FROM transport.loads WHERE status <> 'delivered'")
        assert cur.fetchone()[0] == 0
        # A delivered fulfillment has a load behind it — the status was not
        # just flipped by an UPDATE that never carried goods.
        cur.execute("""
            SELECT count(*) FROM fulfillment.orders fo
            WHERE fo.status = 'delivered'
              AND NOT EXISTS (SELECT 1 FROM transport.load_items li
                               WHERE li.fulfillment_id = fo.fulfillment_id)
        """)
        assert cur.fetchone()[0] == 0


def test_realtime_passes_carry_one_row_through_the_machine(db):
    """A realtime tick is an instant: the states are dwells, not stamps."""
    conn = db
    seed = _seed(conn, orders=3, approved_ago=timedelta(minutes=20),
                 order_ago=timedelta(hours=3))
    trucks = _trucks(conn)

    first = fulfillment.process_pending_orders(conn, _pickers(seed), SIM_DT,
                                               day_wide_pass=False)
    with conn.cursor() as cur:
        rows = _rows(cur)
    assert len(rows) == 3

    # Pick one row and walk the clock to its own schedule: whatever state the
    # first pass wrote, the machine must reach 'packed' exactly at the end of
    # the pick, and 'picking' in between.
    fid = next(iter(rows))
    delay, duration, _share, queued = fulfillment._pick_schedule(fid, 1)
    approved_dt = rows[fid][4]
    due = approved_dt + timedelta(minutes=delay)
    done = due + timedelta(minutes=duration)

    if not queued and delay >= (SIM_DT - approved_dt).total_seconds() / 60.0:
        assert rows[fid][0] == 'received', rows[fid]

    mid = due + timedelta(minutes=max(1.0, duration / 2))
    fulfillment.process_pending_orders(conn, _pickers(seed), mid, day_wide_pass=False)
    with conn.cursor() as cur:
        rows = _rows(cur)
    status, started, completed = rows[fid][0], rows[fid][1], rows[fid][2]
    expect = fulfillment._pick_state(fid, approved_dt, mid, 1, day_wide_pass=False)
    assert (status, started, completed) == expect
    if status == 'picking':
        assert started is not None and completed is None
        assert started == due

    later = done + timedelta(minutes=1)
    fulfillment.process_pending_orders(conn, _pickers(seed), later, day_wide_pass=False)
    with conn.cursor() as cur:
        rows = _rows(cur)
    status, started, completed = rows[fid][0], rows[fid][1], rows[fid][2]
    assert status in ('packed', 'shipped'), status
    if status == 'packed':
        assert completed is not None and started < completed

    # Dispatch whatever is packed, and the row that reached a truck is shipped.
    shipped = fulfillment.process_pending_orders(conn, _pickers(seed),
                                                 later + timedelta(minutes=1),
                                                 day_wide_pass=False)
    if shipped:
        transport.dispatch_loads(conn, shipped, trucks, [], seed['warehouse'],
                                 later + timedelta(minutes=1))
        with conn.cursor() as cur:
            assert _counts(cur).get('shipped', 0) == len(shipped)


# ---------------------------------------------------------------------------
# DDL: the values, and the migration that carries them to an existing data dir
# ---------------------------------------------------------------------------

def test_lifecycle_values_are_accepted_and_legacy_packed_still_loads(db):
    """The CHECK admits the machine and rejects anything outside it."""
    conn = db
    with conn.cursor() as cur:
        _wipe(cur)
        cur.execute("""
            INSERT INTO hr.locations
                (name, address, city, state, zip, opened_date, location_type)
            VALUES ('Ddl Store', '1 A', 'C', 'NY', '00000', CURRENT_DATE, 'store'),
                   ('Ddl DC', '2 B', 'C', 'NY', '00000', CURRENT_DATE, 'warehouse')
            RETURNING location_id, location_type
        """)
        locs = {kind: str(loc) for loc, kind in cur.fetchall()}
        cur.execute("""
            INSERT INTO ordering.store_orders
                (store_location_id, warehouse_location_id, status)
            VALUES (%s, %s, 'picking') RETURNING order_id
        """, (locs['store'], locs['warehouse']))
        order = str(cur.fetchone()[0])

        for status in ('pending', 'received', 'picked', 'picking', 'packed',
                       'loaded', 'shipped', 'delivered', 'cancelled'):
            cur.execute("""
                INSERT INTO fulfillment.orders
                    (store_order_id, warehouse_location_id, status)
                VALUES (%s, %s, %s)
            """, (order, locs['warehouse'], status))

        with pytest.raises(Exception):
            cur.execute("""
                INSERT INTO fulfillment.orders
                    (store_order_id, warehouse_location_id, status)
                VALUES (%s, %s, 'teleported')
            """, (order, locs['warehouse']))
    conn.rollback()


def test_migration_widens_a_legacy_status_check(db):
    """The boot migration turns an old data dir's CHECK into the lifecycle one."""
    conn = db
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS fulfillment.orders_legacy")
        cur.execute("CREATE TABLE fulfillment.orders_legacy "
                    "(LIKE fulfillment.orders INCLUDING ALL)")
        # The shape every pre-card dir has: the auto-named inline CHECK.
        cur.execute(f"ALTER TABLE fulfillment.orders_legacy DROP CONSTRAINT {LIFECYCLE_CHECK}")
        cur.execute(f"""
            ALTER TABLE fulfillment.orders_legacy ADD CONSTRAINT {LEGACY_CHECK}
            CHECK (status IN ('pending','picking','packed','loaded','cancelled'))
        """)
        # A row in the old shape must survive the migration untouched.
        cur.execute("""
            INSERT INTO fulfillment.orders_legacy
                (store_order_id, warehouse_location_id, status)
            SELECT store_order_id, warehouse_location_id, 'packed'
            FROM fulfillment.orders LIMIT 1
        """)
    conn.commit()

    sql = (open(MIGRATION_PATH, encoding="utf-8").read()
           .replace("fulfillment.orders", "fulfillment.orders_legacy"))
    with conn.cursor() as cur:
        cur.execute(sql)

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fulfillment.orders_legacy WHERE status = 'packed'")
        assert cur.fetchone()[0] == 1, "the migration lost a legacy 'packed' row"

        cur.execute("SELECT count(*) FROM pg_constraint "
                    "WHERE conrelid = 'fulfillment.orders_legacy'::regclass "
                    f"  AND conname = '{LIFECYCLE_CHECK}'")
        assert cur.fetchone()[0] == 1

        cur.execute("SELECT count(*) FROM pg_constraint "
                    "WHERE conrelid = 'fulfillment.orders_legacy'::regclass "
                    f"  AND conname = '{LEGACY_CHECK}'")
        assert cur.fetchone()[0] == 0, "the old CHECK survived and still rejects the machine"

        cur.execute("UPDATE fulfillment.orders_legacy SET status = 'shipped' "
                    "WHERE status = 'packed'")
        cur.execute("UPDATE fulfillment.orders_legacy SET status = 'delivered' "
                    "WHERE status = 'shipped'")
        with pytest.raises(Exception):
            cur.execute("UPDATE fulfillment.orders_legacy SET status = 'teleported'")
    conn.rollback()

    # Idempotent: the second boot of the same volume is a no-op.
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("DROP TABLE IF EXISTS fulfillment.orders_legacy")
    conn.commit()
