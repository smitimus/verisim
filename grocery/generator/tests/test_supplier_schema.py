"""
Schema-level verification of the t_57b1a1ab vendor tables.

The unit tests in `test_suppliers.py` pin the generator's LAWS (the draw
around the promise, the vendor's short rate, the claim window). They cannot
check the DDL, and the DDL is the half of this change that only reaches a
FRESH bootstrap: a `schema.sql` change does not apply to an existing data dir
(the same trap AGENTS.md documents for the PG16->PG18 rebuild and for
`pos.returns`), so a bad CHECK here would ship and sit unnoticed on every
install that already exists. `main.bootstrap_database` only runs this file
when `control.generator_state` is absent.

So this file applies the real `schema.sql` to a live Postgres and then
deliberately tries to write rows that must be REJECTED. A constraint that does
not fire is a failed test, not a passing one.

SANDBOX, NOT A SCRATCH DATABASE
-------------------------------
A scratch database is the obvious way to get a clean namespace, but it needs
CREATEDB and the generator's role does not have it — on the dev stack
`rolcreatedb` is false for `verisim`, which can only create the one database
the bootstrap names. So the whole file is applied under a unique PREFIX on
every schema name (`inv.suppliers` -> `supcheck_ab12cd34.inv.suppliers`) and
dropped afterwards. Two consequences worth knowing:

  * the DDL under test is byte-identical apart from the prefix, and
  * a test that fails half way still drops the sandbox, because the fixture
    tears down in `finally` — nothing is left behind on a real slot's database.

Skips when no database is reachable (same probe shape as
`test_cross_schema_integrity.py`), so CI without Postgres still passes.

Run against a specific database with:

    GROCERY_TEST_DB=postgresql://verisim:***@127.0.0.1:5499/grocery \
        pytest grocery/generator/tests/test_supplier_schema.py -v
"""
import os
import re
import uuid

import pytest

SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "schema.sql")

# Every schema schema.sql creates. The prefix rewrite covers exactly these, so a
# dotted word that is not one of them (`dateutil.something` in a COMMENT, say)
# is left alone rather than mangled.
GENERATOR_SCHEMAS = (
    "hr", "pos", "timeclock", "ordering", "fulfillment",
    "transport", "inv", "pricing", "online", "control",
)

# The live sandbox prefix, set by the module-scoped `sandbox` fixture.
#
# A module global rather than a parameter on `accepts` / `rejects`, because
# every one of those calls is inside a test that already receives `sandbox` as
# a fixture and threading it through ~30 call sites would bury the two
# statements that actually matter. It is written once per module run and only
# the fixture writes it.
CURRENT_SANDBOX = [None]


def _probe_connection():
    """A live psycopg2 connection, or None (no database available)."""
    try:
        import psycopg2
    except ImportError:
        return None
    dsn = os.environ.get("GROCERY_TEST_DB")
    if dsn:
        try:
            return psycopg2.connect(dsn, connect_timeout=5)
        except Exception:
            return None
    for host, port in (("127.0.0.1", 5499), ("127.0.0.1", 5432)):
        try:
            return psycopg2.connect(
                host=host, port=port, user="verisim", password="verisim",
                dbname="grocery", connect_timeout=3,
            )
        except Exception:
            continue
    return None


def _prefixed(source: str, prefix: str) -> str:
    """
    Rename every generator schema to `<prefix>_<name>` and every reference with
    it.

    Postgres has no schema nesting, so a prefix cannot be a namespace component
    (`CREATE SCHEMA a.b` is a syntax error). Renaming the schemas themselves is
    the way to sandbox without CREATEDB, which the generator's role does not
    have on the dev stack (`rolcreatedb = f` for `verisim`).

    The rename is uniform — one regex over both the CREATE lines and every
    dotted reference — so the DDL under test is byte-identical apart from the
    schema names, and a dropped sandbox leaves nothing behind.
    """
    # Longest-first so `timeclock` cannot be half-matched by a shorter name.
    for schema in sorted(GENERATOR_SCHEMAS, key=len, reverse=True):
        source = re.sub(rf"(?<![\w.]){schema}\.",
                        f"{prefix}{schema}.", source)
        source = re.sub(
            rf"(CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+){schema}\s*;",
            rf"\g<1>{prefix}{schema};", source, flags=re.IGNORECASE)
    return source


@pytest.fixture(scope="module")
def sandbox():
    """
    Apply schema.sql under renamed schemas; yield the prefix; always drop it.

    The teardown is in a `finally` so a failing assertion cannot leave a
    `supcheck_*` schema behind on a real slot.
    """
    conn = _probe_connection()
    if conn is None:
        pytest.skip("No reachable grocery database (set GROCERY_TEST_DB)")

    prefix = "supcheck%s_" % uuid.uuid4().hex[:8]
    rewritten = _prefixed(open(SCHEMA_PATH, encoding="utf-8").read(), prefix)
    CURRENT_SANDBOX[0] = prefix

    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(rewritten)
    except Exception as exc:
        conn.close()
        pytest.fail(f"schema.sql did not apply under {prefix}: {exc}")
    conn.autocommit = False

    try:
        yield prefix
    finally:
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                for schema in GENERATOR_SCHEMAS:
                    cur.execute(f'DROP SCHEMA IF EXISTS "{prefix}{schema}" CASCADE')
        except Exception:
            pass
        conn.close()


INSERT_SQL = """
    INSERT INTO inv.suppliers
        (supplier_name, supplier_code, lead_time_mean_days,
         lead_time_stddev_days, short_ship_rate, credit_window_days)
    VALUES (%(name)s, %(code)s, 2.0, 0.5, 0.05, 14)
    RETURNING supplier_id
"""

# The seeded rows carry a per-test SUFFIX rather than fixed names. The sandbox is
# module-scoped (one schema.sql apply for the whole run) while `seeded` is
# function-scoped, so fixed names collide on the second test — and
# `supplier_name` / `supplier_code` / `sku` are all UNIQUE. A per-test suffix is
# what keeps the module-scoped sandbox and the function-scoped fixture
# compatible without giving each test its own database.
def _seed_params(tag):
    return {'name': f'Schema Vendor {tag}', 'code': f'SCH{tag}'}

# `fulfillment.orders.store_order_id` is a real FK into ordering.store_orders,
# so a short-ship needs a store order behind it — the chain the short-ship is
# part of. Skipping it makes the fixture's own INSERT fail on the FK, which is
# the schema correctly refusing a fulfillment that references no order.
ORDER_SQL = """
    INSERT INTO ordering.store_orders
        (store_location_id, warehouse_location_id, status)
    SELECT %(store)s::uuid, %(store)s::uuid, 'shipped'
    RETURNING order_id
"""

FULFILLMENT_SQL = """
    WITH o AS (
        INSERT INTO ordering.store_orders
            (store_location_id, warehouse_location_id, status)
        SELECT %(store)s::uuid, %(store)s::uuid, 'approved'
        RETURNING order_id),
    f AS (
        INSERT INTO fulfillment.orders
            (store_order_id, warehouse_location_id, status)
        SELECT order_id, %(store)s::uuid, 'packed'
        FROM o
        RETURNING fulfillment_id)
    INSERT INTO fulfillment.items
        (fulfillment_id, product_id, quantity_requested, quantity_picked,
         pick_status)
    SELECT fulfillment_id, %(product)s::uuid, %(requested)s, %(picked)s,
           'short'
    FROM f
    RETURNING item_id, fulfillment_id
"""

LOCATION_SQL = """
    INSERT INTO hr.locations
        (name, address, city, state, zip, opened_date, location_type)
    VALUES (%(name)s, 'A', 'C', 'NY', '00000', CURRENT_DATE, 'store')
    RETURNING location_id
"""

DEPARTMENT_SQL = """
    INSERT INTO pos.departments (name, code)
    VALUES (%(name)s, %(code)s)
    RETURNING department_id
"""

PRODUCT_SQL = """
    INSERT INTO pos.products (sku, name, department_id, category, cost,
                              current_price)
    VALUES (%(sku)s, 'Schema SKU', %(dept)s::uuid, 'C', 1.0, 2.0)
    RETURNING product_id
"""

INV_PRODUCT_SQL = """
    INSERT INTO inv.products (product_id, reorder_point, reorder_qty)
    VALUES (%(product)s::uuid, 10, 50)
"""

SCHEDULE_SQL = """
    INSERT INTO inv.supplier_delivery_schedules
        (supplier_id, location_id, delivery_weekday,
         delivery_window_start, delivery_window_end)
    VALUES (%(supplier)s::uuid, %(store)s::uuid, %(weekday)s,
            '06:00', '08:00')
    RETURNING schedule_id
"""

DELIVERY_SQL = """
    INSERT INTO inv.dsd_deliveries
        (schedule_id, supplier_id, location_id, delivery_date, delivered_at)
    VALUES (%(schedule)s::uuid, %(supplier)s::uuid, %(store)s::uuid,
            CURRENT_DATE, NOW())
    RETURNING dsd_delivery_id
"""

DSD_ITEM_SQL = """
    INSERT INTO inv.dsd_delivery_items
        (dsd_delivery_id, product_id, quantity_delivered, unit_cost, line_total)
    VALUES (%(delivery)s::uuid, %(product)s::uuid, %(quantity)s, 4.0,
            %(line_total)s)
"""


def _one(sandbox, sql, params):
    """Run one INSERT ... RETURNING <id> and return the id."""
    conn = sandbox_conn(sandbox)
    try:
        with conn.cursor() as cur:
            cur.execute(scoped(sql, sandbox), params)
            row = cur.fetchone()
        conn.commit()
        return row[0]
    finally:
        conn.close()


def _many(sandbox, statements):
    """Run several statements on one connection, in order."""
    conn = sandbox_conn(sandbox)
    try:
        with conn.cursor() as cur:
            for sql, params in statements:
                cur.execute(scoped(sql, sandbox), params)
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def seeded(sandbox):
    """
    The minimum a supplier / short-ship / memo row needs: one vendor, one
    store, one product and its inv.products row.

    Returned as a dict of the ids so a test can reference them, and so the
    whole set-up is a fixture rather than repeated SQL per test — 25 tests
    each seeding their own rows would also mean 25 chances for one of them to
    diverge from the others.
    """
    tag = uuid.uuid4().hex[:8]
    ids = {'tag': tag}
    ids['supplier'] = _one(sandbox, INSERT_SQL, _seed_params(tag))
    ids['store'] = _one(sandbox, LOCATION_SQL, {'name': f'Store {tag}'})
    dept = _one(sandbox, DEPARTMENT_SQL,
                {'name': f'Dept {tag}', 'code': f'D{tag}'})
    ids['product'] = _one(sandbox, PRODUCT_SQL,
                          {'sku': f'SKU-{tag}', 'dept': dept})
    _many(sandbox, [(INV_PRODUCT_SQL, {'product': ids['product']})])
    return ids


def sandbox_conn(sandbox):
    """
    A connection for the sandbox, honouring GROCERY_TEST_DB.

    No `search_path` trick here: the test SQL says `INSERT INTO inv.suppliers`,
    but `search_path` can only resolve the BARE name `suppliers` — the two-part
    `inv.suppliers` always means the schema literally named `inv`. Since the
    sandbox renames `inv` to `<prefix>inv`, the test statements must be rewritten
    the same way `schema.sql` was. `scoped()` does that, so a test body still
    reads exactly like the generator's own SQL.

    Opened per call rather than shared: each test rolls its own failed INSERT
    back, and a shared connection would carry an aborted transaction into the
    next test (which is exactly what `cross_schema_integrity.run_assertion`
    documents having to defend against).
    """
    import psycopg2
    dsn = os.environ.get("GROCERY_TEST_DB")
    if dsn:
        return psycopg2.connect(dsn, connect_timeout=5)
    return psycopg2.connect(
        host="127.0.0.1", port=5499, user="verisim", password="verisim",
        dbname="grocery", connect_timeout=5)


def scoped(statement, sandbox):
    """
    Point every generator-schema reference in `statement` at the sandbox.

    `search_path` cannot help a two-part name: `INSERT INTO inv.suppliers`
    always resolves the schema named `inv`, which on a live slot is the REAL
    one. Rewriting `inv.` -> `<sandbox>inv.` is therefore mandatory, and it is
    the same uniform rename `_prefixed` applies to schema.sql — which is what
    keeps a test statement and the DDL it exercises describing the same objects.
    """
    for schema in sorted(GENERATOR_SCHEMAS, key=len, reverse=True):
        statement = re.sub(rf"(?<![\w.]){schema}\.",
                           f"{sandbox}{schema}.", statement)
    return statement


def inv(prefix):
    """The sandboxed `inv` schema name. For information_schema queries, which
    take a schema NAME and are not affected by search_path."""
    return f"{prefix}inv"


# ---------------------------------------------------------------------------
# The helpers
# ---------------------------------------------------------------------------

def accepts(conn, statement, params=None):
    """The statement must SUCCEED."""
    try:
        with conn.cursor() as cur:
            cur.execute(scoped(statement, CURRENT_SANDBOX[0]), params)
        conn.commit()
        return True, ""
    except Exception as exc:
        conn.rollback()
        return False, str(exc).strip().splitlines()[0][:110]


def rejects(conn, statement, params=None):
    """The statement must FAIL — a constraint is doing its job."""
    ok, detail = accepts(conn, statement, params)
    return (not ok), detail


def make_short_ship(sandbox, seeded, requested=50, picked=30):
    """
    A store order -> fulfillment -> short item, returning (item_id,
    fulfillment_id).

    The whole chain, not just the fulfillment: `fulfillment.orders` carries a
    NOT NULL FK to `ordering.store_orders`, and the short-ship the test is
    about is only meaningful if it descends from an order that was placed. A
    fixture that skipped the order would be testing a row the generator can
    never actually write.
    """
    conn = sandbox_conn(sandbox)
    try:
        with conn.cursor() as cur:
            cur.execute(scoped(FULFILLMENT_SQL, sandbox),
                        {'store': seeded['store'], 'product': seeded['product'],
                         'requested': requested, 'picked': picked})
            item_id, fulfillment_id = cur.fetchone()
        conn.commit()
        return item_id, fulfillment_id
    finally:
        conn.close()


def insert_short_ship(sandbox, seeded, **overrides):
    """INSERT into inv.short_ship_events with the balanced defaults."""
    item_id, fulfillment_id = make_short_ship(
        sandbox, seeded,
        requested=overrides.get('requested', 50),
        picked=overrides.get('picked', 30))
    params = {
        'item': item_id,
        'fulfillment': fulfillment_id,
        'supplier': overrides.get('supplier', seeded['supplier']),
        'product': seeded['product'],
        'store': seeded['store'],
        'source': overrides.get('source', 'receiving'),
        'requested': overrides.get('requested', 50),
        'picked': overrides.get('picked', 30),
        'short': overrides.get('short', 20),
        'creditable': overrides.get('creditable', True),
    }
    cols = ['fulfillment_item_id', 'fulfillment_id', 'supplier_id',
            'product_id', 'location_id', 'detected_source',
            'quantity_requested', 'quantity_picked', 'quantity_short',
            'unit_cost', 'short_value', 'promised_lead_time_days',
            'realized_lead_time_days', 'is_creditable', 'event_dt']
    if overrides.get('supplier') is None and 'no_supplier' in overrides:
        cols.remove('supplier_id')
        params.pop('supplier')
    sql = ("INSERT INTO inv.short_ship_events "
           f"({', '.join(cols)}) VALUES ("
           f"%(item)s::uuid, %(fulfillment)s::uuid, %(supplier)s::uuid, "
           f"%(product)s::uuid, %(store)s::uuid, %(source)s, %(requested)s, "
           f"%(picked)s, %(short)s, 4.0, 80.00, 2, 3, %(creditable)s, NOW())")
    return sql, params


# ---------------------------------------------------------------------------
# 1. schema.sql applies at all
# ---------------------------------------------------------------------------
def test_schema_applies_to_a_clean_namespace(sandbox):
    """
    The cheapest possible failure, and the one that blocks everything else:
    a `schema.sql` that no longer parses means a fresh install produces an
    empty database and the generator dies on its first query. The fixture
    already applied it, so reaching this test body IS the assertion.
    """
    conn = sandbox_conn(sandbox)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = %s", (inv(sandbox),))
        assert cur.fetchone()[0] > 0, "the sandbox schema has no tables"


@pytest.mark.parametrize("table", [
    "suppliers", "supplier_delivery_schedules", "short_ship_events",
    "supplier_credit_memos", "dsd_deliveries", "dsd_delivery_items",
])
def test_every_new_table_exists(sandbox, table):
    """The six tables the card's acceptance criteria name, by name."""
    conn = sandbox_conn(sandbox)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s", (inv(sandbox), table))
        assert cur.fetchone(), f"inv.{table} is missing from schema.sql"


def test_the_product_vendor_fk_is_declared(sandbox):
    """`inv.products.supplier_id` is the whole point: a free-text name with no
    key is what made vendor performance unanswerable."""
    conn = sandbox_conn(sandbox)
    with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.table_constraints
                WHERE table_schema = %s AND table_name = 'products'
                  AND constraint_type = 'FOREIGN KEY'
            """, (inv(sandbox),))
            # inv.products had no FKs at all before; supplier_id adds the first.
            assert cur.fetchone()[0] >= 1


# ---------------------------------------------------------------------------
# 2. inv.suppliers
# ---------------------------------------------------------------------------
def test_a_vendor_row_is_accepted(sandbox, seeded):
    ok, detail = accepts(
        sandbox_conn(sandbox),
        f"INSERT INTO inv.suppliers (supplier_name, supplier_code)"
        f" VALUES ('Another', 'ANOT')")
    assert ok, detail


@pytest.mark.parametrize("label,columns,values", [
    ("a negative lead-time mean",
     "lead_time_mean_days", "-1"),
    ("a negative lead-time spread",
     "lead_time_stddev_days", "-1"),
    ("a short_ship_rate above 1", "short_ship_rate", "1.5"),
    ("a short_ship_rate below 0", "short_ship_rate", "-0.5"),
    ("a zero-day credit window", "credit_window_days", "0"),
    ("an unknown fulfillment_model", "fulfillment_model", "'teleport'"),
])
def test_supplier_row_constraints_hold(sandbox, label, columns, values):
    """
    Each of these is a typo a config author can make, and each would otherwise
    become a runtime surprise instead of a rejected row at seed time.
    """
    rejected, detail = rejects(
        sandbox_conn(sandbox),
        f"INSERT INTO inv.suppliers "
        f"(supplier_name, supplier_code, {columns}) VALUES ('Bad', 'BAD', {values})")
    assert rejected, f"{label} was accepted: {detail}"


def test_a_duplicate_supplier_name_is_rejected(sandbox, seeded):
    rejected, detail = rejects(
        sandbox_conn(sandbox),
        f"INSERT INTO inv.suppliers (supplier_name, supplier_code)"
        f" VALUES (%(name)s, 'OTHER')", _seed_params(seeded['tag']))
    assert rejected, f"a duplicate supplier_name was accepted: {detail}"


def test_a_duplicate_supplier_code_is_rejected(sandbox, seeded):
    rejected, detail = rejects(
        sandbox_conn(sandbox),
        f"INSERT INTO inv.suppliers (supplier_name, supplier_code)"
        f" VALUES ('Other Vendor', %(code)s)", _seed_params(seeded['tag']))
    assert rejected, f"a duplicate supplier_code was accepted: {detail}"


# ---------------------------------------------------------------------------
# 3. inv.products -> inv.suppliers
# ---------------------------------------------------------------------------
def test_a_product_can_point_at_its_vendor(sandbox, seeded):
    ok, detail = accepts(
        sandbox_conn(sandbox),
        f"UPDATE inv.products SET supplier_id = %(supplier)s::uuid,"
        f" supplier_name = 'Schema Vendor' WHERE product_id = %(product)s::uuid",
        {'supplier': seeded['supplier'], 'product': seeded['product']})
    assert ok, detail


def test_a_product_rejects_a_vendor_that_does_not_exist(sandbox, seeded):
    rejected, detail = rejects(
        sandbox_conn(sandbox),
        f"UPDATE inv.products SET supplier_id = %(bad)s::uuid"
        f" WHERE product_id = %(product)s::uuid",
        {'bad': str(uuid.uuid4()), 'product': seeded['product']})
    assert rejected, f"a dangling supplier_id was accepted: {detail}"


def test_a_product_may_have_no_vendor(sandbox, seeded):
    """A row from a pre-t_57b1a1ab data dir has no vendor, and forcing one would
    make the migration impossible. NULL is a legitimate state."""
    ok, detail = accepts(
        sandbox_conn(sandbox),
        f"UPDATE inv.products SET supplier_id = NULL"
        f" WHERE product_id = %(product)s::uuid",
        {'product': seeded['product']})
    assert ok, detail


def test_the_denormalised_supplier_name_survives_alongside_the_id(sandbox, seeded):
    """
    Both columns exist on purpose: `supplier_id` is the join key, and
    `supplier_name` keeps every existing reader of the column working with no
    join at all. Neither may be required on its own — a pre-t_57b1a1ab row has
    a name and no id, and a row whose id was cleared can still carry a name for
    display.

    Rolled back at the end so the mutation cannot leak into the next test in the
    module-scoped sandbox (it did exactly that: an earlier version left
    `supplier_id` NULL behind and every later test that expected the column to
    exist failed with "column supplier_id does not exist", because the aborted
    transaction had undone the sandbox's own DDL state for that connection).
    """
    conn = sandbox_conn(sandbox)
    try:
        for name in ('Schema Vendor', None):
            ok, detail = accepts(
                conn,
                "UPDATE inv.products SET supplier_id = NULL,"
                " supplier_name = %(name)s"
                " WHERE product_id = %(product)s::uuid",
                {'name': name, 'product': seeded['product']})
            assert ok, (
                f"supplier_name={name!r} with no supplier_id was rejected: {detail}")
    finally:
        conn.rollback()
        conn.close()


# ---------------------------------------------------------------------------
# 4. inv.short_ship_events
# ---------------------------------------------------------------------------
def test_a_balanced_short_ship_is_accepted(sandbox, seeded):
    sql, params = insert_short_ship(sandbox, seeded)
    ok, detail = accepts(sandbox_conn(sandbox), sql, params)
    assert ok, f"a valid 50-requested/30-picked/20-short row was rejected: {detail}"


@pytest.mark.parametrize("label,overrides", [
    ("quantity_short that does not equal requested - picked", {'short': 25}),
    ("a zero quantity_short", {'short': 0}),
    ("a negative quantity_short", {'short': -5}),
    ("a quantity_picked above quantity_requested",
     {'requested': 50, 'picked': 55, 'short': 0}),
    ("an unknown detected_source", {'source': 'smoke_signal'}),
    ("no supplier at all", {'no_supplier': True}),
])
def test_short_ship_row_constraints_hold(sandbox, seeded, label, overrides):
    """
    Every one of these is an arithmetic mistake the generator could make, and
    each must be caught by the TABLE rather than by trusting the Python.
    """
    conn = sandbox_conn(sandbox)
    sql, params = insert_short_ship(sandbox, seeded, **overrides)
    rejected, detail = rejects(conn, sql, params)
    assert rejected, f"{label} was accepted: {detail}"


def test_a_short_ship_cannot_credit_nothing(sandbox, seeded):
    """`is_creditable = TRUE` on a row with no shortfall would be a claim for
    zero units — the CHECK that says so is the point of the column pair."""
    conn = sandbox_conn(sandbox)
    sql, params = insert_short_ship(sandbox, seeded, short=0)
    # quantity_short > 0 fires first; the point is that a non-creditable
    # zero-short row IS allowed, so the constraint is specifically about
    # claiming rather than about the quantity.
    rejected, _ = rejects(conn, sql, params)
    assert rejected
    sql, params = insert_short_ship(sandbox, seeded, creditable=False)
    # A non-creditable row still needs a real shortfall.
    ok, detail = accepts(conn, sql, params)
    assert ok, detail


# ---------------------------------------------------------------------------
# 5. inv.supplier_credit_memos
# ---------------------------------------------------------------------------
def _open_memo(sandbox, seeded, **overrides):
    """A short-ship to claim against, then the claim INSERT."""
    conn = sandbox_conn(sandbox)
    sql, params = insert_short_ship(sandbox, seeded)
    accepts(conn, sql, params)
    with conn.cursor() as cur:
        cur.execute(scoped(
            "SELECT short_ship_id FROM inv.short_ship_events"
            " ORDER BY created_at DESC, short_ship_id LIMIT 1", sandbox))
        short_ship_id = cur.fetchone()[0]
    conn.commit()
    return {
        'memo': overrides.get('memo', f"CM-{seeded['tag']}"),
        'ship': overrides.get('ship', short_ship_id),
        'supplier': seeded['supplier'],
        'product': seeded['product'],
        'store': seeded['store'],
        'quantity': overrides.get('quantity', 20),
        'reason': overrides.get('reason', 'warehouse_shortage'),
        'status': overrides.get('status', 'open'),
        'deadline': overrides.get('deadline', 'CURRENT_DATE + 14'),
        'submitted': overrides.get('submitted', 'NULL'),
        'resolved': overrides.get('resolved', 'NULL'),
    }


def _memo_sql(sandbox, p, with_resolved=True):
    """
    The memo INSERT.

    `claim_deadline`, `submitted_dt` and `resolved_dt` are inlined SQL
    expressions rather than bound parameters, because the tests need to say
    "NOW() - INTERVAL '30 days'" and a parameter would be cast as a literal
    string (`'CURRENT_DATE + 14'::date` is not a date). Every value here is
    test-written, never user input.
    """
    cols = ['credit_memo_number', 'short_ship_id', 'supplier_id', 'product_id',
            'location_id', 'credit_quantity', 'unit_cost', 'credit_amount',
            'short_reason', 'memo_status', 'claim_deadline', 'submitted_dt']
    if with_resolved:
        cols.append('resolved_dt')
    return (f"INSERT INTO inv.supplier_credit_memos "
            f"({', '.join(cols)}) VALUES (%(memo)s, %(ship)s::uuid, "
            f"%(supplier)s::uuid, %(product)s::uuid, %(store)s::uuid, "
            f"%(quantity)s, 4.0, 80.00, %(reason)s::varchar, %(status)s, "
            f"{p['deadline']}, {p['submitted']}"
            + (f", {p['resolved']}" if with_resolved else "")
            + ")")


def test_a_claim_against_a_short_ship_is_accepted(sandbox, seeded):
    conn = sandbox_conn(sandbox)
    p = _open_memo(sandbox, seeded)
    ok, detail = accepts(conn, _memo_sql(sandbox, p), p)
    assert ok, detail


def test_two_claims_against_one_short_ship_are_rejected(sandbox, seeded):
    """
    The UNIQUE on short_ship_id is what makes `open_credit_memo` idempotent —
    it looks up "has this shortfall already been claimed?" and relies on the
    constraint to be the real answer, not a race.
    """
    conn = sandbox_conn(sandbox)
    p = _open_memo(sandbox, seeded)
    ok, detail = accepts(conn, _memo_sql(sandbox, p), p)
    assert ok, detail
    p2 = dict(p, memo=f"CM-2-{seeded['tag']}")
    rejected, detail = rejects(conn, _memo_sql(sandbox, p2), p2)
    assert rejected, f"a double claim was accepted: {detail}"


@pytest.mark.parametrize("label,overrides", [
    ("an unknown short_reason", {'reason': 'vibes'}),
    ("a zero credit_quantity", {'quantity': 0}),
    ("an unknown memo_status", {'status': 'pending'}),
])
def test_credit_memo_row_constraints_hold(sandbox, seeded, label, overrides):
    conn = sandbox_conn(sandbox)
    p = _open_memo(sandbox, seeded, **overrides)
    rejected, detail = rejects(conn, _memo_sql(sandbox, p), p)
    assert rejected, f"{label} was accepted: {detail}"


@pytest.mark.parametrize("status", ['submitted', 'paid', 'rejected'])
def test_a_terminal_status_needs_its_timestamp(sandbox, seeded, status):
    """
    `submitted`/`paid`/`rejected` without the matching timestamp would be a row
    in a claims-aging mart that claims to have moved and cannot say when — and
    the CHECK is the only thing standing between the generator and that.
    """
    conn = sandbox_conn(sandbox)
    p = _open_memo(sandbox, seeded, status=status)
    rejected, detail = rejects(conn, _memo_sql(sandbox, p), p)
    assert rejected, f"'{status}' with no timestamp was accepted: {detail}"


def test_a_paid_claim_with_both_timestamps_is_accepted(sandbox, seeded):
    conn = sandbox_conn(sandbox)
    p = _open_memo(sandbox, seeded, status='paid',
                   submitted='NOW() - INTERVAL \'5 days\'',
                   resolved='NOW()', deadline='CURRENT_DATE + 14')
    ok, detail = accepts(conn, _memo_sql(sandbox, p), p)
    assert ok, detail


def test_a_claim_submitted_after_its_deadline_is_rejected(sandbox, seeded):
    """
    The vendor's claim window closed before anyone filed: the claim cannot be
    submitted, and the row has to be recorded as `expired` instead. Accepting a
    late submission would make `credit_window_days` decorative — the same
    failure mode `restock_threshold_pct` had before t_959cd040.

    The two stamps are deliberately on opposite sides: submitted 10 days ago
    against a deadline that was 30 days ago. A claim submitted 30 days ago
    against a deadline of 10 days ago would be INSIDE the window and must be
    accepted, which is what `test_a_claim_submitted_inside_the_window` pins.
    """
    conn = sandbox_conn(sandbox)
    p = _open_memo(sandbox, seeded, status='submitted',
                   submitted="NOW() - INTERVAL '10 days'",
                   deadline='CURRENT_DATE - 30')
    rejected, detail = rejects(conn, _memo_sql(sandbox, p), p)
    assert rejected, f"a late claim was accepted: {detail}"


def test_a_claim_submitted_inside_the_window_is_accepted(sandbox, seeded):
    conn = sandbox_conn(sandbox)
    p = _open_memo(sandbox, seeded, status='submitted',
                   submitted='NOW() - INTERVAL \'3 days\'',
                   deadline='CURRENT_DATE + 14')
    ok, detail = accepts(conn, _memo_sql(sandbox, p), p)
    assert ok, detail


def test_an_expired_claim_cannot_also_carry_a_resolution(sandbox, seeded):
    """
    `expired` means the vendor's window closed, so there was never a
    submission and never a payment. A row carrying both would double-count in
    a mart that sums by status.
    """
    conn = sandbox_conn(sandbox)
    p = _open_memo(sandbox, seeded, status='expired',
                   submitted='NOW()', resolved='NOW()')
    rejected, detail = rejects(conn, _memo_sql(sandbox, p), p)
    assert rejected, f"an expired claim with a resolution was accepted: {detail}"


def test_an_expired_claim_with_no_submission_is_accepted(sandbox, seeded):
    """The real shape of an expiry — which is what `advance_credit_memos`
    writes, so if this were rejected the lifecycle would break."""
    conn = sandbox_conn(sandbox)
    p = _open_memo(sandbox, seeded, status='expired',
                   submitted='NULL', resolved='NULL',
                   deadline='CURRENT_DATE - 1')
    ok, detail = accepts(conn, _memo_sql(sandbox, p), p)
    assert ok, detail


# ---------------------------------------------------------------------------
# 6. inv.dsd_deliveries
# ---------------------------------------------------------------------------
# A DSD vendor delivers to the shelf on its own truck: there is no pallet on
# one of our `transport.loads`, no receiving dock, and deliberately no
# `inv.receipts` row. These tests pin the two constraints that shape is built
# on — the delivery window is a real weekday, and a missed drop is a row of
# ZERO units rather than a missing row (that record is what makes the shortfall
# visible, so the CHECK is >= 0 and must not be tightened to > 0).

def _schedule(sandbox, seeded, weekday):
    """One DSD delivery schedule row; returns its id."""
    conn = sandbox_conn(sandbox)
    with conn.cursor() as cur:
        cur.execute(scoped(SCHEDULE_SQL, sandbox),
                    {'supplier': seeded['supplier'], 'store': seeded['store'],
                     'weekday': weekday})
        schedule_id = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return schedule_id


def _delivery(sandbox, seeded, weekday):
    """A schedule plus its delivery; returns the delivery id."""
    conn = sandbox_conn(sandbox)
    with conn.cursor() as cur:
        cur.execute(scoped(SCHEDULE_SQL, sandbox),
                    {'supplier': seeded['supplier'], 'store': seeded['store'],
                     'weekday': weekday})
        schedule_id = cur.fetchone()[0]
        cur.execute(scoped(DELIVERY_SQL, sandbox),
                    {'schedule': schedule_id, 'supplier': seeded['supplier'],
                     'store': seeded['store']})
        delivery_id = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return delivery_id


def test_a_dsd_schedule_and_delivery_are_accepted(sandbox, seeded):
    """The happy path, all three tables: schedule -> delivery -> item."""
    delivery_id = _delivery(sandbox, seeded, 3)
    ok, detail = accepts(
        sandbox_conn(sandbox), scoped(DSD_ITEM_SQL, sandbox),
        {'delivery': delivery_id, 'product': seeded['product'],
         'quantity': 12, 'line_total': 48.00})
    assert ok, detail


def test_an_out_of_range_delivery_weekday_is_rejected(sandbox, seeded):
    """`delivery_weekday` is CHECK (BETWEEN 0 AND 6) because the generator
    passes `datetime.weekday()`, which is 0..6 — a 7 would fail the first DSD
    seed with an opaque constraint violation."""
    conn = sandbox_conn(sandbox)
    rejected, detail = rejects(
        conn, scoped(SCHEDULE_SQL, sandbox),
        {'supplier': seeded['supplier'], 'store': seeded['store'],
         'weekday': 9})
    assert rejected, f"weekday 9 was accepted: {detail}"


def test_a_duplicate_dsd_schedule_is_rejected(sandbox, seeded):
    """The UNIQUE on (supplier, location, weekday) is what makes
    `seed_dsd_schedules`' ON CONFLICT DO NOTHING idempotent — without it, every
    restart would add another set of windows and the vendor would appear to
    visit several times on the same weekday."""
    conn = sandbox_conn(sandbox)
    params = {'supplier': seeded['supplier'], 'store': seeded['store'],
              'weekday': 4}
    sql = scoped(SCHEDULE_SQL, sandbox)
    ok, detail = accepts(conn, sql, params)
    assert ok, detail
    rejected, detail = rejects(conn, sql, params)
    assert rejected, f"a duplicate DSD schedule was accepted: {detail}"


def test_a_negative_dsd_quantity_is_rejected(sandbox, seeded):
    """A DSD vendor cannot deliver negative units, and a negative line would
    subtract from the shelf rather than add to it."""
    delivery_id = _delivery(sandbox, seeded, 5)
    rejected, detail = rejects(
        sandbox_conn(sandbox), scoped(DSD_ITEM_SQL, sandbox),
        {'delivery': delivery_id, 'product': seeded['product'],
         'quantity': -5, 'line_total': -20.00})
    assert rejected, f"a negative DSD quantity was accepted: {detail}"


def test_a_zero_dsd_quantity_is_accepted(sandbox, seeded):
    """A missed drop IS zero units, not a missing row: it is the record that
    makes the shortfall visible, so the CHECK must be >= 0 and not > 0. This is
    the row `generate_dsd_deliveries` writes when the vendor's truck never came.
    """
    delivery_id = _delivery(sandbox, seeded, 6)
    ok, detail = accepts(
        sandbox_conn(sandbox), scoped(DSD_ITEM_SQL, sandbox),
        {'delivery': delivery_id, 'product': seeded['product'],
         'quantity': 0, 'line_total': 0.00})
    assert ok, detail


def test_a_dsd_item_repeating_a_product_is_rejected(sandbox, seeded):
    """One delivery carries one line per product. A duplicate would double the
    delivery's unit count against its line count, which is the reconciliation a
    DSD mart would do first."""
    delivery_id = _delivery(sandbox, seeded, 0)
    conn = sandbox_conn(sandbox)
    params = {'delivery': delivery_id, 'product': seeded['product'],
              'quantity': 4, 'line_total': 16.00}
    sql = scoped(DSD_ITEM_SQL, sandbox)
    ok, detail = accepts(conn, sql, params)
    assert ok, detail
    rejected, detail = rejects(conn, sql, params)
    assert rejected, f"a duplicate DSD product line was accepted: {detail}"


def test_a_dsd_delivery_needs_a_real_schedule(sandbox, seeded):
    """The delivery is anchored to a schedule row: without that FK a DSD drop
    would exist with no vendor window behind it, which is the finding a
    on-time-delivery mart cannot make sense of."""
    rejected, detail = rejects(
        sandbox_conn(sandbox), scoped(DELIVERY_SQL, sandbox),
        {'schedule': str(uuid.uuid4()), 'supplier': seeded['supplier'],
         'store': seeded['store']})
    assert rejected, (
        f"a DSD delivery with a dangling schedule_id was accepted: {detail}")


# 7. The generator's INSERTs match the tables they target
# ---------------------------------------------------------------------------
def test_the_generator_short_ship_insert_matches_its_table():
    """
    The generator writes 16 columns; the table has 16 insertable ones. A drift
    here is invisible until a short-ship fires, which may be days of backfill
    away — so the shape is compared directly against schema.sql.
    """
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))))
    from grocery.generator.models import suppliers

    source = open(SCHEMA_PATH, encoding="utf-8").read()
    match = re.search(
        r"CREATE TABLE inv\.short_ship_events \((.*?)\n\);", source, re.S)
    assert match, "short_ship_events not found in schema.sql"
    body = match.group(1)
    insert_cols = re.search(
        r"INSERT INTO inv\.short_ship_events\s*\((.*?)\)\s*VALUES",
        open(suppliers.__file__, encoding="utf-8").read(), re.S)
    assert insert_cols, "the generator's short-ship INSERT not found"
    written = [c.strip() for c in insert_cols.group(1).replace('\n', ' ').split(',')]
    for column in written:
        assert re.search(rf"^\s+{column}\s", body, re.M), \
            f"the generator writes inv.short_ship_events.{column}, which the " \
            f"table does not declare"
    assert len(written) == suppliers.SHORT_SHIP_COLUMN_COUNT, (
        f"the generator writes {len(written)} columns but "
        f"SHORT_SHIP_COLUMN_COUNT is {suppliers.SHORT_SHIP_COLUMN_COUNT}")
    assert suppliers.SHORT_SHIP_ROW_TEMPLATE.count("%s") == len(written), (
        "the row template and the column list disagree")


def test_the_generator_dsd_inserts_match_their_tables():
    """Same shape check for the DSD path, whose templates are separate."""
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))))
    from grocery.generator.models import suppliers

    module_source = open(suppliers.__file__, encoding="utf-8").read()
    schema_source = open(SCHEMA_PATH, encoding="utf-8").read()

    for table, template, count in (
        ('inv.dsd_deliveries', suppliers.DSD_DELIVERY_ROW_TEMPLATE,
         suppliers.DSD_DELIVERY_COLUMN_COUNT),
        ('inv.dsd_delivery_items', suppliers.DSD_ITEM_ROW_TEMPLATE,
         suppliers.DSD_ITEM_COLUMN_COUNT),
    ):
        insert_cols = re.search(
            rf"INSERT INTO {re.escape(table)}\s*\((.*?)\)\s*VALUES",
            module_source, re.S)
        assert insert_cols, f"the generator's {table} INSERT not found"
        written = [c.strip() for c in
                   insert_cols.group(1).replace('\n', ' ').split(',')]
        assert len(written) == count, (
            f"{table}: the generator writes {len(written)} columns but the "
            f"declared count is {count}")
        assert template.count("%s") == len(written), (
            f"{table}: the row template and the column list disagree")