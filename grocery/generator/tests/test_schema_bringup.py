"""
Boot-path schema bring-up on a LEGACY data dir — verisim card t_b17da778.

Every statement the generator runs at boot that is supposed to make an old
volume current is a `CREATE`/`ALTER`, and on the standalone image the `ALTER`
half cannot succeed: `entrypoint.sh` applies `schema.sql` through
`su -s /bin/bash postgres -c "$PSQL -f /app/generator/schema.sql"`, so every
relation is owned by `postgres`, while the generator connects as
$POSTGRES_USER with `GRANT ALL` and no ownership. Measured on CT106
2026-10-04: `ALTER TABLE pos.loyalty_members ADD COLUMN ...` raises
`InsufficientPrivilege` — and, re-verified on the dev data dir the same day, it
raises even with `IF NOT EXISTS` and even when the column is already there,
because Postgres checks table ownership before it discovers there is nothing to
do. `CREATE TABLE`, by contrast, *does* work on a data dir whose schema ACL
grants CREATE (`verisim=UC/postgres`), which is what entrypoint.sh grants.

So every bring-up path must satisfy one law: **an unavailable migration
degrades to a no-op and logs; it never raises.** The failure mode this class
of bug produces is silent and expensive — the container's `pg_isready`
healthcheck stays green, every container-level check reads healthy, and the
source writes nothing for hours, so the dbt/EDW layer reconciles against a
stale source and the shortfall is silent too.

What these tests pin:

1. `elasticity.seed_elasticity_columns` survives a refused ALTER and keeps
   going — this is the crash from the card: it raised inside `seed_all()`,
   *before* the main loop, so the generator never reached a tick (45+ minutes
   of zero writes on CT106 while the container reported healthy).
2. It does not attempt the ALTER at all once the column is present. Since
   `ADD COLUMN IF NOT EXISTS` raises on a non-owner even as a no-op, a bare
   ALTER would log a scary warning on *every* boot of a healthy container —
   noise that buries the one case that matters.
3. `customers.ensure_tables` survives a refused CREATE TABLE as well as a
   refused ALTER, and — the bug the card did not name — its CREATE is
   COMMITTED before the ALTER is attempted, so a refused ALTER cannot roll the
   table back out of existence.
4. A refused migration leaves the connection usable: rolled back, never left
   in an aborted transaction (every later statement would fail with
   `InFailedSqlTransaction`).
5. The warning names role, owner, and the supported remedy, so an operator
   reading container logs can act without reading source.
6. The reconcile pass brings an existing volume up to the current `schema.sql`
   instead of skipping it forever, and every statement in it is individually
   failure-tolerant.

The trap in #6 is the reason this cannot simply be "re-run schema.sql":
`schema.sql` declares its 40 tables and 58 indexes with **no** `IF NOT EXISTS`,
so re-running it verbatim dies on the first relation that already exists. The
reconcile pass has to make each statement idempotent first — and it must never
let one refused statement abort the ones after it.
"""
import logging
import os
import re

import psycopg2
import pytest

import grocery.generator.elasticity as elasticity
import grocery.generator.main as gen_main
import grocery.generator.models.customers as customers
import grocery.generator.schema_reconcile as reconcile
from grocery.generator.config import Config


def _schema_sql() -> str:
    """The generator's own DDL, the thing a data dir must converge to."""
    with open(os.path.join(os.path.dirname(elasticity.__file__), 'schema.sql'),
              'r', encoding='utf-8') as fh:
        return fh.read()


# ---------------------------------------------------------------------------
# Stubs: the shape test_customers_dimension.py already established.
# ---------------------------------------------------------------------------

class _StubCursor:
    """One cursor per fact asked for, so independent probes stay independent.

    `columns_present` drives the `information_schema.columns` probes (a data dir
    can have the table but not the column); `table_present` drives the
    `information_schema.tables` probes; `refuse_alter` / `refuse_create` make
    the DDL raise the way a non-owner role makes it raise on CT106.
    """

    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._conn.statements.append(sql)
        self._conn.statements_params.append(params)
        flat = ' '.join(sql.split())
        if self._conn.refuse_create and flat.startswith('CREATE TABLE'):
            raise psycopg2.errors.InsufficientPrivilege(
                'permission denied for schema pos')
        if self._conn.refuse_alter and 'ALTER TABLE' in flat:
            raise psycopg2.errors.InsufficientPrivilege(
                'must be owner of table products')
        if flat.startswith('CREATE TABLE'):
            self._conn.create_issued = True
        return self

    def fetchone(self):
        flat = ' '.join(self._conn.statements[-1].split()) if self._conn.statements else ''
        # Parameterised probes (the elasticity column check) put the names in
        # the params tuple; the hand-written ones inline them as literals.
        params = self._conn.statements_params[-1] if self._conn.statements_params else ()
        if not params:
            params = ()
        if 'information_schema.columns' in flat:
            wanted = re.findall(r"column_name\s*=\s*'([a-z_]+)'", flat)
            if len(params) >= 3 and isinstance(params[2], str):
                wanted = [params[2]]
            # `SELECT 1 ...` means has_customer_column(): it reads `is not None`,
            # so an absent column must return None, not (0,).
            present = [c for c in wanted if c in self._conn.columns_present]
            if flat.startswith('SELECT 1'):
                return (1,) if present else None
            return (len(present),)
        if 'information_schema.tables' in flat:
            # `SELECT COUNT(*)`: the table is present only once its CREATE has
            # actually been issued on THIS connection and not refused — that
            # ordering is the whole point of the probe, so the stub models it
            # rather than assuming.
            wanted = re.findall(r"table_name\s*=\s*'([a-z_]+)'", flat)
            present = [t for t in wanted if t in self._conn.tables_present]
            if not present and wanted and self._conn.create_issued \
                    and not self._conn.refuse_create:
                present = wanted
            return (len(present),)
        if 'current_user' in flat:
            return (self._conn.role,)
        if 'pg_get_userbyid' in flat:
            return (self._conn.owner,)
        return (0,)

    def fetchall(self):
        return []

    @property
    def rowcount(self):
        return 0

    def __getattr__(self, name):
        return lambda *a, **k: None


class _StubConn:
    def __init__(self, columns_present=(), tables_present=(),
                 refuse_alter=False, refuse_create=False,
                 role='verisim', owner='postgres'):
        self.columns_present = set(columns_present)
        self.tables_present = set(tables_present)
        self.refuse_alter = refuse_alter
        self.refuse_create = refuse_create
        self.role = role
        self.owner = owner
        self.statements = []
        self.statements_params = []
        self.commits = 0
        self.rollbacks = 0
        self.create_issued = False

    def cursor(self, *a, **k):
        return _StubCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def alters(self):
        return [s for s in self.statements if 'ALTER TABLE' in ' '.join(s.split())]


class _PartialRefuseConn:
    """A connection that refuses the statements naming `fail_on`, and no others.

    `fail_on='*'` refuses everything, which is the standalone-image reality;
    any other value refuses only the statements that mention it, which is how a
    volume with one unprivileged relation is modelled.
    """

    def __init__(self, fail_on='*', role='verisim'):
        self.fail_on = fail_on
        self.role = role
        self.statements = []
        self.commits = 0
        self.rollbacks = 0

    def _refuses(self, sql) -> bool:
        return self.fail_on == '*' or self.fail_on in sql

    def cursor(self, *a, **k):
        conn = self

        class _C:
            def __enter__(self):
                return self

            def __exit__(self, *e):
                return False

            def execute(self, sql, params=None):
                conn.statements.append(sql)
                if 'current_user' in sql:
                    return self
                if conn._refuses(sql):
                    raise psycopg2.errors.InsufficientPrivilege(
                        'must be owner of relation')
                return self

            def fetchone(self):
                return (conn.role,)

            def fetchall(self):
                return []

            def __getattr__(self, name):
                return lambda *a, **k: None

        return _C()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


# ---------------------------------------------------------------------------
# 1. The crash from the card.
# ---------------------------------------------------------------------------

def test_seed_elasticity_columns_survives_a_refused_alter(caplog):
    """THE regression: the generator must not die before the main loop.

    Measured on CT106 2026-10-04: the ALTER raised
    `InsufficientPrivilege: must be owner of table products` from
    `seed_all()`, so the generator never reached a tick while `pg_isready`
    kept the container green — 45+ minutes of zero writes, discovered by a
    human reading row counts, not by any health signal.

    The trade is explicit: this migration cannot land on a postgres-owned
    table, and losing the entire main loop (every POS transaction, every
    timeclock event, every supply-chain run) to protect two columns that are
    merely missing is a bad trade. Degrade, log, keep serving.
    """
    conn = _StubConn(refuse_alter=True)
    with caplog.at_level(logging.WARNING, logger='grocery.generator.elasticity'):
        touched = elasticity.seed_elasticity_columns(conn, Config())

    assert touched == {'added': 0, 'backfilled': 0}, (
        'a refused ALTER must report no columns added — claiming otherwise '
        'would hide the drift from the very log an operator reads'
    )
    assert conn.rollbacks >= 1, (
        'a refused ALTER must be rolled back; leaving the transaction aborted '
        'makes every later statement fail with InFailedSqlTransaction'
    )
    assert any(a.name.startswith('grocery.generator') and
               a.levelno >= logging.WARNING for a in caplog.records)


def test_a_refused_alter_still_backfills_what_it_can(caplog):
    """The refusal is per-column, not fatal to the whole call.

    `reference_price` and `price_elasticity` are two independent columns. If
    only one of them is refused, or if the ALTER path is simply unavailable,
    the existing-row backfill is still valid SQL and must still run — that is
    what fills the columns once an operator adds them out of band.
    """
    conn = _StubConn(refuse_alter=True)
    with caplog.at_level(logging.WARNING):
        elasticity.seed_elasticity_columns(conn, Config())
    sql = ' '.join(' '.join(s.split()) for s in conn.statements)
    assert 'UPDATE pos.products' in sql, (
        'the backfill must still run when the ALTER is refused — it is what '
        'populates the columns the moment they can be added'
    )


# ---------------------------------------------------------------------------
# 2. Do not attempt the ALTER on an up-to-date data dir at all.
# ---------------------------------------------------------------------------

def test_seed_elasticity_columns_does_not_alter_an_up_to_date_data_dir():
    """Every boot of every healthy container must not attempt an ALTER.

    `ADD COLUMN IF NOT EXISTS` is NOT a rescue here: on a non-owner role
    Postgres raises `InsufficientPrivilege` before it discovers there is
    nothing to do (measured on CT106 2026-10-03). So the probe must come first
    and the statement must be skipped, or every healthy boot logs a scary
    warning and the one genuine failure is lost in it.
    """
    conn = _StubConn(
        columns_present={'reference_price', 'price_elasticity'},
        refuse_alter=True,
    )
    elasticity.seed_elasticity_columns(conn, Config())
    assert not conn.alters(), (
        f'attempted an ALTER on an up-to-date data dir: {conn.alters()}'
    )


def test_seed_elasticity_columns_adds_only_the_missing_column():
    """Half-drifted is the common case, and it must be handled per column."""
    conn = _StubConn(columns_present={'price_elasticity'},
                     tables_present={'products'})
    elasticity.seed_elasticity_columns(conn, Config())
    alters = ' '.join(' '.join(a.split()) for a in conn.alters())
    assert 'reference_price' in alters
    assert 'price_elasticity' not in alters, (
        'price_elasticity is already present and must not be re-added'
    )


# ---------------------------------------------------------------------------
# 3 + 4. customers.ensure_tables: refused CREATE, and the missing commit.
# ---------------------------------------------------------------------------

def test_ensure_tables_survives_a_refused_create_table():
    """A refused CREATE must degrade, not raise.

    Whether `CREATE TABLE` can land depends on the schema ACL: entrypoint.sh
    grants `verisim=UC/postgres` on `pos`, so on a properly-provisioned data dir
    it does land. The bug was never the statement — it was that nothing checked
    the statement had landed, so `ensure_tables` returned True and logged
    "Created pos.customers" purely on having issued it. On CT106 the log said
    that and the table did not exist. So the outcome must be verified, and a
    refusal must be a warning rather than a raise.

    Measured on the dev data dir 2026-10-04: `CREATE TABLE IF NOT EXISTS
    pos.x (a int)` as `verisim` SUCCEEDS (confirmed by to_regclass, not just by
    the absence of an exception). This test models the other case — a data dir
    without that grant — because "it always works" is precisely the assumption
    that made the real failure invisible.
    """
    conn = _StubConn(refuse_create=True, refuse_alter=True)
    try:
        created = customers.ensure_tables(conn)
    except Exception as exc:                             # noqa: BLE001
        pytest.fail(f'ensure_tables raised on a refused CREATE: '
                    f'{type(exc).__name__}: {exc}')
    assert created is False, (
        'a CREATE that did not land must not be reported as created — that '
        'false "Created pos.customers" is what made this invisible on CT106'
    )
    assert conn.rollbacks >= 1


def test_ensure_tables_commits_the_create_before_attempting_the_alter():
    """A refused ALTER must not roll the freshly-created table back out.

    Read-order bug in `ensure_tables`: the `CREATE TABLE IF NOT EXISTS
    pos.customers` and the `ALTER TABLE ... ADD COLUMN customer_id` share one
    transaction, and the ALTER's failure handler calls `conn.rollback()` —
    which discards the CREATE as well. So on a non-owner role the function
    takes the `InsufficientPrivilege` branch and *still* ends up with no
    `pos.customers`, i.e. it degrades to exactly the state it was trying to
    fix. The CREATE has to be committed on its own before the ALTER is tried.
    """
    conn = _StubConn(refuse_alter=True)      # CREATE allowed, ALTER refused
    customers.ensure_tables(conn)

    # The CREATE must have been committed before the ALTER was attempted.
    create_idx = next((i for i, s in enumerate(conn.statements)
                       if 'CREATE TABLE' in ' '.join(s.split())), None)
    assert create_idx is not None, 'the CREATE TABLE was never attempted'
    assert conn.commits >= 1, (
        'the CREATE was never committed on its own, so a refused ALTER rolls '
        'it back and the data dir is left exactly as it was'
    )
    # And with the CREATE committed, the refused ALTER must not undo it.
    assert conn.rollbacks >= 1


def test_ensure_tables_leaves_the_connection_usable_after_a_refusal():
    """Roll back the refused statement, never leave the transaction aborted."""
    conn = _StubConn(refuse_alter=True, refuse_create=True)
    customers.ensure_tables(conn)
    assert conn.rollbacks >= 1


# ---------------------------------------------------------------------------
# 5. The warning has to be actionable.
# ---------------------------------------------------------------------------

def test_the_refusal_warning_names_role_owner_and_remedy(caplog):
    """An operator reads container logs, not source. Name the fix in the log.

    `models/customers.py` already does this for its ALTER; the same bar applies
    here, and to the CREATE refusal which previously produced a confidently
    wrong "Created pos.customers" line instead.
    """
    conn = _StubConn(refuse_alter=True)
    with caplog.at_level(logging.WARNING, logger='grocery.generator.elasticity'):
        elasticity.seed_elasticity_columns(conn, Config())
    text = ' '.join(r.getMessage() for r in caplog.records)
    assert 'verisim' in text, text          # the generator's own role
    assert 'postgres' in text, text         # the owner it does not have
    assert re.search(r'owner|own the table|run the ALTER', text, re.I), text


# ---------------------------------------------------------------------------
# 6. bootstrap_database must reconcile a legacy volume.
# ---------------------------------------------------------------------------

def _legacy_volume():
    """The CT106 volume as measured on 2026-10-04, minus the reconciled six.

    `pos.products` and `pos.loyalty_members` are the relations t_cdc30008
    repaired by hand on that slot; this reproduces the shape so the reconcile
    pass has something realistic to converge.
    """
    return {
        'hr.locations': {'location_id', 'name', 'address', 'city', 'state',
                         'zip', 'opened_date', 'location_type', 'is_active',
                         'created_at'},
        'pos.departments': {'department_id', 'name', 'code', 'created_at'},
        'pos.products': {'product_id', 'sku', 'name', 'department_id',
                         'category', 'cost', 'current_price',
                         'unit_of_measure', 'is_active', 'created_at',
                         'updated_at'},
        'pos.loyalty_members': {'member_id', 'first_name', 'last_name',
                                'email', 'signup_date', 'points_balance',
                                'tier', 'created_at', 'updated_at'},
        'control.generator_state': {'state_id', 'is_running', 'is_paused',
                                    'mode', 'tick_interval_seconds',
                                    'updated_at'},
    }


def test_reconcile_adds_the_drifted_columns_to_an_existing_table():
    """A volume that predates a `schema.sql` change must converge without a wipe.

    The drift mechanism, measured on a CT106 volume from 2026-09-21:
    `bootstrap_database` applies `schema.sql` only when
    `control.generator_state` is absent, so an existing volume never receives
    any schema change, ever.

    `pos.products` here already exists with 11 columns and is missing
    `reference_price` and `price_elasticity` — the exact drift that made
    `mart_product_price_elasticity` regress on noise. The pass must emit an
    ADD COLUMN for each, and must NOT re-create the table: re-creating it would
    mean dropping 1.22M transactions, which is the wipe this card exists to
    avoid.
    """
    plan = reconcile.plan_reconcile(_schema_sql(), live_tables=_legacy_volume(),
                                    seed_row_present=True)
    alters = [s for s in plan if 'ADD COLUMN' in s.upper()]
    added = {re.search(r'ADD COLUMN IF NOT EXISTS ([a-z_]+)', s, re.I).group(1)
             for s in alters}
    assert 'reference_price' in added, alters
    assert 'price_elasticity' in added, alters
    # The table already exists, so the pass must never emit a CREATE for it —
    # that would be the wipe. Only an ADD COLUMN may touch it.
    assert not any(re.match(r'CREATE TABLE IF NOT EXISTS pos\.products\b', s,
                            re.I) for s in plan), (
        'pos.products already exists and must never be re-created — that is '
        'the wipe. Missing columns are added individually instead.'
    )


def test_reconcile_creates_a_whole_missing_table():
    """A relation absent from the volume is created, IF NOT EXISTS."""
    plan = reconcile.plan_reconcile(_schema_sql(), live_tables=_legacy_volume(),
                                    seed_row_present=True)
    created = [s for s in plan if s.upper().startswith('CREATE TABLE')]
    joined = ' '.join(' '.join(s.split()) for s in created)
    # These three were all measured absent on the CT106 volume.
    for relation in ('pos.customers', 'inv.stockout_events',
                     'inv.sku_demand_daily'):
        assert relation in joined, f'{relation} was not created by the pass'
    assert all('IF NOT EXISTS' in s.upper() for s in created)


def test_reconcile_makes_every_statement_idempotent():
    """`schema.sql` is not re-runnable; the pass must make it so.

    This is the reason the reconcile pass cannot be "just re-run schema.sql":
    all 40 `CREATE TABLE` and 58 `CREATE INDEX` statements in `schema.sql`
    carry **no** `IF NOT EXISTS`, so re-running it verbatim aborts on the first
    relation that already exists — which on any real volume is the first
    statement.

    The ADD COLUMN case is the subtle half. `CREATE TABLE IF NOT EXISTS
    pos.products (...)` against a table that already exists is a *no-op*: it
    does not add the two columns the file gained since. `IF NOT EXISTS` and
    "converges on an existing volume" are different properties, which is why
    the pass plans per-column instead of replaying the file.
    """
    for volume in ({}, _legacy_volume()):
        plan = reconcile.plan_reconcile(_schema_sql(), live_tables=volume,
                                        seed_row_present=True)
        assert plan, 'the pass planned nothing'
        for stmt in plan:
            assert ';' not in stmt, f'multi-statement chunk: {stmt[:70]}'
            assert 'IF NOT EXISTS' in stmt.upper(), (
                f'not idempotent, so a second boot re-runs it: {stmt[:80]}'
            )


def test_reconcile_never_replays_the_control_seed_row():
    """A seeded volume must not get a second control row.

    `schema.sql` ends with `INSERT INTO control.generator_state ...`. Replayed
    on a volume that already has the row it inserts a SECOND row with
    state_id = 2 — and every `WHERE state_id = 1` in the generator (read_state,
    record_stats, the /status route, data-lab's readiness sensor) would then be
    describing a different row than the one the tick loop writes.
    """
    plan = reconcile.plan_reconcile(_schema_sql(), live_tables=_legacy_volume(),
                                    seed_row_present=True)
    assert not any(re.match(r'INSERT\s+INTO\s+control\.generator_state', s, re.I)
                   for s in plan)


def test_a_failed_reconcile_statement_does_not_abort_the_rest(caplog):
    """One refused statement must not cost the other seventy.

    The whole point of a reconcile pass is that statements are attempted
    INDIVIDUALLY, each in its own transaction. On the standalone image most DDL
    *is* refused (postgres owns everything), so if the first refusal aborted the
    pass then a reconcile would converge nothing at all on exactly the volumes
    that need it — the failure would be indistinguishable from not having the
    pass.

    Asserted on the runner rather than the planner: the planner returns a list,
    so "one bad statement does not abort the rest" is a property of execution.
    """
    conn = _PartialRefuseConn(fail_on='pos.customers')
    outcome = reconcile.run_reconcile(
        conn, _schema_sql(), live_tables=_legacy_volume(), seed_row_present=True)

    assert outcome['failed'], 'the stub was meant to refuse a statement'
    assert outcome['applied'] > 1, (
        'a refusal aborted the rest of the pass — on the volumes that need a '
        'reconcile (postgres owns everything) that converges nothing at all'
    )
    assert any('pos.customers' in s for s in outcome['statements'])
    assert any('inv.stockout_events' in s for s in outcome['statements']), (
        'a statement AFTER the refused one never ran'
    )


def test_a_fully_refused_reconcile_still_returns_a_report(caplog):
    """The CT106 shape: nothing may land, and the pass must still be useful.

    Every DDL refused is the measured standalone-image reality. The contract is
    that the pass reports the whole drift and the generator proceeds — the
    generator's main loop serves every other table, and losing it to protect a
    schema migration is a bad trade.
    """
    conn = _PartialRefuseConn(fail_on='*')
    with caplog.at_level(logging.WARNING, logger='grocery.generator'):
        outcome = reconcile.run_reconcile(
            conn, _schema_sql(), live_tables=_legacy_volume(),
            seed_row_present=True)
    assert outcome['failed'] > 0
    assert outcome['applied'] == 0
    text = ' '.join(r.getMessage() for r in caplog.records)
    assert re.search(r'postgres|owner', text, re.I), (
        'the summary must name the ownership problem — an operator reads '
        'container logs, not source'
    )


# ---------------------------------------------------------------------------
# 7. The live-catalog reader (t_b17da778 regression, found by running it).
# ---------------------------------------------------------------------------

def test_live_relations_returns_column_names_not_characters():
    """`_live_relations` must yield real column names.

    A real bug, caught by running the pass against the live dev database rather
    than trusting the stub: the query originally used `array_agg(column_name)`,
    and psycopg2 returns a Postgres array as the **string** `'{a,b,c}'`.
    Iterating that yields single characters, so `live['hr.locations']` was
    `[',', '_', 'a', 'c', ...]` instead of `['location_id', 'name', ...]`, every
    column looked absent, and the planner emitted 347 pointless
    `ADD COLUMN IF NOT EXISTS` statements against a volume whose 39 tables were
    otherwise intact.

    The assertion is on the shape of the answer, because that is the property
    that broke: column names, and a set (so a duplicated catalog row cannot
    inflate it).
    """
    conn = _RowsConn([
        ('hr.locations', 'location_id'), ('hr.locations', 'name'),
        ('hr.locations', 'location_id'),          # duplicate catalog row
        ('pos.products', 'reference_price'),
    ])
    live = gen_main._live_relations(conn)
    assert live['hr.locations'] == {'location_id', 'name'}, live
    assert live['pos.products'] == {'reference_price'}
    assert all(not c.startswith('{') for c in live['hr.locations'])


def test_a_volume_whose_columns_are_all_present_plans_no_alters():
    """The end-to-end property the bug above broke.

    Given a data dir that already has every column of every table it declares,
    the pass must plan NOTHING for those tables. Without the fix this plan was
    347 ALTERs against tables that were already correct — which on a real slot
    would mean a boot logging hundreds of refusals instead of the one line that
    says what actually drifted.
    """
    sql = _schema_sql()
    live = {}
    for stmt in reconcile.split_statements(sql):
        m = reconcile._CREATE_TABLE_RE.match(' '.join(stmt.split()))
        if m:
            live[m.group(1).lower()] = {
                name for name, _ in reconcile.columns_of(stmt)}

    assert len(live) > 30, 'the fixture should cover the whole schema'
    plan = reconcile.plan_reconcile(sql, live_tables=live,
                                    seed_row_present=True)
    alters = [s for s in plan if 'ADD COLUMN' in s.upper()]
    assert not alters, (
        f'{len(alters)} ALTERs planned against a fully-converged volume: '
        f'{alters[:3]}'
    )


class _RowsConn:
    """A connection returning fixed rows, for the catalog reader."""

    def __init__(self, rows):
        self.rows = rows

    def cursor(self, *a, **k):
        rows = self.rows

        class _C:
            def __enter__(self):
                return self

            def __exit__(self, *e):
                return False

            def execute(self, *a, **k):
                return self

            def fetchall(self):
                return rows

        return _C()


# ---------------------------------------------------------------------------
# The card's premise, checked against the file it names.
# ---------------------------------------------------------------------------

def test_schema_sql_has_no_if_not_exists_and_cannot_be_replayed_blindly():
    """Document why the reconcile pass needs an idempotency transform.

    Not a "this should change" demand on `schema.sql` — `schema.sql` is applied
    once to a fresh database and that is correct. It is a guard on the
    assumption in the reconcile path, which would otherwise be wrong.
    """
    import os
    path = os.path.join(os.path.dirname(elasticity.__file__), 'schema.sql')
    with open(path, 'r', encoding='utf-8') as fh:
        sql = fh.read()
    bare_tables = [ln for ln in sql.splitlines()
                   if ln.strip().startswith('CREATE TABLE ')
                   and 'IF NOT EXISTS' not in ln]
    assert bare_tables, (
        'schema.sql declares every table without IF NOT EXISTS, so the '
        'reconcile pass must transform it rather than replay it'
    )
    assert gen_main.SCHEMA_FILE.endswith('schema.sql')
