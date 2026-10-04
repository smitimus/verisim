"""
Bringing a LEGACY data dir up to the current `schema.sql` without a wipe.

THE TRAP. `bootstrap_database` applies `schema.sql` only when
`control.generator_state` is absent, so a volume that already exists never
receives a single schema change, ever. Measured on a CT106 volume from
2026-09-21: `pos.products.reference_price`, `pos.products.price_elasticity`,
`inv.sku_demand_daily`, `inv.stockout_events`, `pos.customers` and
`pos.loyalty_members.customer_id` were all absent, while the volume held
1.22M `pos.transactions` and 10.1M `online.order_items`. Every one of those
gaps is a silent hole for whoever reads it.

WHY NOT JUST RE-RUN schema.sql. Because it is not re-runnable: all 40
`CREATE TABLE` and 58 `CREATE INDEX` statements in it carry **no**
`IF NOT EXISTS`, so replaying the file aborts on the very first relation that
already exists — which on any real volume is the first statement. A file
applied once to an empty database is correct; replaying it over a populated
one is not, and `IF NOT EXISTS` cannot simply be pasted in either, because
`CREATE TABLE IF NOT EXISTS pos.products (...)` against a table that already
exists is a *no-op* — it does not add the two columns the file gained since.
`IF NOT EXISTS` and "converge" are different properties.

WHAT THIS DOES INSTEAD. It plans the difference rather than replaying the file:

* a table the volume does not have            -> `CREATE TABLE IF NOT EXISTS`
* a table it has, missing a column             -> `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
* an index it does not have                   -> `CREATE INDEX IF NOT EXISTS`
* a schema it does not have                   -> `CREATE SCHEMA IF NOT EXISTS`
* the `control.generator_state` seed row      -> skipped (see `_plan`)

Each planned statement then runs in its OWN transaction, so one refusal — and
on the standalone image most DDL *is* refused, because `entrypoint.sh` applies
`schema.sql` as `postgres` and the generator connects as $POSTGRES_USER with
`GRANT ALL` and no ownership — costs only that statement. That is the whole
design: the pass converges as far as the operator's privileges allow and
reports the rest, instead of dying on statement one and leaving the volume
exactly as drifted as it found it.

WHAT IT DELIBERATELY DOES NOT DO. It does not add missing table-level
constraints (CHECK/UNIQUE/PRIMARY KEY) to an existing table, and it does not
reorder or rewrite history: those need a real migration with a backfill plan
and a rollback, and inventing one at boot would be a far worse trade than
reporting the gap. Columns whose definition is `NOT NULL` with no default
cannot be added to a populated table at all; they are attempted (they succeed
on an empty one) and a refusal is reported like any other, so the log names
the column an operator has to backfill by hand.

This module only *plans and reports*. It is `bootstrap_database` that decides
to call it, and it is safe to call on a volume that needs nothing.
"""
import logging
import re
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# A relation, schema-qualified. Verisim's DDL is always schema-qualified, so a
# bare word here would mean the parser mis-read the file.
_QUALIFIED = r'([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*)'

_CREATE_SCHEMA_RE = re.compile(
    r'^CREATE\s+SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z_][a-z0-9_]*)', re.I)

_CREATE_TABLE_RE = re.compile(
    r'^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?' + _QUALIFIED + r'\s*\(', re.I)

_CREATE_INDEX_RE = re.compile(
    r'^CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?'
    r'(?:IF\s+NOT\s+EXISTS\s+)?([a-z_][a-z0-9_]*)\s+ON\s+' + _QUALIFIED,
    re.I)

# A clause that is a table constraint, not a column definition. These are not
# re-emitted for an existing table (see the module docstring).
_TABLE_CONSTRAINT_RE = re.compile(
    r'^(CONSTRAINT|PRIMARY\s+KEY|UNIQUE|CHECK|FOREIGN\s+KEY|EXCLUDE|LIKE)\b',
    re.I)


# ---------------------------------------------------------------------------
# Statement splitting
# ---------------------------------------------------------------------------

def strip_comments(sql: str) -> str:
    """Drop `--` line comments, leaving string literals alone.

    A naive split on `;` would also split inside a `CHECK (... IN ('a', 'b'))`
    body, so the comment text has to go before the statement walk.
    """
    out = []
    in_quote = None
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if in_quote:
            out.append(ch)
            if ch == in_quote:
                in_quote = None
            i += 1
            continue
        if ch in "'\"":
            in_quote = ch
            out.append(ch)
            i += 1
            continue
        if ch == '-' and sql.startswith('--', i):
            while i < n and sql[i] != '\n':
                i += 1
            continue
        out.append(ch)
        i += 1
    return ''.join(out)


def split_statements(sql: str) -> List[str]:
    """Split a DDL file into top-level statements.

    Aware of string literals and parentheses, because Verisim's DDL puts
    commas and parentheses inside `CHECK` bodies and `NUMERIC(8,2)` types, and a
    naive `sql.split(';')` would tear a table definition in half.
    """
    sql = strip_comments(sql)
    statements, buf = [], []
    in_quote = None
    depth = 0
    for ch in sql:
        if in_quote:
            buf.append(ch)
            if ch == in_quote:
                in_quote = None
            continue
        if ch in "'\"":
            in_quote = ch
            buf.append(ch)
            continue
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth = max(0, depth - 1)
        if ch == ';' and depth == 0:
            stmt = ''.join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            continue
        buf.append(ch)
    tail = ''.join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def split_columns(body: str) -> List[str]:
    """Split a `CREATE TABLE` body into its top-level clauses.

    Commas inside `CHECK (... IN ('a','b'))` and inside a multi-line
    `CONSTRAINT` body must not split a clause, so this tracks quotes and
    parenthesis depth exactly as `split_statements` does.
    """
    clauses, buf = [], []
    in_quote = None
    depth = 0
    for ch in body:
        if in_quote:
            buf.append(ch)
            if ch == in_quote:
                in_quote = None
            continue
        if ch in "'\"":
            in_quote = ch
            buf.append(ch)
            continue
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth = max(0, depth - 1)
        if ch == ',' and depth == 0:
            clause = ''.join(buf).strip()
            if clause:
                clauses.append(clause)
            buf = []
            continue
        buf.append(ch)
    tail = ''.join(buf).strip()
    if tail:
        clauses.append(tail)
    return clauses


def column_name(clause: str) -> Optional[str]:
    """The column a clause defines, or None when the clause is a constraint.

    Kept separate from the type/default so a column can be re-emitted with its
    definition intact (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS <clause>`),
    which is what preserves `DEFAULT` and `REFERENCES` across the migration.
    """
    stripped = clause.strip()
    if _TABLE_CONSTRAINT_RE.match(stripped):
        return None
    match = re.match(r'^([a-z_][a-z0-9_]*)\s', stripped, re.I)
    return match.group(1).lower() if match else None


def columns_of(statement: str) -> List[Tuple[str, str]]:
    """`(column_name, clause)` for every column a `CREATE TABLE` declares."""
    open_at = statement.find('(')
    close_at = statement.rfind(')')
    if open_at == -1 or close_at <= open_at:
        return []
    found = []
    for clause in split_columns(statement[open_at + 1:close_at]):
        name = column_name(clause)
        if name:
            found.append((name, clause))
    return found


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------

def plan_reconcile(sql: str, live_tables: Dict[str, set],
                   seed_row_present: bool = False) -> List[str]:
    """The idempotent statements that bring `live_tables` up to `sql`.

    `live_tables` maps a schema-qualified relation name to the set of columns
    it already has. A relation not in the mapping is absent from the volume.

    `seed_row_present` suppresses the `control.generator_state` INSERT. That
    INSERT exists to seed the single control row on a *fresh* bootstrap; on a
    volume that already has one it would insert a second row with
    `state_id = 2`, and every `WHERE state_id = 1` in the generator would then
    be talking about a different row than the one `read_state` returns.
    """
    planned: List[str] = []
    for statement in split_statements(sql):
        flat = ' '.join(statement.split())

        schema_match = _CREATE_SCHEMA_RE.match(flat)
        if schema_match:
            name = schema_match.group(1).lower()
            planned.append(f'CREATE SCHEMA IF NOT EXISTS {name}')
            continue

        index_match = _CREATE_INDEX_RE.match(flat)
        if index_match:
            # Always planned, in file order. An index needs its table to exist
            # first, and the table's CREATE TABLE appears earlier in the file,
            # so on a volume missing the table the CREATE lands and this
            # follows it. If that CREATE was refused (the non-owner case) this
            # fails too and is reported like any other refusal — which is the
            # right outcome, because an index on a table that does not exist
            # is a consequence of the refusal, not a separate drift.
            planned.append(_with_if_not_exists_index(
                statement, index_match.group(1)))
            continue

        table_match = _CREATE_TABLE_RE.match(flat)
        if table_match:
            table = table_match.group(1).lower()
            if table not in live_tables:
                planned.append(_with_if_not_exists_table(statement, table))
            else:
                present = {c.lower() for c in live_tables[table]}
                for name, clause in columns_of(statement):
                    if name not in present:
                        planned.append(
                            f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {clause}')
            continue

        if re.match(r'^INSERT\s+INTO\s+control\.generator_state', flat, re.I):
            # `bootstrap_database` applies the whole file verbatim on a fresh
            # bootstrap, so this INSERT is never *planned*; it is only ever
            # re-emitted here, and only for a volume that has the table and
            # not yet the row (an interrupted first bootstrap). Replaying it on
            # a seeded volume would insert a second row with state_id = 2, and
            # every `WHERE state_id = 1` in the generator would then describe a
            # different row than `read_state` returns.
            if seed_row_present:
                log.info('Reconcile: control.generator_state already seeded; '
                         'not inserting a second control row.')
                continue
            planned.append(statement)
            continue

    return planned


def _with_if_not_exists_table(statement: str, table: str) -> str:
    """`CREATE TABLE x (...)` -> `CREATE TABLE IF NOT EXISTS x (...)`."""
    return re.sub(r'^(CREATE\s+TABLE\s+)', r'\1IF NOT EXISTS ',
                  statement, count=1, flags=re.I)


def _with_if_not_exists_index(statement: str, index_name: str) -> str:
    """`CREATE INDEX i ON t (...)` -> `CREATE INDEX IF NOT EXISTS i ON t (...)`."""
    return re.sub(r'^(CREATE\s+(?:UNIQUE\s+)?INDEX\s+)',
                  r'\1IF NOT EXISTS ', statement, count=1, flags=re.I)


def plan_summary(planned: List[str]) -> str:
    """A one-line description of the plan, for the boot log."""
    creates = sum(1 for s in planned if s.upper().startswith('CREATE TABLE'))
    columns = sum(1 for s in planned if s.upper().startswith('ALTER TABLE'))
    indexes = sum(1 for s in planned if 'INDEX' in s.upper())
    schemas = sum(1 for s in planned if s.upper().startswith('CREATE SCHEMA'))
    return (f'{creates} tables, {columns} columns, {indexes} indexes, '
            f'{schemas} schemas')


def _relation_of(statement: str) -> str:
    """The relation a planned statement acts on, for log grouping."""
    match = (re.match(r'^(?:CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?|'
                      r'ALTER\s+TABLE\s+)([a-z_.]+)', statement, re.I)
             or re.match(r'^CREATE\s+INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?'
                         r'[a-z_][a-z0-9_]*\s+ON\s+([a-z_.]+)', statement, re.I))
    return match.group(1).lower() if match else statement.split()[1].lower()


def run_reconcile(conn, sql: str, live_tables: Dict[str, set],
                  seed_row_present: bool = False) -> Dict[str, object]:
    """Run the planned statements, each in its own transaction.

    Returns `{'planned', 'applied', 'failed', 'statements'}` where
    `statements` is every statement attempted, in order, so a caller (or a
    test) can see what was reached and not just what succeeded.

    Per-statement transactions are the whole point. `entrypoint.sh` applies
    `schema.sql` as `postgres`, so on the standalone image the generator's role
    is refused most DDL; a single transaction around the pass would abort on the
    first refusal and converge nothing at all — indistinguishable, from the
    outside, from not having the pass. Failures are collected and summarised
    instead, so the generator proceeds and the log says exactly what did not
    land.
    """
    planned = plan_reconcile(sql, live_tables=live_tables,
                             seed_row_present=seed_row_present)
    applied, failed = 0, 0
    for statement in planned:
        try:
            with conn.cursor() as cur:
                cur.execute(statement)
            conn.commit()
            applied += 1
        except Exception as exc:                          # noqa: BLE001
            # Any psycopg2 error aborts the transaction; roll it back so the
            # NEXT statement in the pass, and every later tick, still has a
            # usable connection.
            conn.rollback()
            failed += 1

    outcome = {'planned': len(planned), 'applied': applied, 'failed': failed,
               'statements': planned}

    if not planned:
        log.info('Schema reconcile: data dir already matches schema.sql.')
        return outcome

    if failed:
        # Name role and owner once. The per-statement refusals are the
        # operator's drill-down; this line is the summary they act on.
        log.warning(
            'Schema reconcile applied %d of %d planned statements (%s); %d '
            'refused. On the standalone image schema.sql is applied by the '
            'postgres role while the generator connects as %s, so this role '
            'cannot CREATE or ALTER most relations — that is expected there and '
            'harmless. Run the outstanding DDL as the owner, or re-bootstrap '
            'the data dir, to converge them. Outstanding: %s',
            applied, len(planned), plan_summary(planned), failed,
            _current_role(conn),
            ', '.join(sorted({_relation_of(s) for s in planned})[:12])
            or 'none',
        )
    else:
        log.info('Schema reconcile applied %d statements (%s) — this data '
                 'dir predated a schema.sql change.',
                 applied, plan_summary(planned))
    return outcome


def _current_role(conn) -> str:
    """The role the generator is connected as, for the reconcile summary."""
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT current_user')
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else '<unknown>'
    except Exception:                                      # noqa: BLE001
        return '<unknown>'
