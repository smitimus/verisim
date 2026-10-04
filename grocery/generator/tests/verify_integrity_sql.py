"""
Parse-check every cross-schema assertion against a live database.

`test_cross_schema_integrity.py` executes each assertion, so a malformed query
fails there — but only if a database happens to be reachable, and it reports a
`psycopg2` syntax error rather than "you wrote this wrong". This runs every
query against the sandbox schema built from the real schema.sql, so a new
assertion is known-good at authoring time and cannot land broken.

`PREPARE` is the right tool: Postgres parses and resolves the statement without
running it, so a query referencing a column that does not exist fails here
rather than as a mysterious empty result later.

The sandbox is renamed (`supcheck<x>_inv`, ...) by the same uniform rewrite the
schema test uses, so these queries describe the same objects the assertions do.

Usage:  python3 grocery/generator/tests/verify_integrity_sql.py
"""
import os
import pathlib
import re
import sys
import uuid

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent.parent))

# Derived from the file, never hand-listed: a hardcoded list goes stale the
# moment a card adds a schema (t_2ab1fb0a added `weather`), and then that schema
# is applied under its ORIGINAL name while the rest are renamed, so it collides
# with a real one — `relation "daily" already exists`, which reads like a
# schema.sql bug and is not one.
GENERATOR_SCHEMAS = tuple(sorted(set(re.findall(
    r"CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+(\w+)\s*;",
    (HERE.parent / "schema.sql").read_text(encoding="utf-8"),
    flags=re.IGNORECASE))))


def _psycopg2():
    """psycopg2 from the running interpreter or the uv wheel cache."""
    try:
        import psycopg2
        return psycopg2
    except ImportError:
        pass
    import glob
    import sysconfig
    suffix = sysconfig.get_config_var("EXT_SUFFIX") or ".so"
    tag = suffix.split(".")[1] if suffix.startswith(".") else ""
    for entry in sorted(glob.glob(
            os.path.expanduser("~/.cache/uv/archive-v0/*"))):
        if not os.path.isdir(entry):
            continue
        if not os.path.exists(os.path.join(entry, "psycopg2")):
            continue
        compiled = [f for f in os.listdir(entry)
                    if f.endswith((".so", ".pyd"))]
        if compiled and not any(tag and tag in f for f in compiled):
            continue
        sys.path.insert(0, entry)
        import psycopg2
        return psycopg2
    raise SystemExit("psycopg2 not available")


def _connect(psycopg2, dbname=None):
    dsn = os.environ.get("GROCERY_TEST_DB")
    if dsn:
        return psycopg2.connect(dsn, dbname=dbname, connect_timeout=5)
    return psycopg2.connect(
        host="127.0.0.1", port=5499, user="verisim", password="verisim",
        dbname=dbname or "grocery", connect_timeout=5)


def _prefixed(source, prefix):
    for schema in sorted(GENERATOR_SCHEMAS, key=len, reverse=True):
        source = re.sub(rf"(?<![\w.]){schema}\.", f"{prefix}{schema}.", source)
        source = re.sub(
            rf"(CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+){schema}\s*;",
            rf"\g<1>{prefix}{schema};", source, flags=re.IGNORECASE)
    return source


def main():
    psycopg2 = _psycopg2()
    from grocery.generator.tests.cross_schema_integrity import ASSERTIONS

    prefix = "sqlcheck%s_" % uuid.uuid4().hex[:8]
    schema_sql = HERE.parent / "schema.sql"

    conn = _connect(psycopg2)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(_prefixed(schema_sql.read_text(encoding="utf-8"),
                                 prefix))
    except Exception as exc:
        conn.close()
        print(f"schema.sql did not apply under {prefix}: {exc}", file=sys.stderr)
        return 2

    failures = []
    try:
        for spec in ASSERTIONS:
            # Every reference to a generator schema is rewritten to the sandbox,
            # exactly as schema.sql itself was.
            sql = _prefixed(spec.sql, prefix)
            try:
                with conn.cursor() as cur:
                    # PREPARE parses + resolves without executing, so a missing
                    # column or a bad reference is caught here.
                    cur.execute(f"PREPARE verify_{spec.id.replace('-', '_')} AS "
                                f"{sql.rstrip().rstrip(';')}")
                    cur.execute(
                        f"DEALLOCATE verify_{spec.id.replace('-', '_')}")
            except Exception as exc:
                failures.append((spec.id, spec.title,
                                 str(exc).strip().splitlines()[0][:120]))
                print(f"  [FAIL] {spec.id} {spec.title}")
                print(f"         {failures[-1][2]}")
                continue
            print(f"  [ok]   {spec.id} {spec.title}")
    finally:
        with conn.cursor() as cur:
            for schema in GENERATOR_SCHEMAS:
                cur.execute(f'DROP SCHEMA IF EXISTS "{prefix}{schema}" CASCADE')
        conn.close()

    print()
    total = len(ASSERTIONS)
    print(f"{total - len(failures)}/{total} assertions parse and resolve.")
    if failures:
        print(f"\n{len(failures)} MALFORMED:")
        for spec_id, title, detail in failures:
            print(f"  - {spec_id} {title}\n      {detail}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())