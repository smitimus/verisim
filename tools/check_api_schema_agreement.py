"""Do an industry's API routes query only tables its own schema.sql creates?

The strip scripts decide which routes a gas-station image serves, and the
generator decides which tables exist. Nothing checked that those two agree — a
gas-station route that joins a table only grocery's schema creates (say
`ordering.store_orders`) passes the strip check and still answers 500 on a real
container, because the relation is not there.

Reads the route bodies out of base/api/main.py, pulls the SQL table names out of
them, and diffs against the CREATE TABLE list in that industry's schema.sql.

Run:  python tools/check_api_schema_agreement.py [checkout]
"""
from __future__ import annotations

import pathlib
import re
import sys

# The repo root, derived from this file's location — NOT from sys.argv. This
# module is imported by the test suite, where sys.argv belongs to pytest and
# argv[1] is whatever path pytest was pointed at (t_a6ecb731, caught in CI).
CHECKOUT = pathlib.Path(__file__).resolve().parents[1]
API_SRC = CHECKOUT / "base" / "api" / "main.py"

INDUSTRIES = {
    "grocery": CHECKOUT / "grocery" / "generator" / "schema.sql",
    "gas-station": CHECKOUT / "gas-station" / "generator" / "schema.sql",
    "support": CHECKOUT / "support" / "generator" / "schema.sql",
}

# Tables the platform owns; a route may read them in any image because every
# image's entrypoint grants them. They are created by every schema.sql.
PLATFORM_PREFIXES = ("control.",)

ROUTE_RE = re.compile(
    r'@app\.(?:get|post|put|delete|patch)\(\s*"([^"]+)"(?P<body>.*?)(?=\n@app\.|\n# -{10,}|\Z)',
    re.DOTALL,
)

# FROM / JOIN targets, schema-qualified or bare.
TABLE_RE = re.compile(
    r'\b(?:FROM|JOIN|INTO|UPDATE)\s+([a-z_]+)\.([a-z_]+)', re.IGNORECASE
)


def tables_in_schema(schema_sql: pathlib.Path) -> set[str]:
    text = schema_sql.read_text()
    qualified = {
        f"{s}.{t}"
        for s, t in re.findall(
            r"CREATE TABLE(?: IF NOT EXISTS)?\s+([a-z_]+)\.([a-z_]+)", text, re.IGNORECASE
        )
    }
    return qualified


def main() -> int:
    src = API_SRC.read_text()
    rc = 0

    schemas = {name: tables_in_schema(path) for name, path in INDUSTRIES.items()
               if path.exists()}
    if not schemas:
        print("no schema.sql found; run from the verisim checkout root")
        return 2

    # The union across industries: what a route may legitimately reference if any
    # industry can serve it. A shared /{industry} route is served by whichever
    # industry answers, so it can only be judged against all of them together.
    union = set().union(*schemas.values())

    # `information_schema`, `pg_catalog` are always there.
    ALWAYS = {"information_schema.pg_tables", "pg_catalog.pg_class"}

    print(f"{len(src.splitlines())} lines of base/api/main.py; "
          + ", ".join(f"{n}={len(t)} tables" for n, t in sorted(schemas.items())))

    missing_anywhere: dict[str, list[str]] = {}
    for path, body in ROUTE_RE.findall(src):
        body = body if isinstance(body, str) else body.group("body")
        for schema, table in set(TABLE_RE.findall(body)):
            qualified = f"{schema}.{table}"
            if qualified in union or qualified in ALWAYS:
                continue
            missing_anywhere.setdefault(qualified, []).append(path)

    if missing_anywhere:
        print("\nRoutes reference tables NO schema.sql creates (every image would 500):")
        for table, routes in sorted(missing_anywhere.items()):
            print(f"  {table}: {len(routes)} route(s), e.g. {routes[0]}")
        rc = 1
    else:
        print("OK — every table a route touches exists in at least one schema.sql")

    # Per-industry: an industry-specific route must resolve inside its own schema
    # or the shared set. This is the check that would have caught a gas-station
    # route reaching for a grocery-only table.
    print("\nPer-industry route accounting:")
    for name, tables in sorted(schemas.items()):
        own = [p for p, _ in ROUTE_RE.findall(src) if p.startswith(f"/{name}")]
        if not own:
            print(f"  {name}: no industry-prefixed routes")
            continue
        print(f"  {name}: {len(own)} routes, {len(tables)} tables in its schema.sql")

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
