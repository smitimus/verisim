"""Do an industry's API routes query only tables and columns its schema.sql creates?

The strip scripts decide which routes a gas-station image serves, and the
generator decides which relations exist. Nothing checked that those two agree —
a gas-station route that joins a table only grocery's schema creates (say
`ordering.store_orders`) passes the strip check and still answers 500 on a real
container, because the relation is not there.

Reads the route bodies out of `base/api/main.py`, pulls the SQL table names out
of them, and diffs against the CREATE TABLE list in that industry's schema.sql.

Two levels are checked, because the schemas disagree at both:

1. **Tables.** A route naming a relation *no* `schema.sql` creates would 500 on
   every image, so that is judged against the union across industries.

2. **Columns of a shared route.** `base/api/main.py` runs ONE SQL string per
   path for every industry, so a projection naming a column only some
   industries' `schema.sql` declares is a parse-time `42703` — HTTP 500 on the
   rest, for every page. That is `t_04e1e8be`: `/{industry}/inventory/receipts`
   projected `r.load_id` and `/{industry}/inventory/products` projected
   `ip.supplier_id`, while `gas-station/generator/schema.sql` (and
   `base/db/init.sql`, `base/db/03_grocery_schema.sql`) declare neither, so both
   routes answered 500 on a real gas-station data dir. The union-level table
   check passed throughout, which is why it did not catch this.

   A shared projection is therefore judged per industry, and a column an
   industry may lack has to be guarded **in that route** by a probe naming it —
   `_has_column(industry, "inv", "receipts", "load_id")` — so the SQL only names
   the column where it exists. A route that serves different SQL per industry
   (`if industry == "grocery":`) or gates on a probe
   (`if not _has_vendor_tables(industry): return ...`) is out of scope here: its
   branches are not one projection, and the API tests own that behaviour.

Run:  python tools/check_api_schema_agreement.py
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

#: `CREATE TABLE schema.table (` — the head of a DDL block.
CREATE_TABLE_RE = re.compile(
    r"CREATE TABLE(?: IF NOT EXISTS)?\s+([a-z_]+)\.([a-z_]+)\s*\(", re.IGNORECASE
)

#: A line inside a CREATE TABLE block that declares a constraint, not a column.
CONSTRAINT_LINE_RE = re.compile(
    r"^(PRIMARY\s+KEY|FOREIGN\s+KEY|CONSTRAINT|UNIQUE|CHECK|EXCLUDE|LIKE|INHERITS)\b",
    re.IGNORECASE,
)

#: A column declaration: `<name> <type> ...`.
COLUMN_LINE_RE = re.compile(r"^([a-z_][a-z0-9_]*)\s+[a-z]", re.IGNORECASE)

#: `FROM schema.table alias` — the alias map for one route body.
FROM_JOIN_RE = re.compile(
    r"\b(?:FROM|JOIN|UPDATE|INTO)\s+([a-z_]+)\.([a-z_]+)"
    r"(?:\s+(?:AS\s+)?([a-z_][a-z0-9_]*))?",
    re.IGNORECASE,
)

#: Words that can follow a table name without being its alias.
SQL_KEYWORDS = {
    "where", "on", "group", "order", "limit", "offset", "set", "values", "select",
    "and", "or", "as", "using", "inner", "outer", "cross", "natural", "left",
    "right", "full", "join", "returning", "having", "union", "window", "do",
    "conflict", "with", "fetch", "for", "into", "from", "by",
}

#: `alias.column` — a column reference whose left side is a FROM/JOIN alias.
COLUMN_REF_RE = re.compile(r"\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)\b")

#: A route that compares the industry against a literal serves different SQL to
#: different industries, so the union of its branches is not one projection.
INDUSTRY_BRANCH_RE = re.compile(r"\bindustry\s*(==|!=|\bin\b|\bnot\s+in\b)\s*[\(\[]?[\"']")

#: The inline column guard: `_has_column(industry, "inv", "receipts", "load_id")`.
COLUMN_GUARD_RE = re.compile(
    r"_has_column\(\s*industry\s*,\s*[\"']([a-z_]+)[\"']\s*,\s*[\"']([a-z_]+)[\"']"
    r"\s*,\s*[\"']([a-z_]+)[\"']\s*\)"
)

#: Any other helper called with the industry: the route is not one projection
#: for every industry (it branches, or it gates on a relation probe).
OTHER_GATE_RE = re.compile(r"\b(_[a-z_]+)\(\s*industry\b")


def industries(checkout: pathlib.Path = CHECKOUT) -> dict[str, pathlib.Path]:
    """Every industry's schema.sql, resolved under `checkout`."""
    return {
        "grocery": checkout / "grocery" / "generator" / "schema.sql",
        "gas-station": checkout / "gas-station" / "generator" / "schema.sql",
        "support": checkout / "support" / "generator" / "schema.sql",
    }


def tables_in_schema(schema_sql: pathlib.Path) -> set[str]:
    text = schema_sql.read_text()
    qualified = {
        f"{s}.{t}"
        for s, t in re.findall(
            r"CREATE TABLE(?: IF NOT EXISTS)?\s+([a-z_]+)\.([a-z_]+)", text, re.IGNORECASE
        )
    }
    return qualified


def columns_in_schema(schema_sql: pathlib.Path) -> dict[str, set[str]]:
    """`{"schema.table": {"column", ...}}` for every CREATE TABLE in the file."""
    text = schema_sql.read_text()
    out: dict[str, set[str]] = {}
    for match in CREATE_TABLE_RE.finditer(text):
        schema, table = match.group(1).lower(), match.group(2).lower()
        # The block ends at the `);` that closes it — indented or not.
        tail = text[match.end():]
        end = re.search(r"^\s*\);", tail, re.MULTILINE)
        block = tail[: end.start()] if end else tail
        columns = set()
        for raw in block.splitlines():
            line = raw.strip()
            if not line or line.startswith("--") or CONSTRAINT_LINE_RE.match(line):
                continue
            column = COLUMN_LINE_RE.match(line)
            if column:
                columns.add(column.group(1).lower())
        out[f"{schema}.{table}"] = columns
    return out


def served_industries(route_path: str, names) -> list[str]:
    """Which industries a route path answers for."""
    if route_path.startswith("/{industry}"):
        return sorted(names)
    if route_path.startswith("/{"):
        return []
    name = route_path.strip("/").split("/")[0]
    return [name] if name in names else []


def route_aliases(body: str) -> dict[str, str]:
    """`{alias: "schema.table"}` for the FROM/JOIN targets in one route body."""
    aliases: dict[str, str] = {}
    for schema, table, alias in FROM_JOIN_RE.findall(body):
        if not alias or alias.lower() in SQL_KEYWORDS:
            continue
        aliases.setdefault(alias.lower(), f"{schema.lower()}.{table.lower()}")
    return aliases


def column_findings(src: str, schemas: dict[str, dict[str, set[str]]]):
    """Per-industry column audit of the routes that serve every industry.

    Returns `(findings, checked, skipped)` where `findings` is a list of
    `(route, industry, "schema.table", "alias.column")` tuples, `checked` maps
    industry -> number of route bodies audited for it, and `skipped` is a list
    of `(route, reason)` for the shared routes this check cannot judge.
    """
    findings: list[tuple[str, str, str, str]] = []
    checked = {name: 0 for name in schemas}
    skipped: list[tuple[str, str]] = []

    for route_path, body in ROUTE_RE.findall(src):
        served = served_industries(route_path, schemas)
        if not served:
            continue
        bracketed = route_path.startswith("/{")
        if INDUSTRY_BRANCH_RE.search(body):
            if bracketed:
                skipped.append((route_path, "branches on the industry"))
            continue
        gates = sorted({name for name in OTHER_GATE_RE.findall(body) if name != "_has_column"})
        if gates:
            if bracketed:
                skipped.append((route_path, "gated by " + ", ".join(gates)))
            continue

        guards = {(schema, table, column)
                  for schema, table, column in COLUMN_GUARD_RE.findall(body)}
        aliases = route_aliases(body)
        refs = {(alias, column) for alias, column in COLUMN_REF_RE.findall(body)
                if alias.lower() in aliases}

        for industry in served:
            checked[industry] += 1
            columns_by_table = schemas[industry]
            for alias, column in sorted(refs):
                qualified = aliases[alias.lower()]
                schema, _, table = qualified.partition(".")
                if (schema, table, column.lower()) in guards:
                    continue
                declared = columns_by_table.get(qualified)
                if declared is None or column.lower() in declared:
                    # No table here: the union-level check owns that, and a
                    # table that exists but already has the column is fine.
                    continue
                findings.append((route_path, industry, qualified, f"{alias}.{column}"))
    return findings, checked, skipped


def main(checkout: pathlib.Path = CHECKOUT) -> int:
    src = (checkout / "base" / "api" / "main.py").read_text()
    rc = 0

    paths = industries(checkout)
    schemas = {name: tables_in_schema(path) for name, path in paths.items()
               if path.exists()}
    if not schemas:
        print("no schema.sql found; run from the verisim checkout root")
        return 2
    columns = {name: columns_in_schema(paths[name]) for name in schemas}

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

    # Per-industry columns: a shared route body runs the same SQL for every
    # industry, so a column it names must exist in each of them (or be guarded).
    findings, checked, skipped = column_findings(src, columns)
    print("\nShared-route column accounting (one SQL string serves every industry):")
    print("  audited: " + ", ".join(f"{name}={count} route bodies"
                                    for name, count in sorted(checked.items())))
    if skipped:
        print(f"  not audited ({len(skipped)} shared route(s) — a branch or a probe, "
              "not one projection):")
        for route_path, reason in skipped:
            print(f"    {route_path}: {reason}")
    if findings:
        print("\nShared routes projecting a column an industry's schema.sql does not "
              "declare (HTTP 500 `42703` on that data dir):")
        seen = set()
        for route_path, industry, qualified, ref in findings:
            key = (route_path, industry, qualified, ref)
            if key in seen:
                continue
            seen.add(key)
            print(f"  {route_path} [{industry}]: {ref} — {qualified} has no such "
                  "column; guard it with _has_column(industry, ...) or drop it")
        rc = 1
    else:
        print("OK — every column a shared route projects exists in every schema.sql "
              "that serves it")

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
