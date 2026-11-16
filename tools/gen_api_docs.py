"""Generate the served-route table from the BUILD PRODUCT, not from the source.

`base/api/main.py` carries every industry's routes; each standalone image strips the
other industries at build time (`*/standalone/strip_*.py`). So the file a reader greps
is **not** the surface an image serves, and a hand-written endpoint list drifts from
reality the moment a route is added, renamed, or re-sectioned.

That is not hypothetical: `AGENTS.md`'s endpoint table documented
`/grocery/transactions`, `/grocery/transaction-items`, `/grocery/employees`,
`/grocery/products`, `/grocery/timeclock-events`, `/grocery/supply-chain/orders`,
`/grocery/supply-chain/fulfillments`, `/grocery/supply-chain/shipments`,
`/grocery/generator/status` and `/grocery/distributions/{hourly,daily,department}`.
Not one of those paths has ever been served: every grocery route is
`/grocery/<schema>/...` or the shared `/{industry}/...`, and `supply-chain` is not a
namespace at all (t_f0e9c805). Every one of them answered 404.

The table is therefore derived, never typed:

  1. run the industry's strip script for real — the same call the Dockerfile makes;
  2. `ast`-parse the surviving `@app.<verb>("<path>", tags=[...])` decorators;
  3. emit one row per (path, verb), grouped by the decorator's own `tags=`.

No third party import. That matters: the blocking CI `test` job installs pytest,
psycopg2, faker and pyyaml but **not** fastapi, so a checker that imported the app
would fail there while passing on a dev box.

Usage:
    python3 tools/gen_api_docs.py                        # rewrite grocery's doc
    python3 tools/gen_api_docs.py --check                # exit 1 when it is stale
    python3 tools/gen_api_docs.py --industry gas-station # another industry
    python3 tools/gen_api_docs.py --industry all

`main(root)` accepts a checkout so a test can point it at a mutated copy.
"""
from __future__ import annotations

import argparse
import ast
import difflib
import pathlib
import re
import subprocess
import sys
import tempfile

# repo root from this file's location, never sys.argv (which belongs to pytest when
# this module is imported by the suite — same trap check_workflows.py documents).
CHECKOUT = pathlib.Path(__file__).resolve().parents[1]

VERBS = ("get", "post", "put", "patch", "delete")

# industry -> (strip script that produces that image's api/main.py, generated doc)
INDUSTRIES: dict[str, tuple[str, str]] = {
    "grocery": ("grocery/standalone/strip_gas_station.py",
                "grocery/generator/docs/api_routes.md"),
    "gas-station": ("gas-station/standalone/strip_grocery.py",
                    "gas-station/generator/docs/api_routes.md"),
    "support": ("support/standalone/strip_support.py",
                "support/generator/docs/api_routes.md"),
}

SOURCE = "base/api/main.py"


def strip_source(checkout: pathlib.Path, industry: str, workdir: pathlib.Path) -> pathlib.Path:
    """Run the industry's strip script for real and return the stripped file."""
    script, _ = INDUSTRIES[industry]
    dest = workdir / f"api_{industry.replace('-', '_')}.py"
    subprocess.run(
        [sys.executable, str(checkout / script), str(checkout / SOURCE), str(dest)],
        check=True, capture_output=True, timeout=120,
    )
    if not dest.exists():
        raise SystemExit(f"{script} exited 0 but wrote no output — the strip is broken")
    return dest


def routes_from_source(path: pathlib.Path) -> list[dict]:
    """Every (verb, path) an `@app.<verb>(...)` decorator declares, in source order."""
    tree = ast.parse(path.read_text())
    found: list[dict[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        doc = (ast.get_docstring(node) or "").strip()
        # One sentence, never a mid-word cut: the first line of a wrapped docstring
        # ends wherever the 88-column margin lands ("... the vendor's own truck at the").
        flat = " ".join(doc.split())
        stop = flat.find(". ")
        if stop != -1 and stop < 200:
            flat = flat[:stop + 1]
        elif len(flat) > 180:
            flat = flat[:177].rstrip() + "…"
        summary = flat
        for dec in node.decorator_list:
            if not (isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute)):
                continue
            if not (isinstance(dec.func.value, ast.Name) and dec.func.value.id == "app"):
                continue
            verb = dec.func.attr.lower()
            if verb not in VERBS or not dec.args:
                continue
            arg = dec.args[0]
            if not (isinstance(arg, ast.Constant) and isinstance(arg.value, str)):
                continue
            tag = ""
            for kw in dec.keywords:
                if kw.arg == "tags" and isinstance(kw.value, (ast.List, ast.Tuple)) and kw.value.elts:
                    first = kw.value.elts[0]
                    if isinstance(first, ast.Constant) and isinstance(first.value, str):
                        tag = first.value
            found.append({
                "verb": verb.upper(),
                "path": arg.value,
                "tag": tag or "Untagged",
                "summary": summary or node.name,
                "has_doc": bool(summary),
                "tables": _tables_touched(node),
            })
    return found


_TABLE_RE = re.compile(r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([a-z_]+\.[a-z_]+)", re.IGNORECASE)


def _tables_touched(node: ast.AST) -> str:
    """The `schema.table` relations the handler's SQL names, in first-seen order.

    Used only as a description fallback when the handler has no docstring: a route
    documented as "reads `hr.employees`" is more useful to a reader than one
    documented as "hr_employees", and it cannot be wrong as long as it is derived.
    """
    seen: list[str] = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
            for table in _TABLE_RE.findall(sub.value):
                if table.lower() not in (s.lower() for s in seen):
                    seen.append(table.lower())
    return ", ".join(f"`{t}`" for t in seen)



def render(industry: str, routes: list[dict[str, str]]) -> str:
    """The generated markdown document."""
    script, _ = INDUSTRIES[industry]
    order: list[str] = []
    grouped: dict[str, list[dict[str, str]]] = {}
    for route in routes:
        if route["tag"] not in grouped:
            order.append(route["tag"])
            grouped[route["tag"]] = []
        grouped[route["tag"]].append(route)

    out = [
        f"# {industry} served routes",
        "",
        "<!-- GENERATED FILE — do not edit by hand.",
        "     Regenerate: python3 tools/gen_api_docs.py"
        f" --industry {industry}",
        f"     Verified in CI by tools/gen_api_docs.py --check"
        " (gas-station/generator/tests/test_build_checks.py). -->",
        "",
        f"Produced by running `{script}` over `{SOURCE}` and parsing the surviving",
        "`@app.<verb>(...)` decorators — i.e. the surface the **image** serves, not the",
        "source file (which also carries every other industry's routes).",
        "",
        f"**{len(routes)} routes.** Every path is served relative to the API root, port 8010.",
        "`/{industry}/...` routes are shared by every industry; the rest are this one's own.",
        "",
    ]
    for tag in order:
        out.append(f"## {tag}")
        out.append("")
        out.append("| Method | Path | Description |")
        out.append("|--------|------|-------------|")
        for route in grouped[tag]:
            summary = route["summary"]
            if not route.get("has_doc") and route.get("tables"):
                summary = f"reads {route['tables']}"
            summary = summary.replace("|", "\\|")
            out.append(f"| {route['verb']} | `{route['path']}` | {summary} |")
        out.append("")
    return "\n".join(out).rstrip() + "\n"


def generate(checkout: pathlib.Path, industry: str) -> str:
    with tempfile.TemporaryDirectory() as tmp:
        stripped = strip_source(checkout, industry, pathlib.Path(tmp))
        routes = routes_from_source(stripped)
    if not routes:
        raise SystemExit(f"no routes found after stripping for {industry}")
    return render(industry, routes)


def main(root: pathlib.Path | None = None, argv: list[str] | None = None) -> int:
    checkout = pathlib.Path(root) if root is not None else CHECKOUT
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--industry", default="grocery",
                        choices=[*INDUSTRIES, "all"])
    parser.add_argument("--check", action="store_true",
                        help="compare with the committed file; non-zero when stale")
    args = parser.parse_args(argv)

    industries = list(INDUSTRIES) if args.industry == "all" else [args.industry]
    status = 0
    for industry in industries:
        _, rel = INDUSTRIES[industry]
        target = checkout / rel
        generated = generate(checkout, industry)
        if args.check:
            current = target.read_text() if target.exists() else ""
            if current == generated:
                print(f"{rel}: up to date ({len(generated.splitlines())} lines)")
                continue
            status = 1
            diff = difflib.unified_diff(
                current.splitlines(keepends=True), generated.splitlines(keepends=True),
                fromfile=f"a/{rel} (on disk)", tofile=f"b/{rel} (regenerated)", n=2)
            sys.stdout.writelines(diff)
            print(f"\n{rel}: STALE — run 'python3 tools/gen_api_docs.py --industry {industry}'",
                  file=sys.stderr)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(generated)
            print(f"wrote {rel} ({len(generated.splitlines())} lines)")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
