"""Does each industry's strip script still produce a valid, importable API?

The strip scripts delete route *sections* out of base/api/main.py by matching the
banner comment above them (t_a6ecb731 acceptance). That coupling is silent: if a
banner is renamed or the section boundaries shift, the script still exits 0 and
writes a file — just with the wrong routes, or with a dangling helper that
NameErrors at import. So the check is not "does the script run", it is "does the
output import, and does it keep exactly the routes the industry declares".

Run:  python tools/check_strip_scripts.py [checkout]
"""
import pathlib
import re
import subprocess
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import _offline_deps  # noqa: E402  (needs the sys.path insert above)

# The repo root, derived from this file's location — NOT from sys.argv. These
# modules are imported by the test suite, where sys.argv belongs to pytest and
# argv[1] is whatever path pytest was pointed at (t_a6ecb731, caught in CI).
CHECKOUT = pathlib.Path(__file__).resolve().parents[1]
API_SRC = CHECKOUT / "base" / "api" / "main.py"

SCRIPTS = {
    "grocery": CHECKOUT / "grocery" / "standalone" / "strip_gas_station.py",
    "gas-station": CHECKOUT / "gas-station" / "standalone" / "strip_grocery.py",
    "support": CHECKOUT / "support" / "standalone" / "strip_support.py",
}

# A route belongs to the industry whose name is in its path prefix.
PREFIX = re.compile(r'^/("?\w+"?)/')


def route_paths(source: str):
    """Every @app.{get,post,...} path literal in the source, in order."""
    return re.findall(r'@app\.(?:get|post|put|delete|patch)\(\s*"([^"]+)"', source)


def import_env() -> dict:
    """Env for the child import check: third-party deps + the DB names main.py reads.

    Nothing may be pip-installed on the host running this check, so the uv
    unpacked-wheel archives go on PYTHONPATH (see tools/_offline_deps.py). The DB
    env vars are what base/api main.py reads at module scope; without them the
    import fails for a reason that has nothing to do with the strip script.
    """
    return _offline_deps.env()


IMPORT_ENV = import_env()

# Sentinel: the API's own dependencies are not importable here, so the import
# check cannot run. Distinct from "imported fine" (None) and from a real
# failure (a string).
_NO_API_DEPS = object()

# What the API module needs at import time, and what its absence means.
_API_DEPS = ("fastapi", "uvicorn", "psycopg2", "pydantic")


def _api_deps_available() -> bool:
    """True when the stripped API's own dependencies can be imported here.

    Checked in a child process rather than in-process because the check itself
    may be running with a PYTHONPATH the child inherits — and because importing
    fastapi into the test process to answer the question would be a side effect
    in a program that is otherwise just reading files.
    """
    probe = subprocess.run(
        [sys.executable, "-c",
         "import " + ", ".join(_API_DEPS)],
        capture_output=True, text=True, env=IMPORT_ENV,
    )
    return probe.returncode == 0


def _try_import(module_path: pathlib.Path):
    """Import the stripped API in a child process.

    Returns None if it imported, _NO_API_DEPS if its dependencies are missing,
    or the last stderr line if it failed for a real reason.
    """
    if not _api_deps_available():
        return _NO_API_DEPS
    proc = subprocess.run(
        [sys.executable, "-c",
         f"import sys; sys.path.insert(0, {str(module_path.parent)!r}); "
         f"import main; print(len(main.app.router.routes))"],
        capture_output=True, text=True, env=IMPORT_ENV,
    )
    if proc.returncode == 0:
        return None
    return proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else "unknown error"


def industry_of(path: str) -> str:
    """Which industry a route belongs to.

    `/{industry}/...` routes are *shared*: every industry serves them, and which
    one answers is decided at request time. So they are not attributed to
    anything — they are in everyone's wanted set (see SHARED).
    """
    first = path.split("/")[1] if "/" in path[1:] else ""
    first = first.strip('"')
    return "" if first == "{industry}" else first


SHARED = {p for p in route_paths(API_SRC.read_text()) if industry_of(p) in ("", "{industry}")}


def main() -> int:
    if not API_SRC.exists():
        print(f"no base/api/main.py under {CHECKOUT}")
        return 2

    src = API_SRC.read_text()
    all_routes = route_paths(src)
    by_industry = {}
    for r in all_routes:
        by_industry.setdefault(industry_of(r), []).append(r)

    print(f"base/api/main.py declares {len(all_routes)} routes: "
          + ", ".join(f"{k or '(platform)'}={len(v)}" for k, v in sorted(by_industry.items())))

    rc = 0
    for industry, script in SCRIPTS.items():
        if not script.exists():
            print(f"{industry}: FAIL — {script.relative_to(CHECKOUT)} is missing")
            rc = 1
            continue

        with tempfile.TemporaryDirectory() as td:
            out = pathlib.Path(td) / "main.py"
            proc = subprocess.run(
                [sys.executable, str(script), str(API_SRC), str(out)],
                capture_output=True, text=True,
            )
            if proc.returncode != 0:
                print(f"{industry}: FAIL — strip script exited {proc.returncode}: "
                      f"{proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else '?'}")
                rc = 1
                continue

            stripped = out.read_text()
            kept = route_paths(stripped)

            # 1. It must at least *parse*. This is the check that needs no
            #    dependencies, so it runs everywhere — including the generator
            #    test job, which installs psycopg2/faker/pyyaml but NOT fastapi.
            try:
                compile(stripped, str(out), "exec")
            except SyntaxError as exc:
                print(f"{industry}: FAIL — stripped API does not parse: {exc}")
                rc = 1
                continue

            # 2. Where the API's own dependencies are importable, also prove the
            #    module imports — that is what catches a strip that deleted a
            #    section a later route still calls (a NameError at import).
            #    Without fastapi we cannot run that half, and saying so beats
            #    reporting a failure that is really a missing dev dependency.
            imp = _try_import(out)
            if imp is _NO_API_DEPS:
                print(f"{industry}: SKIP import check — fastapi/uvicorn/psycopg2 not "
                      f"installed here (route set still verified)")
            elif imp is not None:
                print(f"{industry}: FAIL — stripped API does not import: {imp}")
                rc = 1
                continue

            # 3. It must keep this industry's routes plus the shared/platform
            #    ones, and drop every other industry's.
            wanted = set(by_industry.get(industry, [])) | SHARED
            dropped_wrong = sorted(wanted - set(kept))
            kept_wrong = sorted(set(kept) - wanted)

            if dropped_wrong:
                print(f"{industry}: FAIL — dropped routes it should keep "
                      f"({len(dropped_wrong)}): {dropped_wrong[:5]}")
                rc = 1
            elif kept_wrong:
                print(f"{industry}: FAIL — kept routes from another industry "
                      f"({len(kept_wrong)}): {kept_wrong[:5]}")
                rc = 1
            else:
                title = re.search(r'title="([^"]+)"', stripped)
                shared = len(SHARED)
                import_note = ("imports clean" if imp is None
                               else "import check skipped")
                print(f"{industry}: OK — {len(kept)} routes kept "
                      f"({len(wanted) - shared} industry + {shared} shared/platform), "
                      f"{import_note}, title={title.group(1) if title else '?'!r}")

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
