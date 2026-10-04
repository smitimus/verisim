"""Put third-party modules on sys.path from uv's unpacked-wheel cache.

Only needed on hosts where `pip install` is unavailable (this box's scanner
blocks it) — CI installs the real dependencies, so it never imports this.

uv keeps every wheel it has ever unpacked under `~/.cache/uv/archive-v0/<hash>/`,
and several *versions* of the same distribution live side by side. Order matters:
putting an old one first shadows a newer one and produces failures that look like
bugs in the code under test. Concretely, typing_extensions 4.15.0 sorted ahead of
4.16.0 makes `from typing_extensions import sentinel` (a pydantic 2.10 import)
fail with a message that has nothing to do with either.

So: sort archives by the version in their `*.dist-info` directory name, newest
first, and keep the rest in a stable order.
"""

from __future__ import annotations

import glob
import os
import re
import sys

ARCHIVE_GLOB = "/home/smitimus/.cache/uv/archive-v0/*/"

_VERSION = re.compile(r"(\d+)")


def _abi_tag() -> str:
    """This interpreter's compiled-extension tag, e.g. 'cpython-311'."""
    return f"cpython-{sys.version_info.major}{sys.version_info.minor}"


def _compiled_for_other_abi(path: str) -> bool:
    """True when the archive ships C extensions built for a different Python.

    A psycopg2 2.9.11 archive unpacked for CPython 3.14 shadows the 2.9.10 one
    built for 3.11 if you order by version alone, and then `import psycopg2`
    fails with `No module named 'psycopg2._psycopg'` — a mismatch in the cache,
    not a bug in the code under test.
    """
    mine = _abi_tag()
    for so in glob.glob(os.path.join(path, "**", "*.so"), recursive=True):
        other = re.search(r"cpython-(\d+)\d*", os.path.basename(so))
        if other and f"cpython-{other.group(1)}" != mine:
            return True
    return False


def _version_key(path: str):
    """Sort key: newest distribution version first, then a stable path tiebreak."""
    versions = []
    for dist_info in glob.glob(os.path.join(path, "*.dist-info")):
        stem = os.path.basename(dist_info)[: -len(".dist-info")]
        parts = _VERSION.findall(stem)
        if parts:
            versions.append(tuple(int(p) for p in parts))
    # Archives with no dist-info (a venv, say) sort last but stay available.
    return (max(versions) if versions else (), path)


def _rank(path: str):
    """Ordering key: same-ABI archives first, then newest version, then path."""
    return (1 if not _compiled_for_other_abi(path) else 0,) + _version_key(path)


def archive_paths() -> list[str]:
    """Every archive dir, best match first (same ABI, then newest version)."""
    return sorted(glob.glob(ARCHIVE_GLOB), key=_rank, reverse=True)


def pythonpath(existing: str = "") -> str:
    """A PYTHONPATH value with the archives ahead of anything already set."""
    return os.pathsep.join(archive_paths() + ([existing] if existing else []))


def env(base: dict | None = None, **overrides) -> dict:
    """A child-process env with the archives importable and DB names defaulted.

    The DB-name defaults are what base/api/main.py reads at module scope; without
    them an import fails for a reason unrelated to whatever is being checked.
    """
    out = dict(base if base is not None else os.environ)
    archives = archive_paths()
    if archives:
        out["PYTHONPATH"] = pythonpath(out.get("PYTHONPATH", ""))
    out.setdefault("GAS_STATION_DB", "gas_station")
    out.setdefault("GROCERY_DB", "grocery")
    out.setdefault("SUPPORT_DB", "support")
    out.update(overrides)
    return out
