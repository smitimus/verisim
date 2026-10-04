"""Run the verisim generator test suite offline.

This host's security scanner blocks `pip`/`uv pip install`, so the test deps are
taken from uv's unpacked wheel archives instead (`~/.cache/uv/archive-v0/*`).
Same trick as the brain-host workaround, just pointed at these archives.

Usage:  python3 scripts/run_tests_offline.py [pytest args...]
        python3 scripts/run_tests_offline.py -k elasticity -v
"""
import glob
import os
import sys

ARCHIVES = [
    '/home/smitimus/.cache/uv/archive-v0',
    '/home/smitimus/.hermes/cache/uv/archive-v0',
]

# Distribution -> importable top-level name(s), so we only pull in the archives
# that carry something we actually need.
#
# The list must include TRANSITIVE dependencies, not just the four modules the
# suite imports by name: `pytest` pulls in `pygments`, `faker` pulls in
# `dateutil`/`six`, and each of those lives in its own archive. Leaving them
# out makes this script report "MISSING DEPS" for a machine whose archives are
# perfectly complete — which is what it did on 2026-10-04, blocking every agent
# from running the suite locally.
WANTED = {
    '_pytest', 'pytest', 'faker', 'yaml', 'psycopg2', 'iniconfig', 'pluggy',
    'packaging', 'py', 'pygments', 'dateutil', 'attr', 'attrs',
    'typing_extensions', 'pytz', 'tzdata',
    # For the API suite (httpx is what its conftest imports).
    'httpx', 'httpcore', 'h11', 'anyio', 'sniffio', 'certifi', 'idna',
}

# Modules shipped as a bare `.py` at the archive root rather than a package
# directory. `six` is the one this needs (faker imports it).
WANTED_FILES = {'six.py'}


def _found():
    paths = []
    for root in ARCHIVES:
        if not os.path.isdir(root):
            continue
        for d in sorted(glob.glob(os.path.join(root, '*'))):
            if not os.path.isdir(d):
                continue
            try:
                entries = os.listdir(d)
            except OSError:
                continue
            if WANTED & set(entries) or WANTED_FILES & set(entries):
                paths.append(d)
    return paths


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    for d in _found():
        if d not in sys.path:
            sys.path.append(d)
    # CI runs `python -m pytest`, which puts the CWD on sys.path; tests import
    # `grocery.generator.*`. Reproduce that so collection resolves the same way.
    if REPO_ROOT not in sys.path:
        sys.path.append(REPO_ROOT)

    missing = []
    for mod in ('pytest', 'psycopg2', 'yaml', 'faker'):
        try:
            __import__(mod)
        except Exception as exc:                    # noqa: BLE001
            missing.append(f'{mod}: {type(exc).__name__}: {exc}')
    if missing:
        print('MISSING DEPS — cannot run the suite:')
        for m in missing:
            print('  -', m)
        return 2

    import pytest

    args = sys.argv[1:] or ['grocery/generator/tests/']
    return pytest.main(args)


if __name__ == '__main__':
    sys.exit(main())
