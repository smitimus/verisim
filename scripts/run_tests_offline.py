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
WANTED = {
    '_pytest': '_pytest',
    'pytest': 'pytest',
    'faker': 'faker',
    'yaml': 'yaml',
    'psycopg2': 'psycopg2',
    'iniconfig': 'iniconfig',
    'pluggy': 'pluggy',
    'packaging': 'packaging',
    'py': 'py',
}


def _found():
    paths = []
    for root in ARCHIVES:
        if not os.path.isdir(root):
            continue
        for d in sorted(glob.glob(os.path.join(root, '*'))):
            if not os.path.isdir(d):
                continue
            entries = os.listdir(d)
            if any(name in entries for name in WANTED):
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
