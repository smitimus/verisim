"""
The session-timezone pin in `get_connection` (t_35b4d860).

Every simulated stamp the generators write — `simulation_dt`,
`transaction_dt`, `event_dt`, `placed_dt` — is built by a NAIVE
`datetime.now()` and stored in a `TIMESTAMPTZ` column. Postgres resolves a naive
timestamp against the SESSION's timezone, so the instant it stores is decided by
the connection rather than by the wall clock that produced the value. Pin the
session to the writer's own zone and that ambiguity disappears.

The bug this guards against is not hypothetical and it is not a display issue:
`transaction_dt::date` is how a generator decides which simulated day a backfill
still owes, and `DATE_TRUNC('day', transaction_dt)::date` is how the API reports
daily distribution. If the two disagree about which zone a naive stamp is in, a
23:30 store close lands on the wrong day and the generator's own bookkeeping
disagrees with the API's.

**These tests pin the STATEMENT, not the environment.** They assert that every
generator calls `SET SESSION TIME ZONE` and that it does so with a value derived
from the process's own local clock, without needing a Postgres server — a
regression that removes the pin is caught by CI's unit job rather than needing a
slot. The behaviour itself is verified against the live dev database; the
reasoning is recorded in `get_connection`'s docstring.
"""
import os
import pathlib
import re

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

GENERATORS = {
    "grocery": REPO_ROOT / "grocery" / "generator" / "main.py",
    "gas-station": REPO_ROOT / "gas-station" / "generator" / "main.py",
    "support": REPO_ROOT / "support" / "generator" / "main.py",
}


def _get_connection_body(source: str) -> str:
    """The text of `get_connection`, or fail loudly if it is gone."""
    start = source.find("def get_connection(cfg)")
    assert start != -1, "get_connection(cfg) is missing — the generator's only " \
                        "choke point for a database connection."
    # Up to the next top-level def, so an unrelated later function cannot make
    # this pass.
    rest = source[start:]
    nxt = re.search(r"\ndef\s+\w+", rest)
    return rest[:nxt.start()] if nxt else rest


@pytest.mark.parametrize("industry,path", sorted(GENERATORS.items()))
def test_every_generator_pins_the_session_timezone(industry, path):
    """The pin exists, in all three generators — not just the reported one.

    Gas-station and support have byte-for-byte the same write path (a naive
    `datetime.now()` into a `TIMESTAMPTZ` column), so fixing only grocery would
    leave the identical defect live in two other places.
    """
    body = _get_connection_body(path.read_text())
    assert "SET SESSION TIME ZONE" in body, (
        f"{industry}: get_connection does not pin the session timezone, so every "
        f"naive business timestamp it writes is resolved in the SERVER's zone "
        f"rather than the generator's.")


@pytest.mark.parametrize("industry,path", sorted(GENERATORS.items()))
def test_the_pin_uses_the_process_local_zone_not_utc(industry, path):
    """The zone is the WRITER's, it is parameterised, and it is never a literal UTC.

    Three failure modes pinned here, because all three look like a working fix:

    * A hardcoded `'UTC'`. That is the other common answer to this bug and it is
      wrong in the same way, just reversed — it moves the interpretation of a
      LOCAL business time to UTC, so a 09:00 opening stores as 09:00 UTC and
      reads back as 05:00 New York.
    * String interpolation. `f"SET TIME ZONE '{tz}'"` is a query-injection
      surface; the value comes from the environment, so a placeholder costs
      nothing and does not rot.
    * A value not derived from the writer's own clock. The pin has to name the
      SAME zone `datetime.now()` resolves against, which on a container is the
      `TZ` environment variable — so that is what the implementation must read.
      Asserted by name so that swapping it for a server-derived zone (the
      original bug) or for `astimezone().tzinfo` (the abbreviation Postgres
      rejects) both fail here rather than at a 3am start.
    """
    body = _get_connection_body(path.read_text())
    assert re.search(r"SET SESSION TIME ZONE\s*%s", body), (
        f"{industry}: the pin must bind its zone as a parameter (%s), not "
        f"interpolate it into the statement.")
    # Where the zone comes from: either inline (`os.environ.get("TZ")`) or
    # through the shared helper grocery has (`_local_timezone_name()`), which is
    # the form that keeps the abbreviation bug out of three files at once. The
    # rejected alternative is `astimezone().tzinfo`, whose value Postgres refuses.
    derives_locally = bool(
        re.search(r"os\.environ(\.get)?\(\s*[\"']TZ[\"']", body)
    ) or "_local_timezone_name()" in body
    assert derives_locally, (
        f"{industry}: the pinned zone must come from the writer's own clock "
        f"(the TZ env var, directly or via _local_timezone_name()) so it is the "
        f"same zone datetime.now() was resolved against.")
    if "_local_timezone_name()" in body:
        helper = path.read_text()
        assert 'os.environ.get("TZ")' in helper or "os.environ.get('TZ')" in helper, (
            f"{industry}: _local_timezone_name must read the TZ environment "
            f"variable, not str(tzinfo) — the latter yields the POSIX "
            f"abbreviation ('EDT'), which Postgres rejects outright.")
    # A literal UTC handed to the pin is the mistake above, in the one place it
    # can hide from the check above.
    assert not re.search(r"SET SESSION TIME ZONE\s+%?\s*'?(UTC)'?\s*[,)]", body), (
        f"{industry}: the session is pinned to a hardcoded UTC instead of the "
        f"generator's own local zone.")


@pytest.mark.parametrize("industry,path", sorted(GENERATORS.items()))
def test_the_pin_runs_before_any_query(industry, path):
    """The pin is applied on the connection the generator then uses.

    `get_connection` returns the same handle, so a pin issued on a throwaway
    connection (or issued after the handle is handed back) would leave the very
    first write unprotected while still reading as fixed in review.
    """
    body = _get_connection_body(path.read_text())
    pin = body.find("SET SESSION TIME ZONE")
    ret = body.rfind("return")
    assert pin != -1 and ret != -1 and pin < ret, (
        f"{industry}: get_connection must issue SET SESSION TIME ZONE on the "
        f"connection it returns, before returning it.")


def test_the_documented_reason_names_the_mechanism():
    """The docstring must say WHY, so the pin is not removed as cargo cult.

    A one-line `SET` with no explanation is the thing that gets deleted by the
    next person who cannot work out what it is for. The two claims worth having
    on the record are the mechanism (naive stamp + TIMESTAMPTZ = session zone
    decides the instant) and the consequence (day-boundary aggregates silently
    disagree with the generator).
    """
    body = _get_connection_body(GENERATORS["grocery"].read_text())
    assert "naive" in body.lower(), "the pin must document the naive-stamp mechanism"
    assert "TIMESTAMPTZ" in body, "the pin must name the column type that triggers it"
    assert re.search(r"session", body, re.I), "the pin must name the session timezone"


def _local_timezone_name():
    """The shipped helper, extracted from main.py rather than re-typed.

    A copy would drift from the code it guards, and the bug this file exists to
    catch is precisely a helper that looked right and was wrong (`str(tzinfo)`
    yielding `'EDT'`, which Postgres rejects). Extracting the real function
    source keeps the assertion on the shipped code.

    `main.py` cannot simply be imported: it does `from config import
    load_config` against a sibling directory and pulls in psycopg2 at module
    scope. So the helper's own source is exec'd on its own, with only the
    globals it uses.
    """
    src = REPO_ROOT / "grocery" / "generator" / "main.py"
    text = src.read_text()
    start = text.index("def _local_timezone_name():")
    rest = text[start:]
    snippet = rest[:rest.index("\ndef ", 1)]

    import datetime as _dt

    ns = {"os": os, "datetime": _dt.datetime}
    exec(compile(snippet, str(src), "exec"), ns)   # noqa: S102
    return ns["_local_timezone_name"]


def test_the_pinned_zone_is_never_a_bare_abbreviation():
    """The one that would have shipped a hard startup failure.

    `str(datetime.now().astimezone().tzinfo)` is `'EDT'` on this box, and the
    server answers `ERROR: invalid value for parameter "TimeZone": "EDT"` —
    measured on the live dev database, not inferred. So the helper must return a
    name the server recognises.

    Asserted against the REAL helper (extracted from `main.py`, not re-typed),
    with TZ forced through the values a slot might realistically carry.
    """
    helper = _local_timezone_name()

    # 1) With TZ set — the normal case, and what compose configures.
    os_environ = os.environ
    for tz, expect_iana in (
        ("America/New_York", True),
        ("Asia/Kolkata", True),          # +05:30, a half-hour offset
        ("Pacific/Kiritimati", True),    # +14:00, the extreme east
        ("Australia/Eucla", True),       # +08:45, a quarter-hour offset
    ):
        os_environ["TZ"] = tz
        got = helper()
        assert got == tz, f"TZ={tz} must pin the session to {tz}, got {got!r}"
        assert got not in ("EDT", "EST", "UTC+14"), (
            f"TZ={tz} produced the abbreviation {got!r}, which Postgres rejects "
            f"outright — the generator would refuse to start.")
        assert expect_iana

    # 2) With TZ unset: UTC, stated rather than inherited from the server.
    os_environ.pop("TZ", None)
    got = helper()
    assert got == "UTC", (
        f"with no TZ, Python resolves to UTC, so the pin must SAY UTC rather "
        f"than inherit the server's default — inheriting it is the exact "
        f"mismatch being fixed. Got {got!r}.")


def test_the_helper_never_returns_something_the_server_would_reject():
    """Guard the shape of the return value, whatever TZ is in the environment.

    A POSIX abbreviation is the only form known to be rejected, and it is the
    one `str(tzinfo)` produces — so the property is "not an abbreviation",
    asserted rather than assumed about a specific environment.
    """
    helper = _local_timezone_name()
    got = helper()
    # IANA names are Region/City, UTC is itself, and offsets contain a digit.
    looks_iana = "/" in got
    looks_offset = any(ch.isdigit() for ch in got)
    assert got == "UTC" or looks_iana or looks_offset, (
        f"the pinned zone {got!r} is neither UTC, an IANA name, nor an offset "
        f"string — it looks like a bare abbreviation, which Postgres rejects.")