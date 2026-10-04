"""Phase 4 must resolve each day's shifts under the weather of the DAY BEING
RESOLVED, exactly once — verisim card t_9036dfaa.

## The defect this pins

`hr.schedules.status` has a CHECK constraint limited to terminal-ish states:

    CHECK (status IN ('scheduled','confirmed','completed','no_show','called_out','adjusted'))

`resolve_schedule_actuals(conn, sim_date, scenario)` selects rows where
`scheduled_date = sim_date - 1` AND `status = 'scheduled'`, then rewrites each
one to `completed` / `called_out` / `no_show`. So the call is **consuming**: it
empties the `'scheduled'` pool for that date, and a second call with the same
date finds nothing and returns early (scheduling.py:134-135, `if not ids: return`).

t_2ab1fb0a added a second call to `run_tick`'s Phase 4 so yesterday's shifts are
scored under *yesterday's* weather rather than today's. But it left the original
same-day call in place, a few lines earlier:

    scheduling.resolve_schedule_actuals(conn, sim_dt.date(), scenario)      # line 671
    scheduling.generate_weekly_schedule(...)
    ...
    scheduling.resolve_schedule_actuals(conn, sim_dt.date(), yesterday_ctx) # line 690

Both pass the SAME `sim_dt.date()`. The first one wins every time, so the
`yesterday_ctx` call is dead code and the weather-aware path has never run —
every shift is still scored with the current day's `attendance_modifier`, which
is precisely the bug t_2ab1fb0a set out to fix. The comment above it ("so they
are resolved under YESTERDAY's weather") documents behaviour the code does not
have. `run_backfill` has the identical pair at lines 924 / 936.

Nothing caught it because no test covers schedule actuals at all: the
`hr.schedules` references in `grocery/generator/tests/` are all FK-integrity
checks, and `test_timeclock_*` covers the *timeclock* pairings, not schedules.

This test pins the structural invariant, so the redundant call cannot come back
and the comments cannot drift away from the behaviour again.
"""
import re

import pytest

MAIN = "grocery/generator/main.py"

REALTIME_PHASE4 = "def run_tick("
BACKFILL = "def run_backfill("


def _load(path=MAIN):
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def _function_body(src, header):
    """Return the source of the function whose def line starts `header`."""
    start = src.index(header)
    # Walk forward to the next top-level `def ` — that is the function's end.
    rest = src[start + len(header):]
    nxt = re.search(r"^def ", rest, flags=re.MULTILINE)
    end = start + len(header) + (nxt.start() if nxt else len(rest))
    return src[start:end]


def test_run_tick_phase4_resolves_each_day_exactly_once():
    """run_tick must call resolve_schedule_actuals once per date, not twice."""
    body = _function_body(_load(), REALTIME_PHASE4)
    calls = re.findall(
        r"scheduling\.resolve_schedule_actuals\(\s*conn,\s*([^,]+?),\s*([a-z_]+)\s*\)",
        body,
    )
    dates = [d.strip() for d, _ in calls]
    scenarios = [s for _, s in calls]

    assert len(dates) == len(set(dates)), (
        "run_tick resolves the same day more than once: %r. resolve_schedule_actuals "
        "consumes the rows it finds (status 'scheduled' -> terminal), so the FIRST "
        "call wins and every later one is a silent no-op. The scenario argument of "
        "the shadowed call is dead code -- which is how t_2ab1fb0a's "
        "yesterday-weather resolution never took effect." % (dates,)
    )
    # The one call must be the weather-aware one, not the plain same-day one.
    assert "yesterday_ctx" in scenarios, (
        "run_tick's surviving resolve call uses %r, not `yesterday_ctx`. "
        "Yesterday's shifts must be scored under yesterday's attendance_modifier "
        "(t_2ab1fb0a); scoring them with today's storm writes call-outs onto the "
        "wrong rows." % (scenarios,)
    )


def test_run_backfill_resolves_each_day_exactly_once():
    """run_backfill has the same two-call shadowing; pin it the same way."""
    body = _function_body(_load(), BACKFILL)
    calls = re.findall(
        r"scheduling\.resolve_schedule_actuals\(\s*conn,\s*([^,]+?),\s*([a-z_]+)\s*\)",
        body,
    )
    dates = [d.strip() for d, _ in calls]
    scenarios = [s for _, s in calls]

    assert len(dates) == len(set(dates)), (
        "run_backfill resolves the same day more than once: %r. Same consuming-call "
        "shadowing as run_tick (see run_tick test)." % (dates,)
    )
    assert "prev_ctx" in scenarios, (
        "run_backfill's surviving resolve call uses %r, not `prev_ctx`." % (scenarios,)
    )


@pytest.mark.parametrize("fn", [REALTIME_PHASE4, BACKFILL])
def test_generate_weekly_schedule_cannot_resurrect_the_pool(fn):
    """The shadowing is only harmless while generate_weekly_schedule stays
    forward-looking. If it ever wrote rows dated *yesterday* with status
    'scheduled', the first resolve would empty the pool and the second would
    start finding rows again -- and the guard above would be load-bearing for
    the wrong reason. Pin the forward window so that stays true.
    """
    body = _function_body(_load(), fn)
    calls = re.findall(r"scheduling\.generate_weekly_schedule\([^)]*\)", body)
    assert calls, "expected a weekly-schedule generation call in %r" % fn


def test_scheduling_py_targets_only_the_previous_day():
    """The consuming behaviour that makes double-calling fatal, pinned at source.

    If resolve_schedule_actuals ever stopped filtering status='scheduled' (or
    started writing a date the first call cannot have consumed), the
    once-per-date guard above would need re-deriving.
    """
    src = _load("grocery/generator/models/scheduling.py")
    body = _function_body(src, "def resolve_schedule_actuals(")
    assert "status = 'scheduled'" in body, (
        "resolve_schedule_actuals no longer filters status='scheduled'; the "
        "one-call-per-date invariant asserted above no longer holds."
    )
    assert "if not ids:" in body, (
        "resolve_schedule_actuals lost its empty-pool early return; a second "
        "same-day call would re-roll the rows instead of no-op'ing."
    )