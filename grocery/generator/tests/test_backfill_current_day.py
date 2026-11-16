"""
A restart must be a no-op for the current day — kanban t_9951b23c.

Measured on dev/106 while rolling the discount fix (2026-11-15): the day was
being written by the realtime loop all afternoon, and each mid-day restart ran
the boot gap scan, found today "incomplete" (its last transaction obviously
does not reach hour 23), and **re-generated the day from hour 0** on top of it.
A 15:43 restart replayed hours 0–15 and a 19:19 deploy recreate replayed hours
0–19, so the day's per-hour counts came out at ~3x the same hours on the prior
day (hour 10: 1,507 vs 440) and the shelf was drained by the repeated
depletion (`Stockout: 3416 line(s) short — 4742.7 units / $50991.85 lost`).

The root cause is a rule that only ever held for *past* days: `run_backfill`
resumed a past day after `max(transaction_dt)` but reset `start_hour` to 0 when
`cur_date == today`, and the gap scan asked the past-day question of today as
well. These tests pin the corrected rule, in one place, as `plan_day_write`:

* today, when the newest row is in the hour in progress, is **not** a gap and
  nothing is written — that is the ordinary mid-day restart;
* today, when whole hours are missing (an outage, or a fresh data dir), is a
  gap, and only those hours are written — never an hour that already holds
  rows, and never the hour in progress (realtime owns it and cannot see how
  much of it a backfill already rung);
* a past day keeps exactly the behaviour it had: resume after the newest hour,
  skip a complete day whose clock is whole, repair the clock of one that is not.

`run_backfill` itself — the whole-day pass over a live database — is proven in
`EVIDENCE.md` against a scratch PostgreSQL, because a fake cursor can only echo
this file's arithmetic (see pitfall 41 in the verisim-dev skill).
"""
import inspect
from datetime import date, datetime, timedelta

import grocery.generator.main as gen_main
from grocery.generator.tests.test_timeclock_day_complete import (
    _ClockCursor,
    _ClockDB,
    _rostered,
)
from grocery.generator.tests.test_timeclock_breaks import _employees

TODAY = date(2026, 11, 16)
PAST = TODAY - timedelta(days=1)


def _at(day, hour, minute=0):
    return datetime(day.year, day.month, day.day, hour, minute)


# ---------------------------------------------------------------------------
# The hour range for today
# ---------------------------------------------------------------------------

def test_a_day_with_nothing_on_it_gets_the_whole_day_to_the_hour_in_progress():
    """A fresh seed at 16:20 fills hours 0–16, the last one at `now`."""
    assert gen_main.todays_unwritten_hours(None, _at(TODAY, 16, 20)) == (0, 16)


def test_a_live_day_is_not_a_gap_at_all():
    """The card: realtime wrote the hour in progress, so the restart writes
    nothing — neither the hour below it nor the one it is inside."""
    assert gen_main.todays_unwritten_hours(_at(TODAY, 16, 20), _at(TODAY, 16, 41)) is None


def test_a_row_in_the_previous_hour_does_not_open_the_hour_in_progress():
    """16:00:05, the last realtime tick landed at 15:59:50. The hour of `now`
    belongs to realtime; a backfill cannot know how much of it is already rung,
    so it must not write it (that was the second, narrower double-count)."""
    assert gen_main.todays_unwritten_hours(_at(TODAY, 15, 59), _at(TODAY, 16, 0)) is None


def test_whole_hours_after_the_newest_row_are_the_backfill_s():
    """The generator was down 12:00–16:20: hours 12–15 are nobody's, and hour
    16 stays with realtime."""
    assert gen_main.todays_unwritten_hours(_at(TODAY, 11, 58), _at(TODAY, 16, 20)) == (12, 15)


def test_an_empty_day_at_midnight_still_gets_its_first_hour():
    assert gen_main.todays_unwritten_hours(None, _at(TODAY, 0, 3)) == (0, 0)


def test_a_day_with_rows_at_midnight_waits_for_realtime():
    assert gen_main.todays_unwritten_hours(_at(TODAY, 0, 2), _at(TODAY, 0, 30)) is None


# ---------------------------------------------------------------------------
# The plan `run_backfill` executes
# ---------------------------------------------------------------------------

class _PlanCursor(_ClockCursor):
    """`_ClockDB`'s cursor plus the day's newest transaction instant."""

    def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        if "max(transaction_dt)" in flat:
            self._rows = [(self._db.max_txn.get(params[0]),)]
            return
        super().execute(sql, params)


class _PlanDB(_ClockDB):
    def __init__(self, rows=None, pos_days=(), max_txn=None):
        super().__init__(rows=rows, pos_days=pos_days)
        self.max_txn = dict(max_txn or {})

    def cursor(self, *a, **k):
        return _PlanCursor(self)


def _plan(db, cur_date, now_dt, today=TODAY):
    return gen_main.plan_day_write(db, cur_date, today, now_dt)


def test_the_ordinary_mid_day_restart_writes_nothing():
    """15:43 on 2026-11-15 on dev/106, replayed: `skip`, no hours."""
    db = _PlanDB(pos_days=[TODAY], max_txn={TODAY: _at(TODAY, 15, 43)})
    plan = _plan(db, TODAY, _at(TODAY, 15, 43))
    assert plan['skip'] is True
    assert plan['punch_repair_only'] is False
    assert plan['day_was_empty'] is False


def test_a_fresh_day_plans_hours_zero_through_the_current_hour():
    db = _PlanDB()
    plan = _plan(db, TODAY, _at(TODAY, 16, 20))
    assert plan['skip'] is False
    assert (plan['start_hour'], plan['end_hour']) == (0, 16)
    assert plan['day_was_empty'] is True


def test_a_day_that_lost_whole_hours_plans_only_those_hours():
    db = _PlanDB(pos_days=[TODAY], max_txn={TODAY: _at(TODAY, 11, 58)})
    plan = _plan(db, TODAY, _at(TODAY, 16, 20))
    assert plan['skip'] is False
    assert (plan['start_hour'], plan['end_hour']) == (12, 15)


def test_a_past_day_still_resumes_after_its_newest_hour():
    db = _PlanDB(pos_days=[PAST], max_txn={PAST: _at(PAST, 11, 30)})
    plan = _plan(db, PAST, _at(TODAY, 16, 20))
    assert plan['skip'] is False
    assert (plan['start_hour'], plan['end_hour']) == (12, 23)


def test_a_past_day_with_nothing_on_it_gets_the_whole_day():
    plan = _plan(_PlanDB(), PAST, _at(TODAY, 16, 20))
    assert plan['skip'] is False
    assert (plan['start_hour'], plan['end_hour']) == (0, 23)


def test_a_past_day_with_whole_sales_and_a_whole_clock_is_skipped():
    db = _PlanDB(_past_day_shifts(_rostered_on(PAST)), pos_days=[PAST],
                 max_txn={PAST: _at(PAST, 23, 40)})
    plan = _plan(db, PAST, _at(TODAY, 16, 20))
    assert plan['skip'] is True
    assert plan['punch_repair_only'] is False


def test_a_past_day_with_whole_sales_and_a_lost_clock_is_a_punch_repair():
    db = _PlanDB(pos_days=[PAST], max_txn={PAST: _at(PAST, 23, 40)})
    plan = _plan(db, PAST, _at(TODAY, 16, 20))
    assert plan['skip'] is False
    assert plan['punch_repair_only'] is True
    assert 'no punch on the clock' in plan['punch_gap']


def _rostered_on(day):
    """The first employee on `day`'s roster (the roster is day-dependent)."""
    import grocery.generator.models.timeclock as timeclock
    for emp in _employees():
        if timeclock.plan_shift(day, emp['employee_id']) is not None:
            return emp['employee_id']
    raise AssertionError("no employee is rostered on %s" % day)


def _past_day_shifts(employee_id):
    """The four planned punches of one employee on PAST."""
    import grocery.generator.models.timeclock as timeclock
    shift = timeclock.plan_shift(PAST, employee_id)
    if shift is None:
        return []
    return [(employee_id, 'loc-1', etype, shift[etype], None)
            for etype in timeclock.EVENT_TYPES]


# ---------------------------------------------------------------------------
# One rule, two callers
# ---------------------------------------------------------------------------

def test_the_boot_scan_and_the_backfill_ask_the_same_question():
    """The defect was two answers to one question. The scan asks
    `todays_unwritten_hours` and `run_backfill` asks `plan_day_write`, which
    asks it — so a restart can never queue a day the backfill would have
    skipped, nor skip one it would have queued."""
    assert 'todays_unwritten_hours' in inspect.getsource(
        gen_main.auto_backfill_if_fresh)
    assert 'plan_day_write' in inspect.getsource(gen_main.run_backfill)
    assert 'todays_unwritten_hours' in inspect.getsource(gen_main.plan_day_write)
