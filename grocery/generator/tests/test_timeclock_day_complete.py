"""
A completed day's clock is checked as its own stream — kanban t_30410dea.

The card's shape, measured on test/107 (2026-10-15):

    clock_in  | 16 | 14:38 - 15:56
    clock_out | 63 | 15:01 - 23:30

47 employee-days held a `clock_out` whose `clock_in` did not exist anywhere —
their first `clock_in` was the next day — so data-lab's
`assert_timeclock_pairs` (severity=error) failed 47 rows on the only day of
either slot that had any breach. Nothing could repair it, and the reason is in
`run_backfill`: a past day was declared complete from its POS stream alone
(`max(transaction_dt).hour >= 23`) and skipped, so the clock was never asked
about again. A day whose whole punch stream was gone was even quieter — it
passes `assert_timeclock_pairs` vacuously.

These tests pin the two halves of the fix:

* `punch_stream_state()` / `punch_stream_gap()` report an employee-day with
  half a punch or half a break, and a traded day with no punches at all;
* `main._punch_stream_verdict()` combines that with the trade test, so an
  untraded day is never "repaired" into a full roster;
* `generate_day_events()` — the writer the repair calls — completes the card's
  exact shape: a `clock_out` with no `clock_in` gains the *planned* `clock_in`
  of that employee-day and no second clock-out.

`run_backfill`'s use of `_punch_stream_verdict()` is proven against a real
PostgreSQL (a scratch DB, a day with complete sales and its punches deleted):
the day is no longer skipped, and the repair leaves it paired. A fake cursor
cannot honestly prove that — it would only echo this file's own arithmetic.
"""
from datetime import datetime, timedelta
from unittest.mock import patch

import grocery.generator.models.timeclock as timeclock
from grocery.generator.tests.test_timeclock_breaks import (
    SIM_DATE,
    _capture_into,
    _employees,
)

PATCH = "grocery.generator.models.timeclock.execute_values"
DAY = SIM_DATE.date()


class _ClockCursor:
    """Answers the day-completeness reads from an in-memory clock + POS set.

    The grouping matches the SQL's rule exactly — counting per employee and
    event type — so a fake cannot pass a shape the real query would fail.
    """

    def __init__(self, db):
        self._db = db
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        if "AS employee_days" in flat:
            day = params[0]
            rows = [r for r in self._db.rows if r[3].date() == day]
            counts = {}
            for r in rows:
                bucket = counts.setdefault(r[0], {})
                bucket[r[2]] = bucket.get(r[2], 0) + 1
            open_days = sum(
                1 for c in counts.values()
                if c.get('clock_in', 0) != c.get('clock_out', 0)
                or c.get('break_start', 0) != c.get('break_end', 0))
            self._rows = [(len(rows), len(counts), open_days)]
        elif "EXISTS (SELECT 1 FROM pos.transactions" in flat:
            day = params[0]
            self._rows = [(day in self._db.pos_days,)]
        elif "FROM timeclock.events" in flat:
            day = params[0]
            self._rows = [(r[0], r[2], r[3])
                          for r in self._db.rows if r[3].date() == day]
        else:
            self._rows = []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _ClockDB:
    """In-memory `timeclock.events` (+ which days traded) for the checks."""

    def __init__(self, rows=None, pos_days=()):
        self.rows = list(rows or [])
        self.pos_days = set(pos_days)
        self.commits = 0

    def cursor(self, *a, **k):
        return _ClockCursor(self)

    def commit(self):
        self.commits += 1


def _shift_rows(employee_id, day=SIM_DATE, skip=(), at=None):
    """The four planned punches of one employee-day, minus `skip`."""
    shift = timeclock.plan_shift(day.date(), employee_id)
    if shift is None:
        return []
    rows = []
    for etype in timeclock.EVENT_TYPES:
        if etype in shift and etype not in skip:
            dt = at[etype] if at and etype in at else shift[etype]
            rows.append((employee_id, "loc-1", etype, dt, None))
    return rows


def _rostered(employees):
    return [e for e in employees
            if timeclock.plan_shift(DAY, e['employee_id']) is not None]


# ---------------------------------------------------------------------------
# The completeness read
# ---------------------------------------------------------------------------

def test_a_closed_day_has_no_gap():
    emp = _rostered(_employees())[0]['employee_id']
    db = _ClockDB(_shift_rows(emp), pos_days=[DAY])

    assert timeclock.punch_stream_state(db, DAY) == {
        'events': 4, 'employee_days': 1, 'open': 0}
    assert timeclock.punch_stream_gap(db, DAY) is None


def test_a_clock_out_without_its_clock_in_is_a_gap():
    """The card's exact row: half a punch, on a day that otherwise looks done."""
    emp = _rostered(_employees())[0]['employee_id']
    db = _ClockDB(_shift_rows(emp, skip=('clock_in',),
                              at={'clock_out': datetime(DAY.year, DAY.month, DAY.day, 23, 30)}),
                  pos_days=[DAY])

    state = timeclock.punch_stream_state(db, DAY)
    assert state['employee_days'] == 1
    assert state['open'] == 1
    assert "half a punch" in timeclock.punch_stream_gap(db, DAY)


def test_half_a_break_is_a_gap():
    emp = _rostered(_employees())[0]['employee_id']
    db = _ClockDB(_shift_rows(emp, skip=('break_end',)), pos_days=[DAY])

    assert timeclock.punch_stream_state(db, DAY)['open'] == 1
    assert "half a break" in timeclock.punch_stream_gap(db, DAY)


def test_a_traded_day_with_no_punches_at_all_is_a_gap():
    """The shape that passes data-lab's test vacuously — and so went unnoticed."""
    db = _ClockDB([], pos_days=[DAY])
    assert timeclock.punch_stream_state(db, DAY)['employee_days'] == 0
    assert "no punch on the clock" in timeclock.punch_stream_gap(db, DAY)


def test_an_untraded_day_needs_no_punches():
    """A day with no sales may legitimately hold nothing — do not invent a shift."""
    db = _ClockDB([], pos_days=[])
    assert timeclock.punch_stream_gap(db, DAY, expect_stream=False) is None


def test_the_read_is_scoped_to_the_day_asked_for():
    emp = _rostered(_employees())[0]['employee_id']
    other = SIM_DATE + timedelta(days=1)
    db = _ClockDB(_shift_rows(emp) + _shift_rows(emp, skip=('clock_in',),
                                                 at={'clock_out': other}),)

    assert timeclock.punch_stream_gap(db, DAY, expect_stream=True) is None
    assert timeclock.punch_stream_gap(db, other.date(), expect_stream=True)


# ---------------------------------------------------------------------------
# The decision the backfill makes
# ---------------------------------------------------------------------------

def test_verdict_is_none_for_a_finished_day():
    from grocery.generator.main import _punch_stream_verdict
    emp = _rostered(_employees())[0]['employee_id']
    db = _ClockDB(_shift_rows(emp), pos_days=[DAY])
    assert _punch_stream_verdict(db, DAY) is None


def test_verdict_names_a_lost_clock_on_a_traded_day():
    from grocery.generator.main import _punch_stream_verdict
    db = _ClockDB([], pos_days=[DAY])
    assert "no punch on the clock" in _punch_stream_verdict(db, DAY)


def test_verdict_ignores_a_day_that_never_traded():
    """No sales on the day ⇒ the roster owed nothing ⇒ nothing to repair."""
    from grocery.generator.main import _punch_stream_verdict
    assert _punch_stream_verdict(_ClockDB([], pos_days=[]), DAY) is None


# ---------------------------------------------------------------------------
# The writer the repair calls
# ---------------------------------------------------------------------------

def test_the_day_pass_completes_a_clock_out_that_lost_its_clock_in():
    employees = _employees()
    emp = _rostered(employees)[0]['employee_id']
    shift = timeclock.plan_shift(DAY, emp)
    db = _ClockDB(_shift_rows(emp, skip=('clock_in', 'break_start', 'break_end')))

    with patch(PATCH, side_effect=_capture_into(db)):
        written = timeclock.generate_day_events(db, DAY, employees)

    mine = [r for r in db.rows if r[0] == emp]
    per_type = {}
    for r in mine:
        per_type[r[2]] = per_type.get(r[2], 0) + 1

    assert written >= 1
    assert per_type.get('clock_in') == 1, "the missing punch was not written"
    assert per_type.get('clock_out') == 1, "the existing clock_out was rewritten"
    # ...and the clock_in is the PLAN's instant, not an invented one
    assert [r[3] for r in mine if r[2] == 'clock_in'] == [shift['clock_in']]

    assert timeclock.punch_stream_state(db, DAY)['open'] == 0


def test_completing_the_same_day_twice_adds_no_second_punch():
    employees = _employees()
    emp = _rostered(employees)[0]['employee_id']
    db = _ClockDB(_shift_rows(emp, skip=('clock_in',)))

    with patch(PATCH, side_effect=_capture_into(db)):
        timeclock.generate_day_events(db, DAY, employees)
        after_first = len(db.rows)
        timeclock.generate_day_events(db, DAY, employees)

    assert len(db.rows) == after_first, "the repair is not idempotent"
    assert timeclock.punch_stream_state(db, DAY)['open'] == 0
