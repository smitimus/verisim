"""
`generate_day_events()` writes a punch **once** — kanban t_251755f7.

The card's shape, measured in the source on CT107 (2026-10-25): `timeclock.events`
held every punch of **2026-10-07** twice, with different `event_id`s — 433 rows
on a day with 82 employees where a neighbour day holds 328.  The table's own
`created_at` says who wrote what:

    2026-10-07 10:01 … 11:09   105 rows   realtime, one tick at a time
    2026-10-11 15:35           328 rows   one backfill pass, whole day

The generator was up for the first ~11 hours of 10-07 and then down until
10-11 15:35; on restart the gap-fill backfill re-ran 10-07 (its POS rows were
partial, so `run_backfill`'s "already complete" test did not skip it) and
`generate_day_events()` blindly inserted a full shift for every rostered
employee — a second `clock_in`, `break_start` and `break_end` for everyone whose
shift had already started, and a second `clock_out` nowhere, because the day was
only 11 hours old when realtime wrote it.  data-lab's `assert_timeclock_pairs`
(severity error) then found 55 employee-days with `ins = 2, outs = 1`, and
`assert_labor_net_hours_agree` found 22 rows where two break-pairing strategies
disagreed by -0.5003h on the doubled sequence.

The realtime path has always been idempotent against the day's clock
(`generate_events()` reads it back first).  These tests pin the same contract on
the whole-day writer: an employee-day the table already holds rows for is
**completed**, never rewritten — one row per (employee, event_type, event_dt),
whatever order the writers ran in.

The in-memory `timeclock.events` double lives in test_timeclock_breaks.py.
"""
from collections import Counter
from datetime import datetime, timedelta
from unittest.mock import patch

import grocery.generator.models.timeclock as timeclock
from grocery.generator.tests.test_timeclock_breaks import (
    BREAK_TYPES,
    LOCATIONS,
    PUNCH_TYPES,
    SIM_DATE,
    _FakeDB,
    _capture_into,
    _employees,
    _open_breaks,
    _replay,
    _unpaired_punches,
)

PATCH = "grocery.generator.models.timeclock.execute_values"


def _day_pass(db, employees=None, sim_date=None):
    """Run the backfill's whole-day writer once, as `run_backfill` does."""
    with patch(PATCH, side_effect=_capture_into(db)):
        return timeclock.generate_day_events(
            db, sim_date or SIM_DATE.date(), employees or _employees())


def _duplicates(rows):
    """(employee, type, instant) keys written more than once."""
    keys = Counter((employee_id, event_type, event_dt)
                   for employee_id, _loc, event_type, event_dt, _notes in rows)
    return {k: v for k, v in keys.items() if v > 1}


def _per_employee(rows):
    counts = {}
    for employee_id, _loc, event_type, event_dt, _notes in rows:
        counts.setdefault(employee_id, Counter())[event_type] += 1
    return counts


# ---------------------------------------------------------------------------
# The defect: a resumed day written a second time
# ---------------------------------------------------------------------------

def test_a_whole_day_pass_over_a_partly_written_day_adds_no_second_punch():
    """The card, at the seam that produced it: realtime got the generator to
    11:59 of the day and then it died; the restart's gap-fill backfill runs the
    whole day again. Nothing may be written twice."""
    db = _replay(range(0, 12))
    before = list(db.rows)
    assert before, "the realtime half of the day wrote nothing"

    added = _day_pass(db)

    duplicates = _duplicates(db.rows)
    assert not duplicates, (
        "%d punch(es) written twice, e.g. %s"
        % (sum(v - 1 for v in duplicates.values()),
           sorted(duplicates)[:3]))
    assert added == len(db.rows) - len(before)
    # The day still has to come out complete: the backfill pass exists to fill
    # the hours realtime never reached.
    assert not _unpaired_punches(db.rows), _unpaired_punches(db.rows) or {}
    assert not _open_breaks(db.rows), _open_breaks(db.rows)
    for employee_id, counts in _per_employee(db.rows).items():
        for event_type in BREAK_TYPES + PUNCH_TYPES:
            assert counts[event_type] <= 1, (
                "%s got %d %s on one day"
                % (employee_id, counts[event_type], event_type))


def test_a_complete_day_is_not_written_a_second_time():
    """The idempotency that matters for a replayed backfill: run the day twice
    over the same date and the second pass writes nothing at all."""
    db = _FakeDB()
    first = _day_pass(db)
    rows_after_first = list(db.rows)
    assert first == len(rows_after_first) > 0

    second = _day_pass(db)

    assert second == 0, "%d event(s) written by a second whole-day pass" % second
    assert db.rows == rows_after_first


def test_a_virgin_day_still_writes_the_whole_plan():
    """No regression on the ordinary case: a day with nothing on it yet gets its
    four punches per rostered employee, at the planned instants."""
    employees = _employees()
    rostered = [e for e in employees
                if timeclock.plan_shift(SIM_DATE.date(), e["employee_id"])]
    assert rostered, "the roster has no shift at all"

    db = _FakeDB()
    written = _day_pass(db)

    assert written == 4 * len(rostered), (
        "%d rows for %d rostered employees" % (written, len(rostered)))
    assert not _duplicates(db.rows)


# ---------------------------------------------------------------------------
# Completing a half-written day rather than restarting it
# ---------------------------------------------------------------------------

def _planned_clock_ins(employees):
    rows = []
    for emp in employees:
        shift = timeclock.plan_shift(SIM_DATE.date(), emp["employee_id"])
        if shift is None:
            continue
        rows.append((emp["employee_id"], emp["location_id"], "clock_in",
                     shift["clock_in"], None))
    return rows


def test_an_open_shift_is_completed_at_its_planned_instants():
    """Someone punched in and the generator died before their break: the day
    pass writes the missing halves where they belong, not a second shift."""
    employees = _employees()
    db = _FakeDB(rows=_planned_clock_ins(employees))

    _day_pass(db, employees)

    assert not _duplicates(db.rows)
    assert not _unpaired_punches(db.rows), _unpaired_punches(db.rows) or {}
    assert not _open_breaks(db.rows), _open_breaks(db.rows)

    planned = {emp["employee_id"]: timeclock.plan_shift(SIM_DATE.date(),
                                                       emp["employee_id"])
               for emp in employees}
    for employee_id, counts in _per_employee(db.rows).items():
        shift = planned[employee_id]
        for event_type in BREAK_TYPES + PUNCH_TYPES:
            assert counts[event_type] == 1, (
                "%s holds %d %s" % (employee_id, counts[event_type], event_type))
        mine = {event_type: event_dt
                for emp_id, _l, event_type, event_dt, _n in db.rows
                if emp_id == employee_id}
        for event_type, instant in mine.items():
            assert instant == shift[event_type], (
                "%s %s landed on %s, planned %s"
                % (employee_id, event_type, instant, shift[event_type]))


def test_a_shift_moved_by_realtime_is_completed_from_where_it_was_written():
    """Realtime floors a punch above the day's newest row when it arrives late
    (t_fa8cad04), so a shift can sit later than its plan. The day pass must
    follow the clock_in the table already holds — never write a punch *before*
    it, and never re-issue the clock_in itself."""
    employees = _employees(12)
    moved_by = timedelta(minutes=90)
    shift = None
    subject = None
    for emp in employees:
        shift = timeclock.plan_shift(SIM_DATE.date(), emp["employee_id"])
        if shift is not None:
            subject = emp
            break
    assert subject is not None, "no rostered employee to move"

    written_in = shift["clock_in"] + moved_by
    db = _FakeDB(rows=[(subject["employee_id"], subject["location_id"],
                        "clock_in", written_in, None)])

    _day_pass(db, employees)

    mine = sorted((event_dt, event_type) for emp_id, _l, event_type, event_dt, _n
                  in db.rows if emp_id == subject["employee_id"])
    types = [t for _dt, t in mine]
    assert types.count("clock_in") == 1, "the moved clock_in was written again"
    assert set(types) == set(PUNCH_TYPES + BREAK_TYPES), types
    assert all(dt >= written_in for dt, _t in mine), (
        "a punch was written before the clock_in the day already held: %s" % mine)

    clock_out = dict((t, dt) for dt, t in mine)["clock_out"]
    assert clock_out - written_in == shift["clock_out"] - shift["clock_in"], (
        "the completed shift is %s long, the plan is %s"
        % (clock_out - written_in, shift["clock_out"] - shift["clock_in"]))


def test_an_employee_off_the_roster_is_closed_out_not_reissued():
    """`hr.maybe_terminate_employee()` can fire mid-shift, so the day can hold a
    punch for someone the active roster no longer carries. Their open shift is
    closed — one extra row, not a second shift."""
    db = _FakeDB(
        rows=[("emp-404", "loc-1", "clock_in",
               datetime(SIM_DATE.year, SIM_DATE.month, SIM_DATE.day, 7, 0), None)],
        people={"emp-404": "loc-1"},
    )

    _day_pass(db)

    mine = [(event_type, event_dt) for emp_id, _l, event_type, event_dt, _n
            in db.rows if emp_id == "emp-404"]
    types = [t for t, _dt in mine]
    assert types.count("clock_in") == 1, "the off-roster clock_in was written again"
    assert types.count("clock_out") == 1, (
        "the off-roster shift was left open: %s" % mine)
