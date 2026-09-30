"""
Realtime-day behaviour of the timeclock model — verisim card t_ca6642e0.

Before this card, realtime re-sampled a fraction of the **whole** store pool on
every 30-second tick against a per-calendar-day dedup, so the early-morning
branch (hours 0-5, `len(store)//6` clock-ins per tick) drained every employee
into hour 0 within minutes of midnight and left `can_in`, `can_out` and
`on_shift` empty for the rest of the day. Measured on the dev slot
(192.168.3.6, 2026-09-30): every day from the seed day on held ~100 `clock_in`,
~100 `clock_out`, **0** `break_start` and **0** `break_end`, all at hour 00,
while the backfill's complete days held 4 events per working employee spread
across the day.

These tests replay a *realtime* day — 30-second ticks, one at a time — and pin
the three properties the fix has to hold:

1. the day is a shift plan, not a pool drain: punches spread across the shift
   windows and breaks are emitted at all;
2. every write is above the newest `event_dt` already on the clock, because
   data-lab reads this table incrementally with its cursor on `event_dt` and a
   row written below the cursor is never read again (t_fa8cad04);
3. data-lab's hard `assert_timeclock_pairs` predicate stays empty — per
   employee-day, clock-ins == clock-outs and break_starts == break_ends.

The in-memory `timeclock.events` double lives in test_timeclock_breaks.py.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import grocery.generator.models.timeclock as timeclock
from grocery.generator.tests.test_timeclock_breaks import (
    LOCATIONS,
    SIM_DATE,
    _FakeDB,
    _capture_into,
    _employees,
)

TICK = timedelta(seconds=30)
PATCH = "grocery.generator.models.timeclock.execute_values"


def _capture(db):
    return patch(PATCH, side_effect=_capture_into(db))


def _realtime_day(db=None, employees=None, start_hour=0, end_hour=23,
                  step=TICK):
    """Replay one realtime day tick by tick, as `main.py`'s loop does.

    Returns (db, batches): `batches` holds the rows of every tick that wrote
    something, in the order they were written.
    """
    db = db if db is not None else _FakeDB()
    employees = employees if employees is not None else _employees()
    cursor = datetime(SIM_DATE.year, SIM_DATE.month, SIM_DATE.day, start_hour)
    end = datetime(SIM_DATE.year, SIM_DATE.month, SIM_DATE.day,
                   end_hour, 59, 59)
    batches = []
    with _capture(db):
        while cursor <= end:
            before = len(db.rows)
            timeclock.generate_events(db, cursor, employees, LOCATIONS)
            if len(db.rows) > before:
                batches.append(db.rows[before:])
            cursor += step
    return db, batches


# ---------------------------------------------------------------------------
# 1. The day is a plan
# ---------------------------------------------------------------------------

def test_a_realtime_day_spreads_punches_across_the_shift_windows():
    """The card's symptom: every punch in hour 0, no breaks. After the fix a
    realtime day carries the four event types at their planned hours."""
    db, _batches = _realtime_day()
    rows = db.rows
    assert rows, "no timeclock events generated"

    by_type = {}
    for _emp, _loc, etype, dt, _notes in rows:
        by_type.setdefault(etype, []).append(dt)

    for etype in ("clock_in", "clock_out", "break_start", "break_end"):
        assert by_type.get(etype), "a realtime day emitted no %s" % etype

    ins_by_hour = {}
    for dt in by_type["clock_in"]:
        ins_by_hour[dt.hour] = ins_by_hour.get(dt.hour, 0) + 1

    assert len(ins_by_hour) >= 6, (
        "clock-ins land on %d hour(s): %s"
        % (len(ins_by_hour), sorted(ins_by_hour)))
    worst = max(ins_by_hour.values())
    assert worst < 0.5 * len(by_type["clock_in"]), (
        "one hour holds %d of %d clock-ins - the pool is still being drained: %s"
        % (worst, len(by_type["clock_in"]), sorted(ins_by_hour.items())))

    # breaks are not a rounding error: the plan gives one to every working
    # employee, so the roster's ~80% of 100 shows up here
    assert len(by_type["break_start"]) == len(by_type["break_end"])
    assert len(by_type["break_start"]) >= 60, (
        "only %d breaks on the day" % len(by_type["break_start"]))


def test_a_realtime_day_gives_every_working_employee_one_closed_shift():
    """Four events per employee-day: in, break pair, out - the shape the
    backfill's complete days have always had."""
    db, _batches = _realtime_day()
    per_employee = {}
    for emp, _loc, etype, _dt, _notes in db.rows:
        per_employee.setdefault(emp, []).append(etype)

    assert len(per_employee) >= 60, "only %d employees worked" % len(per_employee)
    for emp, types in per_employee.items():
        assert sorted(types) == ["break_end", "break_start", "clock_in",
                                 "clock_out"], "%s got %s" % (emp, sorted(types))


def test_a_realtime_day_matches_the_backfill_day_for_the_same_date():
    """The same (date, employee) yields the same shift whichever path writes
    it - that is what makes a realtime day look like the backfilled ones."""
    employees = _employees()
    plans = {e["employee_id"]: timeclock.plan_shift(SIM_DATE.date(),
                                                    e["employee_id"])
             for e in employees}
    rostered = [p for p in plans.values() if p is not None]
    assert rostered, "the roster has no shift at all"

    expected = []
    for emp in employees:
        shift = plans[emp["employee_id"]]
        if shift is not None:
            expected.extend(timeclock._shift_records(emp, shift))

    db = _FakeDB()
    with _capture(db):
        timeclock.generate_day_events(db, SIM_DATE.date(), employees)

    assert len(db.rows) == 4 * len(rostered), (
        "%d rows for %d rostered employees" % (len(db.rows), len(rostered)))
    assert sorted(db.rows) == sorted(expected), (
        "generate_day_events() disagreed with the plan")

    realtime_db, _batches = _realtime_day()
    assert sorted(realtime_db.rows) == sorted(expected), (
        "a realtime day disagreed with the backfill's day for the same date")


# ---------------------------------------------------------------------------
# 2. Writes stay above the ingest cursor
# ---------------------------------------------------------------------------

def test_no_realtime_write_lands_below_an_event_already_written():
    """data-lab watermarks on `event_dt`; a row written below the newest one in
    the table is unreachable for every later incremental read (t_fa8cad04)."""
    db, batches = _realtime_day()
    assert len(batches) > 1, "the day was written in a single batch"

    newest = None
    for batch in batches:
        oldest_in_batch = min(row[3] for row in batch)
        if newest is not None:
            assert oldest_in_batch > newest, (
                "wrote %s below the newest row already on the clock (%s)"
                % (oldest_in_batch, newest))
        newest = max(row[3] for row in batch)


def test_a_shift_we_did_not_see_start_is_moved_forward_not_backdated():
    """A generator restart (or an employee hired mid-day) must not back-date a
    punch below the day's newest row - it moves the shift up instead, keeping
    its shape."""
    db = _FakeDB()
    roster = _employees(60)
    latecomer = {"employee_id": "emp-999", "location_id": "loc-1",
                 "location_type": "store", "status": "active",
                 "department": "store"}
    db.people["emp-999"] = "loc-1"
    with _capture(db):
        # a normal mid-afternoon tick writes the shifts of everyone on the roster
        timeclock.generate_events(db, SIM_DATE.replace(hour=14, minute=5),
                                  roster, LOCATIONS)
        assert db.rows, "no events written by the first tick"
        floor = max(r[3] for r in db.rows)

        # the new employee joins mid-afternoon; their plan for today started 07:00
        before = len(db.rows)
        timeclock.generate_events(db, SIM_DATE.replace(hour=15, minute=0),
                                  roster + [latecomer], LOCATIONS)
        first = db.rows[before:]
        assert first, "the mid-day arrival wrote nothing"
        assert min(r[3] for r in first) > floor, (
            "back-dated a punch below the day's newest row: %s <= %s"
            % (min(r[3] for r in first), floor))

        # ...and their shift plays out from there, in one piece
        cursor = SIM_DATE.replace(hour=15, minute=0)
        end = SIM_DATE.replace(hour=23, minute=59, second=59)
        while cursor < end:
            cursor += TICK
            timeclock.generate_events(db, cursor, roster + [latecomer], LOCATIONS)

    types = sorted(r[2] for r in db.rows if r[0] == "emp-999")
    assert types == ["break_end", "break_start", "clock_in", "clock_out"], types
    for _emp, _loc, etype, dt, _notes in db.rows:
        if _emp == "emp-999":
            assert dt.date() == SIM_DATE.date(), (
                "%s landed on %s" % (etype, dt.date()))
    starts = [r[3] for r in db.rows if r[0] == "emp-999" and r[2] == "break_start"]
    ends = [r[3] for r in db.rows if r[0] == "emp-999" and r[2] == "break_end"]
    assert ends[0] - starts[0] == timedelta(minutes=30), (
        "the moved shift lost its break shape: %s -> %s" % (starts[0], ends[0]))


def test_an_aware_event_dt_from_the_driver_still_pairs():
    """`event_dt` is TIMESTAMPTZ, so a driver hands it back as an *aware*
    datetime while the generator's clock (`datetime.now()`) and the plan are
    naive local time. The first real-PostgreSQL run of this model died on
    exactly that comparison (`can't compare offset-naive and offset-aware`),
    so the read-back is normalised — this pins it."""
    tz = timezone(timedelta(hours=-4))
    roster = _employees(20)
    rows = [(e["employee_id"], "loc-1", "clock_in",
             SIM_DATE.replace(hour=0, minute=5, tzinfo=tz), None)
            for e in roster]
    db = _FakeDB(rows)
    with _capture(db):
        timeclock.generate_events(db, SIM_DATE.replace(hour=23), roster, LOCATIONS)

    for emp in roster:
        types = [r[2] for r in db.rows if r[0] == emp["employee_id"]]
        assert "clock_out" in types, "%s was left open: %s" % (emp["employee_id"], types)
    assert not _pairs_violations(db.rows), _pairs_violations(db.rows)[:5]


def test_a_mid_shift_termination_still_clocks_out():
    """`hr.maybe_terminate_employee()` can fire while someone is on shift. The
    employee drops off the active roster - but half a shift is an unpaired
    punch, so a shift that started today has to be closed."""
    roster = _employees(40)
    db = _FakeDB(people={e["employee_id"]: e["location_id"] for e in roster})
    with _capture(db):
        # noon: everyone whose shift started this morning is on the clock
        timeclock.generate_events(db, SIM_DATE.replace(hour=12), roster, LOCATIONS)
        clocked_in = sorted({r[0] for r in db.rows if r[2] == "clock_in"})
        assert clocked_in, "nobody had clocked in by noon"
        victim = clocked_in[0]

        # 23:00: the victim was terminated at 13:00 and is no longer passed in
        remaining = [e for e in roster if e["employee_id"] != victim]
        timeclock.generate_events(db, SIM_DATE.replace(hour=23), remaining, LOCATIONS)

    seen = [etype for emp, _loc, etype, _dt, _notes in db.rows if emp == victim]
    assert "clock_in" in seen, "%s never clocked in" % victim
    assert "clock_out" in seen, (
        "%s was left with an open shift: %s" % (victim, sorted(seen)))
    assert sorted(seen) == ["break_end", "break_start", "clock_in", "clock_out"], (
        "%s got %s" % (victim, sorted(seen)))


# ---------------------------------------------------------------------------
# 3. data-lab's hard invariant
# ---------------------------------------------------------------------------

def _pairs_violations(rows):
    """data-lab's `assert_timeclock_pairs` predicate, evaluated in memory."""
    per_day = {}
    for emp, _loc, etype, dt, _notes in rows:
        counts = per_day.setdefault((emp, dt.date()), {})
        counts[etype] = counts.get(etype, 0) + 1
    return [(emp, day, counts)
            for (emp, day), counts in per_day.items()
            if counts.get("clock_in", 0) != counts.get("clock_out", 0)
            or counts.get("break_start", 0) != counts.get("break_end", 0)]


def test_a_realtime_day_leaves_no_unpaired_event():
    db, _batches = _realtime_day()
    assert not _pairs_violations(db.rows), _pairs_violations(db.rows)[:5]


def test_a_partial_backfill_day_then_realtime_leaves_no_unpaired_event():
    """The handover the card was found on: the backfill replays hours 0...now at
    its hour boundaries, realtime takes over at `datetime.now()` and ticks the
    rest of the day."""
    db = _FakeDB()
    employees = _employees()
    override = datetime(SIM_DATE.year, SIM_DATE.month, SIM_DATE.day, 13, 40, 0)
    with _capture(db):
        for hour in range(0, 14):
            timeclock.generate_events(db, SIM_DATE.replace(hour=hour),
                                      employees, LOCATIONS)
        cursor = override
        end = SIM_DATE.replace(hour=23, minute=59, second=59)
        while cursor < end:
            timeclock.generate_events(db, cursor, employees, LOCATIONS)
            cursor += TICK
    assert db.rows, "no events written"
    assert not _pairs_violations(db.rows), _pairs_violations(db.rows)[:5]


def test_an_outage_across_midnight_closes_the_day_it_interrupted():
    """The generator has to be running at an event's instant to write it. If it
    is down across midnight, the interrupted day is left open — and that is the
    day data-lab's check judges (`event_date < current_date`), so the first tick
    of the new day has to close it."""
    day0 = SIM_DATE.replace(hour=0)
    day1 = SIM_DATE.replace(hour=0) + timedelta(days=1)
    roster = _employees(40)
    db = _FakeDB()
    with _capture(db):
        # the morning of day 0, then the generator dies and only comes back
        # shortly after midnight
        timeclock.generate_events(db, day0.replace(hour=9), roster, LOCATIONS)
        floor = max(r[3] for r in db.rows)
        assert any(r[2] == "clock_in" for r in db.rows), (
            "no shift was open when the generator died")
        before = len(db.rows)
        timeclock.generate_events(db, day1.replace(hour=1), roster, LOCATIONS)

    closed = [r for r in db.rows[before:] if r[3].date() == day0.date()]
    assert closed, "the interrupted day was left open"
    assert min(r[3] for r in closed) > floor, (
        "the repair was written below the day's newest row: %s <= %s"
        % (min(r[3] for r in closed), floor))
    day0_rows = [r for r in db.rows if r[3].date() == day0.date()]
    assert not _pairs_violations(day0_rows), _pairs_violations(day0_rows)[:5]
