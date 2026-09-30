"""
Regression tests for the break contract — verisim cards t_a24cfbc6 and t_ca6642e0.

data-lab's `assert_timeclock_pairs` is a **hard** invariant (`severity: error`,
"clock in/out pairing is a hard integrity invariant; failures block the
pipeline"), so it fails `dbt_test_staging` on every run while a day holds an
unpaired punch or an unpaired break.

What that invariant is checked against: `where event_date < current_date`, so a
shift or a break that is still open *today* is not a violation — but a finished
day must be complete.

`generate_events()` used to write the two halves of a break in **different**
hourly ticks: the EVENING branch (hours 17 and 18) emitted `break_start` in one
tick and only closed it in the *next* hour that has a break branch. Hour 18 —
the last such hour — has no hour 19 behind it and the dedup map is per calendar
day, so every `break_start` written at 18:xx stayed open **forever** (measured
on the test slot 2026-09-30: 12 employee-days on 2026-09-21, all at
18:00–18:53).

Since t_ca6642e0 the model writes each employee's day from its shift plan, one
instant at a time, and the closing tick needs nothing but the plan — so an hour
that has no later branch can no longer orphan anything: every `break_end` comes
due 30 minutes after its `break_start` and is written by the tick that reaches
it. The two halves are deliberately *not* written as one atomic pair any more:
the pair can only be written once its end is due, which would back-date the
start 30 minutes below data-lab's `event_dt` ingest watermark, where no
incremental read can reach it. See the model docstring.

These tests pin: the halves are 30 minutes apart and ordered, a day that has
finished holds no unpaired event, a repeated tick inside one hour adds nothing,
and a legacy open `break_start` is still closed.
"""
from datetime import datetime, timedelta
from unittest.mock import patch

import grocery.generator.models.timeclock as timeclock

SIM_DATE = datetime(2026, 9, 21)
STORE_EMPLOYEES = 100
BREAK_TYPES = ("break_start", "break_end")
PUNCH_TYPES = ("clock_in", "clock_out")

LOCATIONS = {"stores": [{"location_id": "loc-1"}], "warehouses": []}


def _employees(n=STORE_EMPLOYEES):
    return [
        {"employee_id": "emp-%03d" % i, "location_id": "loc-1",
         "location_type": "store", "status": "active", "department": "store"}
        for i in range(1, n + 1)
    ]


class _FakeCursor:
    """Answers the model's reads from the in-memory rows."""

    def __init__(self, db):
        self._db = db
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        if "FROM timeclock.events" in flat:
            day = params[0]
            rows = [r for r in self._db.rows if r[3].date() == day]
            # answer the projection the caller asked for
            if "event_dt" in flat.split("FROM")[0]:
                self._rows = [(r[0], r[2], r[3]) for r in rows]
            else:
                self._rows = [(r[0], r[2]) for r in rows]
        elif "FROM hr.employees" in flat:
            # the closed-shift lookup: location of an employee who dropped off
            # the active roster while their shift was still open
            self._rows = [(eid, self._db.people[eid])
                          for eid in params[0] if eid in self._db.people]
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)


class _FakeDB:
    """In-memory timeclock.events — rows are the 5-tuples the models insert.

    `people` is the hr.employees side (employee_id -> location_id).
    """

    def __init__(self, rows=None, people=None):
        self.rows = list(rows or [])
        self.people = dict(people or {})
        self.commits = 0

    def cursor(self, *a, **k):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1


def _capture_into(db):
    def _execute_values(cur, sql, records, template=None):
        db.rows.extend(records)

    return _execute_values


def _replay(hours, db=None, employees=None):
    """Replay generate_events() at the end of each listed hour of one day.

    Realtime (and the backfill's partial day) calls generate_events() once per
    tick and writes each planned instant as the clock reaches it, so the tick
    at the end of an hour is what writes that hour's events.
    """
    db = db if db is not None else _FakeDB()
    employees = employees if employees is not None else _employees()
    with patch("grocery.generator.models.timeclock.execute_values",
               side_effect=_capture_into(db)):
        for hour in hours:
            timeclock.generate_events(
                db, SIM_DATE.replace(hour=hour, minute=59, second=59),
                employees, LOCATIONS)
    return db


def _balance(rows, types):
    """Net (start − end) per (employee, day) for the given event types."""
    net = {}
    for employee_id, _location, event_type, event_dt, _notes in rows:
        if event_type in types:
            key = (employee_id, event_dt.date())
            net[key] = net.get(key, 0) + (1 if event_type == types[0] else -1)
    return {k: v for k, v in net.items() if v}


def _open_breaks(rows):
    return _balance(rows, BREAK_TYPES)


def _unpaired_punches(rows):
    return _balance(rows, PUNCH_TYPES)


def _breaks_by_employee(rows):
    out = {}
    for employee_id, _location, event_type, event_dt, _notes in rows:
        if event_type in BREAK_TYPES:
            out.setdefault(employee_id, {}).setdefault(event_type, []).append(event_dt)
    return out


# ---------------------------------------------------------------------------
# The reported failures: the backfill's partial day, and the last break hour
# ---------------------------------------------------------------------------

def test_partial_backfill_day_leaves_no_open_break():
    """The card's shape (t_a24cfbc6): the backfill writes today's hours 0…now in
    one pass through the same per-instant logic realtime uses."""
    db = _replay(range(0, 21))             # 00:00 … 20:59, the reported run
    open_breaks = _open_breaks(db.rows)
    assert db.rows, "no timeclock events generated"
    assert not open_breaks, (
        "break_start with no break_end for %d employee-day(s): %s"
        % (len(open_breaks), sorted(open_breaks)[:5])
    )


def test_the_last_break_hour_no_longer_orphans():
    """Hour 18 specifically: the old code wrote break_start-only there and had
    no hour 19 to close it. The plan closes it 30 minutes later, and the check
    is data-lab's — a *finished* day, so hour 19 is replayed too."""
    db = _replay([0, 18, 19])
    starts = [r for r in db.rows if r[2] == "break_start"]
    ends = [r for r in db.rows if r[2] == "break_end"]
    assert starts, "hour 18 produced no break at all"
    assert len(starts) == len(ends), (
        "hour 18 wrote %d break_start and %d break_end" % (len(starts), len(ends))
    )


def test_break_end_follows_its_break_start():
    """A break is a real 30-minute pair, not two events in random order. Its
    halves are written by different ticks, so the day has to play out first."""
    db = _replay(range(0, 24))
    breaks = _breaks_by_employee(db.rows)
    assert breaks, "no breaks generated"
    for employee_id, ev in breaks.items():
        for start in ev.get("break_start", []):
            assert any(end >= start for end in ev.get("break_end", [])), (
                "%s has break_start %s with no break_end at or after it"
                % (employee_id, start)
            )


def test_full_simulated_day_pairs_everything():
    """End to end over a whole day (as the day rolls over into realtime): same
    punch count in and out, same break count start and end, per employee-day."""
    db = _replay(range(0, 24))
    assert not _unpaired_punches(db.rows), _unpaired_punches(db.rows)
    assert not _open_breaks(db.rows), _open_breaks(db.rows)


def test_break_length_is_thirty_minutes():
    db = _replay(range(0, 24))
    breaks = _breaks_by_employee(db.rows)
    pairs = [(e, min(ev["break_start"]), min(ev["break_end"]))
             for e, ev in breaks.items()
             if ev.get("break_start") and ev.get("break_end")]
    assert pairs, "no complete break pair to measure"
    for employee_id, start, end in pairs:
        assert end - start == timedelta(minutes=30), (
            "%s break ran %s" % (employee_id, end - start))


def test_no_break_outlives_its_day():
    """A break must start and end on the same simulated day — an end pushed past
    midnight would split the pair across two `event_date`s downstream."""
    db = _replay(range(0, 24))
    for employee_id, _location, event_type, event_dt, _notes in db.rows:
        if event_type in BREAK_TYPES:
            assert event_dt.date() == SIM_DATE.date(), (
                "%s %s landed on %s" % (employee_id, event_type, event_dt.date())
            )


# ---------------------------------------------------------------------------
# Repair: state written by an older build (or a tick killed mid-break)
# ---------------------------------------------------------------------------

def _clocker_in_db(open_break=False):
    """A day where everyone is on shift — the state an older build leaves."""
    employees = _employees()
    rows = [(e["employee_id"], "loc-1", "clock_in", SIM_DATE.replace(hour=7), None)
            for e in employees]
    if open_break:
        rows.append(("emp-001", "loc-1", "break_start",
                     SIM_DATE.replace(hour=18), None))
    return _FakeDB(rows), employees


def test_an_open_break_from_an_earlier_build_is_closed():
    """A slot that already holds an open break_start — the card's caveat — gets
    it closed by the next evening tick instead of keeping it forever."""
    db, employees = _clocker_in_db(open_break=True)
    _replay([18], db=db, employees=employees)
    assert not _open_breaks(db.rows), (
        "the open break_start was not closed: %s" % _open_breaks(db.rows)
    )


def test_no_breaks_are_emitted_when_nobody_is_on_shift():
    """The repair must not invent a break_end for someone who never broke."""
    db, employees = _clocker_in_db(open_break=False)
    _replay([18, 23], db=db, employees=employees)
    for employee_id, ev in _breaks_by_employee(db.rows).items():
        assert ev.get("break_start", []) and ev.get("break_end", []), (
            "%s got a %s with no counterpart" % (employee_id, sorted(ev))
        )


def test_breaks_are_not_duplicated_by_repeated_ticks_in_one_hour():
    """The dedup map is what makes the extra break_end safe — repeated ticks in
    the same hour must not stack a second break on an employee."""
    db, employees = _clocker_in_db()
    _replay([17, 17, 17, 17, 17, 23], db=db, employees=employees)
    breaks = _breaks_by_employee(db.rows)
    assert breaks, "no breaks generated"
    for employee_id, ev in breaks.items():
        assert len(ev.get("break_start", [])) <= 1, "%s broke twice in a day" % employee_id
        assert len(ev.get("break_end", [])) == len(ev.get("break_start", []))


def test_an_employee_takes_at_most_one_break_in_the_evening():
    """The plan gives each employee one break; the dedup must not hand anyone a
    second one in the same day."""
    db = _replay([0, 17, 18])
    breaks = _breaks_by_employee(db.rows)
    assert breaks, "no breaks generated"
    for employee_id, ev in breaks.items():
        assert len(ev.get("break_start", [])) <= 1, "%s broke twice in a day" % employee_id


# ---------------------------------------------------------------------------
# generate_day_events() — the other half of the contract
# ---------------------------------------------------------------------------

def test_full_day_backfill_pairs_breaks_and_punches():
    """The backfill's complete days go through generate_day_events(); its four
    events per working employee are the contract the realtime path matches."""
    db = _FakeDB()
    with patch("grocery.generator.models.timeclock.execute_values",
               side_effect=_capture_into(db)):
        timeclock.generate_day_events(db, SIM_DATE.date(), _employees())
    assert db.rows, "no events generated"
    assert not _open_breaks(db.rows), _open_breaks(db.rows)
    assert not _unpaired_punches(db.rows), _unpaired_punches(db.rows)
    for employee_id, _location, event_type, event_dt, _notes in db.rows:
        assert event_dt.date() == SIM_DATE.date(), (
            "%s %s landed on %s" % (employee_id, event_type, event_dt.date()))
