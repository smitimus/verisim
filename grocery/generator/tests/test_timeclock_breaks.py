"""
Regression tests for verisim card t_a24cfbc6 — a break must never be left open.

data-lab's `assert_timeclock_pairs` is a **hard** invariant (`severity: error`,
"clock in/out pairing is a hard integrity invariant; failures block the
pipeline"), so it fails `dbt_test_staging` on every run while any employee-day
holds a `break_start` with no `break_end`.

`generate_events()` used to write the two halves of a break in **different**
hourly ticks: the EVENING branch (hours 17 and 18) emitted `break_start` in one
tick and only closed it in the *next* hour that has a break branch. Hour 18 —
the last such hour — has no hour 19 behind it and the dedup map is per calendar
day, so every `break_start` written at 18:xx stayed open forever.

Measured on the `test` slot (192.168.3.7, image digest 39f53c44… = verisim
37c8d7d) on 2026-09-30: 09-21 (the seed day, whose hours 0…20 the backfill
writes in one pass) held 24 `break_start` and 12 `break_end`; exactly the 12 at
18:00–18:53 were unpaired, and they are the 12 employee-days the card lists.
`generate_day_events()` already wrote breaks as a pair — these tests pin the
realtime path to the same contract, and pin the repair of a legacy open break.

The fake below is an in-memory `timeclock.events`: it answers the dedup SELECT
by date and absorbs the bulk INSERT, so a whole simulated day can be replayed
without a database.
"""
from datetime import datetime, timedelta
from unittest.mock import patch
import random

import grocery.generator.models.timeclock as timeclock

SIM_DATE = datetime(2026, 9, 21)
STORE_EMPLOYEES = 100          # → sample_size = 100 // 8 = 12, as on the slot
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
    """Answers generate_events()' dedup SELECT from the in-memory rows."""

    def __init__(self, db):
        self._db = db
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if "FROM timeclock.events" in " ".join(sql.split()):
            day = params[0]
            self._rows = [(r[0], r[2]) for r in self._db.rows if r[3].date() == day]
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)


class _FakeDB:
    """In-memory timeclock.events — rows are the 5-tuples the models insert."""

    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.commits = 0

    def cursor(self, *a, **k):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1


def _capture_into(db):
    def _execute_values(cur, sql, records, template=None):
        db.rows.extend(records)

    return _execute_values


def _replay(hours, db=None, employees=None, seed=1234):
    """Replay generate_events() over the given hours of one simulated day."""
    random.seed(seed)
    db = db if db is not None else _FakeDB()
    employees = employees if employees is not None else _employees()
    with patch("grocery.generator.models.timeclock.execute_values",
               side_effect=_capture_into(db)):
        for hour in hours:
            timeclock.generate_events(db, SIM_DATE.replace(hour=hour),
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
# The reported failure: the backfill's partial day
# ---------------------------------------------------------------------------

def test_partial_backfill_day_leaves_no_open_break():
    """The card's shape: the backfill writes today's hours 0…now.hour in one
    pass, so hour 18 (the last hour with a break branch) is reached and its
    break_starts are never closed."""
    db = _replay(range(0, 21))             # 00:00 … 20:00, the reported run
    open_breaks = _open_breaks(db.rows)
    assert db.rows, "no timeclock events generated"
    assert not open_breaks, (
        "break_start with no break_end for %d employee-day(s): %s"
        % (len(open_breaks), sorted(open_breaks)[:5])
    )


def test_the_last_break_hour_no_longer_orphans():
    """Hour 18 specifically: old code wrote break_start-only there and had no
    hour 19 to close it."""
    db = _replay([0, 18])
    starts = [r for r in db.rows if r[2] == "break_start"]
    ends = [r for r in db.rows if r[2] == "break_end"]
    assert starts, "hour 18 produced no break at all"
    assert len(starts) == len(ends), (
        "hour 18 wrote %d break_start and %d break_end" % (len(starts), len(ends))
    )


def test_break_end_follows_its_break_start():
    """A break is a real 30-minute pair, not two events in random order."""
    db = _replay([0, 17, 18])
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
    db = _replay([0, 17, 18])
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
    db = _replay([0, 17, 18])
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
    _replay([18], db=db, employees=employees, seed=7)
    assert not _open_breaks(db.rows), (
        "the open break_start was not closed: %s" % _open_breaks(db.rows)
    )


def test_no_breaks_are_emitted_when_nobody_is_on_shift():
    """The repair must not invent a break_end for someone who never broke."""
    db, employees = _clocker_in_db(open_break=False)
    _replay([18], db=db, employees=employees, seed=7)
    for employee_id, ev in _breaks_by_employee(db.rows).items():
        assert ev.get("break_start", []) and ev.get("break_end", []), (
            "%s got a %s with no counterpart" % (employee_id, sorted(ev))
        )


def test_breaks_are_not_duplicated_by_repeated_ticks_in_one_hour():
    """The dedup map is what makes the extra break_end safe — repeated ticks in
    the same hour must not stack a second break on an employee."""
    db, employees = _clocker_in_db()
    _replay([17, 17, 17, 17, 17], db=db, employees=employees, seed=11)
    breaks = _breaks_by_employee(db.rows)
    assert breaks, "no breaks generated"
    for employee_id, ev in breaks.items():
        assert len(ev.get("break_start", [])) <= 1, "%s broke twice in a day" % employee_id
        assert len(ev.get("break_end", [])) == len(ev.get("break_start", []))


def test_an_employee_takes_at_most_one_break_in_the_evening():
    """Hours 17 and 18 both offer a break; the dedup must not hand one
    employee two in the same day."""
    db = _replay([0, 17, 18])
    breaks = _breaks_by_employee(db.rows)
    assert breaks, "no breaks generated"
    for employee_id, ev in breaks.items():
        assert len(ev.get("break_start", [])) <= 1, "%s broke twice in a day" % employee_id


# ---------------------------------------------------------------------------
# generate_day_events() — the other half of the contract
# ---------------------------------------------------------------------------

def test_full_day_backfill_pairs_breaks_and_punches():
    """The backfill's complete days go through generate_day_events(); its pairs
    are the contract the realtime path now matches."""
    db = _FakeDB()
    with patch("grocery.generator.models.timeclock.execute_values",
               side_effect=_capture_into(db)):
        random.seed(3)
        timeclock.generate_day_events(db, SIM_DATE.date(), _employees())
    assert db.rows, "no events generated"
    assert not _open_breaks(db.rows), _open_breaks(db.rows)
    assert not _unpaired_punches(db.rows), _unpaired_punches(db.rows)
    for employee_id, _location, event_type, event_dt, _notes in db.rows:
        assert event_dt.date() == SIM_DATE.date(), (
            "%s %s landed on %s" % (employee_id, event_type, event_dt.date()))
