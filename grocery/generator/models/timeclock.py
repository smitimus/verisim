"""
Timeclock model — employee clock-in/clock-out and break events.

A simulated day is a *shift plan*, not a per-tick lottery. For every active
store employee, `plan_shift()` decides — purely from (date, employee_id) —
whether that employee works that day (~80% do) and, if so, the four instants of
their shift:

    clock_in      06-09 / 00-05 / 13-15   (55% / 10% / ~35% of the roster)
    break_start   clock_in + 3h + 0-30min
    break_end     break_start + 30min
    clock_out     clock_in + 8h ± jitter, never past 23:30

Two callers write that same plan, and both agree on the day because the plan is
a pure function — no shared RNG stream, no stored state, the same answer on any
tick, in any order, after any restart:

* `generate_day_events()` — the backfill writes a whole past day in one pass.
* `generate_events()`     — realtime (and the backfill's partial day) writes
  each instant as the clock reaches it, one tick at a time. Idempotency comes
  from the dedup SELECT below: an event is inserted only once, and only once
  its instant has passed.

Why the plan (t_ca6642e0)
-------------------------
Realtime used to re-sample a fraction of the *whole* pool on every tick against
that per-calendar-day dedup. `EARLY_MORNING_IN_HOURS` clocked in
`len(store)//6` employees per tick, so with a 30-second tick the entire roster
was clocked in — and mostly back out — within minutes of midnight; from hour 1
onwards `can_in`, `can_out` and `on_shift` were all empty, so the afternoon
branch clocked out nobody, the EVENING branch emitted no break at all, and
every punch of the day carried hour 0. Measured on the dev slot 2026-09-30:
each of 2026-09-21 … 09-30 held ~100 `clock_in`, ~100 `clock_out`, **0**
`break_start` and **0** `break_end`, all at hour 00, while the backfill's
complete days held 4 events per working employee spread across the day.
`mart_attendance_summary` therefore saw a 0-hour shift for essentially everyone
and `break_hours = 0` on every day after the seed day.

Why not an accelerated simulated clock: the realtime clock IS the wall clock —
`main.py` passes `datetime.now()` — and data-lab's freshness windows, its
`event_dt` ingest watermark and the backfill's notion of "today" are all built on
that. Moving realtime onto a simulated clock changes what "today" means for
every model and every ingest window; making the day a plan fixes the shape of
the day and leaves the clock where it is.

Writes are monotone in event_dt
-------------------------------
Everything below follows from one constraint: data-lab reads this table
incrementally with its cursor on `event_dt`, so a row written *below* the newest
`event_dt` already on the clock can never be read again — the later incremental
run starts at `MAX(event_dt)` and looks forward (t_fa8cad04, and see the
"two windows" table in AGENTS.md: `event_dt` is business time and backdated,
the right delta clock is the insert clock this route does not surface yet).

* An event is written at its planned instant, when that instant arrives; a
  tick that misses one still writes it late, and `_add()` floors every write
  above the day's newest row, so nothing is ever written into that blind spot.
* A break is written in two halves 30 minutes apart, *not* as one atomic pair
  (which is what the previous build did): the pair can only be written once
  its `break_end` is due, and that would back-date `break_start` 30 minutes
  below the cursor — data-lab would receive every `break_end` and no
  `break_start`, failing the very `assert_timeclock_pairs` the atomic pair was
  introduced to fix (t_a24cfbc6). The tick loop runs every 30s, and the
  closing tick needs nothing but the plan, so a missed `break_end` is written
  by the next tick; data-lab's check only looks at days before today, and a
  break's halves are at most 30 minutes apart, so nothing is ever left open on
  a finished day by a running generator.
* A shift whose start was never observed at its instant (a generator restart,
  an employee hired mid-day) is moved as a whole — see `_due_records()` — to
  just above the day's newest row rather than back-dated below it.
* An employee with an open punch or open break on today's clock is always
  closed out, even if the plan does not roster them for the day, and even if
  they have dropped off the active roster (terminated mid-shift). Half a shift
  is an unpaired punch, which is a hard data-lab error.
* A generator that was down across midnight left the *previous* day open, and
  that is the day data-lab's check judges (`event_date < current_date`). The
  first tick of the new day closes whatever is still open on it, before any of
  the new day's rows exist — after that, a timestamp on yesterday would sit
  below today's rows and no incremental read could reach it.
"""
import hashlib
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# Shift windows: the hour of day an employee clocks in.
MORNING_IN_HOURS = [6, 7, 8, 9]
EARLY_MORNING_IN_HOURS = list(range(0, 6))     # overnight stocking / prep
AFTERNOON_IN_HOURS = [13, 14, 15, 16]
SHIFT_LENGTH_HOURS = 8
BREAK_LENGTH_MINUTES = 30

# Latest start that still ends the shift on the same calendar day: SHIFT_LENGTH
# (8h) + max jitter (30min) from hour 15 ends by 23:30.
SAFE_AFTERNOON_HOURS = [h for h in AFTERNOON_IN_HOURS
                        if h + SHIFT_LENGTH_HOURS <= 23]

# Share of the roster that works a given day, and the mix of shift windows.
WORK_PROBABILITY = 0.80
MORNING_SHARE = 0.55
EARLY_MORNING_SHARE = 0.10

EVENT_TYPES = ("clock_in", "break_start", "break_end", "clock_out")


# ---------------------------------------------------------------------------
# The shift plan
# ---------------------------------------------------------------------------

def _plan_seed(sim_date, employee_id) -> int:
    """A stable int seed for one (day, employee) — never the process hash."""
    key = "%s|%s" % (sim_date.isoformat(), employee_id)
    return int.from_bytes(hashlib.sha256(key.encode("utf-8")).digest()[:8], "big")


def _day_end(sim_date) -> datetime:
    return datetime(sim_date.year, sim_date.month, sim_date.day, 23, 59, 59)


def _shift_limit(sim_date) -> datetime:
    """Latest instant a planned event may carry: a shift must not cross
    midnight, or its employee-day would be split in two downstream."""
    return datetime(sim_date.year, sim_date.month, sim_date.day, 23, 30)


def plan_shift(sim_date, employee_id) -> Optional[Dict[str, datetime]]:
    """The four instants of one employee's shift on `sim_date`, or None when
    that employee does not work that day (~20% of employee-days).

    Pure and deterministic in (sim_date, employee_id): every caller, on every
    tick, in any order, derives the same shift — which is what lets realtime and
    the backfill write the same day.
    """
    rng = random.Random(_plan_seed(sim_date, employee_id))
    if rng.random() > WORK_PROBABILITY:
        return None

    roll = rng.random()
    if roll < MORNING_SHARE:
        in_hour = rng.choice(MORNING_IN_HOURS)
    elif roll < MORNING_SHARE + EARLY_MORNING_SHARE:
        in_hour = rng.choice(EARLY_MORNING_IN_HOURS)
    else:
        in_hour = rng.choice(SAFE_AFTERNOON_HOURS)

    clock_in = datetime(sim_date.year, sim_date.month, sim_date.day,
                        in_hour, rng.randint(0, 59))
    clock_out = clock_in + timedelta(hours=SHIFT_LENGTH_HOURS,
                                     minutes=rng.randint(-15, 30))
    clock_out = min(clock_out, _shift_limit(sim_date))
    break_start = clock_in + timedelta(hours=SHIFT_LENGTH_HOURS // 2 - 1,
                                       minutes=rng.randint(0, 30))
    return {
        "clock_in": clock_in,
        "break_start": break_start,
        "break_end": break_start + timedelta(minutes=BREAK_LENGTH_MINUTES),
        "clock_out": clock_out,
    }


def _shift_records(employee, shift: Dict[str, datetime]) -> List[tuple]:
    """The insert rows of one planned shift, in clock order."""
    return [(employee['employee_id'], employee['location_id'], etype,
             shift[etype], None)
            for etype in EVENT_TYPES if etype in shift]


# ---------------------------------------------------------------------------
# Reading the day's clock back
# ---------------------------------------------------------------------------

def _local_naive(dt: datetime) -> datetime:
    """One time frame for the comparisons below.

    `timeclock.events.event_dt` is `TIMESTAMPTZ`, so the driver hands it back as
    an *aware* datetime, while the generator's clock (`datetime.now()` in
    `main.py`) and `plan_shift()` are naive local time — the same wall clock the
    write path sends. Compare and store them as naive local.
    """
    return dt.astimezone().replace(tzinfo=None) if dt.tzinfo is not None else dt


def _events_on(conn, sim_date) -> Dict[str, Dict[str, datetime]]:
    """employee_id -> {event_type: event_dt} for one simulated day."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT employee_id::text, event_type, event_dt
            FROM timeclock.events
            WHERE event_dt::date = %s
        """, (sim_date,))
        today: Dict[str, Dict[str, datetime]] = {}
        for emp_id, etype, event_dt in cur.fetchall():
            today.setdefault(emp_id, {})[etype] = _local_naive(event_dt)
    return today


def _locations_for(conn, employee_ids: Iterable[str]) -> List[Tuple[str, str]]:
    """(employee_id, location_id) for employees no longer on the roster."""
    employee_ids = list(employee_ids)
    if not employee_ids:
        return []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT employee_id::text, location_id::text
            FROM hr.employees
            WHERE employee_id = ANY(%s::uuid[])
        """, (employee_ids,))
        return [(row[0], row[1]) for row in cur.fetchall()]


def _insert(conn, records: List[tuple]) -> int:
    if not records:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO timeclock.events
                (employee_id, location_id, event_type, event_dt, notes)
            VALUES %s
        """, records, template="(%s::uuid,%s::uuid,%s,%s,%s)")
    conn.commit()
    return len(records)


# ---------------------------------------------------------------------------
# Realtime (and the backfill's partial day)
# ---------------------------------------------------------------------------

def _due_records(employee_id: str, location_id, sim_date,
                 seen: Dict[str, datetime], day_max: Optional[datetime],
                 simulation_dt: datetime) -> List[tuple]:
    """The events one employee's simulated day owes as of `simulation_dt`.

    `seen` is what that employee already has on today's clock, `day_max` the
    newest `event_dt` anywhere on it (see the module docstring).
    """
    out: List[tuple] = []

    def add(event_type, planned_dt):
        # Floored above the day's newest row: a row written below it is in the
        # blind spot of every later incremental read (data-lab watermarks on
        # event_dt), so a late event moves up rather than back-dating.
        if day_max is not None and planned_dt <= day_max:
            planned_dt = day_max + timedelta(seconds=1)
        if planned_dt <= simulation_dt:
            out.append((employee_id, location_id, event_type, planned_dt, None))

    shift = plan_shift(sim_date, employee_id)
    if shift is None:
        # Not rostered today: never invent a shift for them, only close what
        # today's clock already holds open (an older build's shape).
        open_in = seen.get('clock_in')
        if seen.get('break_start') is not None and 'break_end' not in seen:
            add('break_end', seen['break_start'] + timedelta(minutes=BREAK_LENGTH_MINUTES))
        if open_in is not None and 'clock_out' not in seen:
            add('clock_out', min(open_in + timedelta(hours=SHIFT_LENGTH_HOURS),
                                 _day_end(sim_date)))
        return out

    written_in = seen.get('clock_in')
    if written_in is not None:
        # The shift is wherever its clock_in was written — which is what keeps
        # a shift moved by an earlier tick (below) in one piece.
        offset = written_in - shift['clock_in']
    elif day_max is not None and shift['clock_in'] <= day_max:
        # We were not looking when this shift started (a generator restart, an
        # employee hired mid-day): move the whole shift — break at the midpoint,
        # out 8h later — to just above the day's newest row. Back-dating it is
        # what would hide its punches from the incremental read.
        offset = day_max - shift['clock_in'] + timedelta(seconds=1)
    else:
        offset = timedelta(0)

    if offset:
        shift = {etype: dt + offset for etype, dt in shift.items()}
        shift['clock_out'] = min(shift['clock_out'], _day_end(sim_date))
        # A shift moved forward can end up too short to hold a break; a day
        # with no break still pairs (0 == 0), one that crossed midnight does not.
        if not (shift['clock_in'] <= shift['break_start']
                and shift['break_end'] <= shift['clock_out']):
            shift.pop('break_start', None)
            shift.pop('break_end', None)

    if written_in is None:
        add('clock_in', shift['clock_in'])
    # A break is two halves 30 minutes apart, each written when it comes due
    # (see the module docstring on why they are not written as one atomic pair);
    # both are only ever written together-or-not-at-all, never one alone.
    if 'break_start' in shift and 'break_end' not in seen:
        if 'break_start' not in seen:
            add('break_start', shift['break_start'])
        add('break_end', shift['break_end'])
    if 'clock_out' not in seen:
        add('clock_out', shift['clock_out'])
    return out


def _day_is_open(events: Dict[str, datetime]) -> bool:
    """True when this employee-day is missing half of a punch or of a break."""
    if ('clock_in' in events) != ('clock_out' in events):
        return True
    return ('break_start' in events) != ('break_end' in events)


def _close_previous_day(conn, sim_date, store_employees: List[Dict],
                        simulation_dt: datetime) -> List[tuple]:
    """Close the previous simulated day's open punches and breaks.

    Only reachable from the first tick of a day (see `generate_events`), which
    is the last moment it can work: once today's own rows exist, a timestamp on
    yesterday would sit below them and no incremental read would ever see it.
    """
    prev_date = sim_date - timedelta(days=1)
    prev = _events_on(conn, prev_date)
    open_ids = {eid for eid, events in prev.items() if _day_is_open(events)}
    if not open_ids:
        return []

    prev_max = max((dt for events in prev.values() for dt in events.values()),
                   default=None)
    location = {str(e['employee_id']): e['location_id'] for e in store_employees}
    location.update(dict(_locations_for(conn, sorted(open_ids - set(location)))))

    records: List[tuple] = []
    for employee_id in sorted(open_ids):
        if location.get(employee_id) is None:
            continue
        # Re-derive the shift from the plan and write whatever half is missing,
        # floored above the previous day's newest row: a day with an unpaired
        # punch is what data-lab's hard check fails on.
        records.extend(_due_records(employee_id, location[employee_id], prev_date,
                                    prev[employee_id], prev_max, simulation_dt))
    if records:
        log.info("Closed %d open timeclock event(s) on %s (generator was down "
                 "across the day boundary)", len(records), prev_date)
    return records


def generate_events(conn, simulation_dt: datetime, employees: List[Dict],
                    locations: Dict[str, List[Dict]]) -> int:
    """
    Write the planned events that have come due on `simulation_dt`'s day.
    Returns the number of events created.

    Called every realtime tick (sub-minute) and once per hour by the backfill's
    partial day. Reads today's clock back first, so an event is written once and
    only once its instant has passed.
    """
    sim_date = simulation_dt.date()
    store_employees = [e for e in employees
                       if e['location_type'] == 'store' and e['status'] == 'active']
    if not store_employees:
        return 0

    seen = _events_on(conn, sim_date)

    records: List[tuple] = []
    if not seen:
        # First tick of a new simulated day. A generator that was down across
        # midnight has left the *previous* day's shifts open — and that is the
        # day data-lab's `assert_timeclock_pairs` judges (`event_date <
        # current_date`). Close them now, before today's rows exist.
        records.extend(_close_previous_day(conn, sim_date, store_employees,
                                           simulation_dt))

    day_max = max((dt for events in seen.values() for dt in events.values()),
                  default=None)

    roster = set()
    for emp in store_employees:
        employee_id = str(emp['employee_id'])
        roster.add(employee_id)
        records.extend(_due_records(employee_id, emp['location_id'], sim_date,
                                    seen.get(employee_id, {}), day_max, simulation_dt))

    # Today's clock also holds employees who are no longer on the active roster:
    # `hr.maybe_terminate_employee()` can fire while someone is on shift, and a
    # shift that started today still has to be closed — half a shift is an
    # unpaired punch, a hard error downstream.
    off_roster = sorted(set(seen) - roster)
    for employee_id, location_id in _locations_for(conn, off_roster):
        records.extend(_due_records(employee_id, location_id, sim_date,
                                    seen[employee_id], day_max, simulation_dt))

    return _insert(conn, records)


# ---------------------------------------------------------------------------
# Backfill — complete days
# ---------------------------------------------------------------------------

def generate_day_events(conn, sim_date, employees: List[Dict]) -> int:
    """
    Write every event of one simulated day in one pass.
    Each working employee gets one shift's worth of events.
    """
    store_employees = [e for e in employees
                       if e['location_type'] == 'store' and e['status'] == 'active']
    if not store_employees:
        return 0

    records: List[tuple] = []
    for emp in store_employees:
        shift = plan_shift(sim_date, emp['employee_id'])
        if shift is None:
            continue
        records.extend(_shift_records(emp, shift))
    return _insert(conn, records)
