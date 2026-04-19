"""
Timeclock model — generates employee clock-in/clock-out and break events.

Shift simulation:
- Morning shift: clock_in 6-9am, clock_out 2-5pm
- Afternoon/evening shift: clock_in 2-5pm, clock_out 10pm-12am
- Each shift includes a 30-min break (break_start + break_end at midpoint)

In realtime mode, this generates events for employees whose shift should
be starting or ending at the current simulated time.
In backfill mode, a full day's worth of events is generated per simulated day.
"""
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# Approximate shift windows (hour of day → event type)
MORNING_IN_HOURS = [6, 7, 8, 9]
AFTERNOON_IN_HOURS = [13, 14, 15, 16]
EVENING_IN_HOURS = [17, 18]
SHIFT_LENGTH_HOURS = 8


def generate_events(conn, simulation_dt: datetime, employees: List[Dict],
                    locations: Dict[str, List[Dict]]) -> int:
    """
    Generate timeclock events for the current simulated hour.
    Returns number of events created.

    Queries existing events for today before inserting so each employee only
    receives each event type once per day (prevents duplicate clock_ins from
    repeated 15-minute ticks within the same hour window).
    """
    hour = simulation_dt.hour
    store_employees = [e for e in employees
                        if e['location_type'] == 'store' and e['status'] == 'active']
    if not store_employees:
        return 0

    # Build a map of employee_id → set of event_types already recorded today
    sim_date = simulation_dt.date()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT employee_id::text, event_type
            FROM timeclock.events
            WHERE event_dt::date = %s
        """, (sim_date,))
        today: dict = {}
        for emp_id, etype in cur.fetchall():
            today.setdefault(emp_id, set()).add(etype)

    def has(emp_id, etype):
        return etype in today.get(str(emp_id), set())

    records = []
    sample_size_morning   = max(1, len(store_employees) // (len(MORNING_IN_HOURS) * 3))
    sample_size_afternoon = max(1, len(store_employees) // (len(AFTERNOON_IN_HOURS) * 3))

    if hour in MORNING_IN_HOURS:
        # Clock in employees who haven't clocked in yet today
        eligible = [e for e in store_employees if not has(e['employee_id'], 'clock_in')]
        for emp in random.sample(eligible, min(sample_size_morning, len(eligible))):
            event_dt = simulation_dt.replace(minute=random.randint(0, 59))
            records.append((emp['employee_id'], emp['location_id'], 'clock_in', event_dt, None))

    elif hour in AFTERNOON_IN_HOURS:
        # Clock out morning workers; clock in afternoon workers
        can_out = [e for e in store_employees
                   if has(e['employee_id'], 'clock_in') and not has(e['employee_id'], 'clock_out')]
        can_in  = [e for e in store_employees if not has(e['employee_id'], 'clock_in')]
        for emp in random.sample(can_out, min(sample_size_afternoon, len(can_out))):
            event_dt = simulation_dt.replace(minute=random.randint(0, 59))
            records.append((emp['employee_id'], emp['location_id'], 'clock_out', event_dt, None))
        for emp in random.sample(can_in, min(sample_size_afternoon, len(can_in))):
            event_dt = simulation_dt.replace(minute=random.randint(0, 59))
            records.append((emp['employee_id'], emp['location_id'], 'clock_in', event_dt, None))

    elif hour in EVENING_IN_HOURS:
        # Break events for employees currently on shift; start closing out
        sample_size = max(1, len(store_employees) // 8)
        on_shift    = [e for e in store_employees if has(e['employee_id'], 'clock_in')
                       and not has(e['employee_id'], 'clock_out')]
        need_break_end = [e for e in on_shift
                          if has(e['employee_id'], 'break_start')
                          and not has(e['employee_id'], 'break_end')]
        need_break_start = [e for e in on_shift
                            if not has(e['employee_id'], 'break_start')]
        # Emit break_end for those mid-break, break_start for those who haven't broken yet
        for emp in random.sample(need_break_end, min(sample_size, len(need_break_end))):
            event_dt = simulation_dt.replace(minute=random.randint(0, 59))
            records.append((emp['employee_id'], emp['location_id'], 'break_end', event_dt, None))
        for emp in random.sample(need_break_start, min(sample_size, len(need_break_start))):
            event_dt = simulation_dt.replace(minute=random.randint(0, 59))
            records.append((emp['employee_id'], emp['location_id'], 'break_start', event_dt, None))

    elif 20 <= hour <= 23:
        # Late evening clock-outs for anyone still clocked in
        sample_size = max(1, len(store_employees) // 10)
        can_out = [e for e in store_employees
                   if has(e['employee_id'], 'clock_in') and not has(e['employee_id'], 'clock_out')]
        for emp in random.sample(can_out, min(sample_size, len(can_out))):
            event_dt = simulation_dt.replace(minute=random.randint(0, 59))
            records.append((emp['employee_id'], emp['location_id'], 'clock_out', event_dt, None))

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


def generate_day_events(conn, sim_date, employees: List[Dict]) -> int:
    """
    Generate a full day of timeclock events for backfill mode.
    Each active store employee gets 1-2 shifts worth of events.
    """
    store_employees = [e for e in employees
                        if e['location_type'] == 'store' and e['status'] == 'active']
    if not store_employees:
        return 0

    records = []

    # Maximum start hour for a shift to guarantee clock_out falls on the same calendar
    # day: SHIFT_LENGTH_HOURS (8) + max_jitter (30 min) must end by 23:59.
    # So latest safe start = 23 - 8 = 15, giving a worst-case end of 15:30+8h = 23:30.
    safe_afternoon_hours = [h for h in AFTERNOON_IN_HOURS if h + SHIFT_LENGTH_HOURS <= 23]

    for emp in store_employees:
        # ~80% of employees work on any given day
        if random.random() > 0.80:
            continue

        # Pick a shift (afternoon capped so shift ends same calendar day)
        if random.random() < 0.55:
            in_hour = random.choice(MORNING_IN_HOURS)
        else:
            in_hour = random.choice(safe_afternoon_hours)

        in_dt = datetime(sim_date.year, sim_date.month, sim_date.day,
                          in_hour, random.randint(0, 59))
        out_dt = in_dt + timedelta(hours=SHIFT_LENGTH_HOURS,
                                    minutes=random.randint(-15, 30))

        # Clamp clock_out to 23:30 same day — guards against in_minute + jitter
        # pushing the shift end past midnight regardless of in_hour choice.
        max_out = datetime(sim_date.year, sim_date.month, sim_date.day, 23, 30)
        out_dt = min(out_dt, max_out)

        records.append((emp['employee_id'], emp['location_id'], 'clock_in', in_dt, None))

        # Break at ~midpoint
        break_start = in_dt + timedelta(hours=SHIFT_LENGTH_HOURS // 2 - 1,
                                         minutes=random.randint(0, 30))
        break_end = break_start + timedelta(minutes=30)
        records.append((emp['employee_id'], emp['location_id'], 'break_start', break_start, None))
        records.append((emp['employee_id'], emp['location_id'], 'break_end', break_end, None))

        # Clock out — always same calendar day due to clamp above
        records.append((emp['employee_id'], emp['location_id'], 'clock_out', out_dt, None))

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
