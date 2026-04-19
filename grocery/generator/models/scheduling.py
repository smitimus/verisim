"""
Labor scheduling model — generates planned shifts and resolves actuals.

Flow per simulated day:
  1. generate_weekly_schedule()     — if no schedule exists for the upcoming
                                      7-day window, create one for store employees
  2. resolve_schedule_actuals()     — mark yesterday's scheduled shifts as
                                      completed / called_out / no_show

Schedules are generated 7 days ahead (Monday of next week).
Timeclock events remain the source of truth for actual hours; schedules
represent planned coverage, enabling scheduled-vs-actual analysis.
"""
import random
import logging
from datetime import datetime, date, timedelta, time
from typing import List, Dict

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# Shift templates: (shift_start, shift_end)
SHIFTS = [
    (time(6,  0), time(14, 0)),
    (time(7,  0), time(15, 0)),
    (time(8,  0), time(16, 0)),
    (time(10, 0), time(18, 0)),
    (time(12, 0), time(20, 0)),
    (time(14, 0), time(22, 0)),
    (time(16, 0), time(22, 0)),
]

# Probability an employee is full-time (works ~5 days) vs part-time (~3 days)
FULLTIME_PROB = 0.40

# Outcome probabilities for a scheduled shift
P_COMPLETED   = 0.91
P_CALLED_OUT  = 0.05
# remainder → no_show


def _week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())


def generate_weekly_schedule(conn, sim_date: date,
                              locations: List[Dict],
                              employees: List[Dict]) -> int:
    """
    Creates hr.schedules for the week starting on the Monday on or after sim_date.
    Idempotent: skips if that week already has > 50 scheduled rows.
    Returns number of shifts created.
    """
    target_week = _week_start(sim_date + timedelta(days=7))  # next week's Monday
    week_end    = target_week + timedelta(days=6)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM hr.schedules
            WHERE scheduled_date >= %s AND scheduled_date <= %s
        """, (target_week, week_end))
        if cur.fetchone()[0] > 50:
            return 0  # already scheduled

    store_employees = [e for e in employees if e.get('location_type') == 'store']
    if not store_employees:
        return 0

    days_in_week = [target_week + timedelta(days=i) for i in range(7)]
    records = []

    for emp in store_employees:
        is_fulltime = random.random() < FULLTIME_PROB
        days_count  = 5 if is_fulltime else random.randint(2, 4)
        work_days   = random.sample(days_in_week, min(days_count, len(days_in_week)))

        for work_date in work_days:
            s, e = random.choice(SHIFTS)
            records.append((
                emp['location_id'],
                emp['employee_id'],
                work_date,
                emp.get('department'),
                s, e,
                'scheduled',
            ))

    if not records:
        return 0

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO hr.schedules
                (location_id, employee_id, scheduled_date, department,
                 shift_start, shift_end, status)
            VALUES %s
            ON CONFLICT DO NOTHING
        """, records, template="(%s::uuid, %s::uuid, %s, %s, %s, %s, %s)")
    conn.commit()
    log.info("Scheduled %d shifts for week of %s", len(records), target_week)
    return len(records)


def resolve_schedule_actuals(conn, sim_date: date) -> None:
    """
    Resolves all 'scheduled' shifts for sim_date (yesterday's shifts):
      91% → completed, 5% → called_out, 4% → no_show
    Call this at the start of each simulated day for the previous day.
    """
    yesterday = sim_date - timedelta(days=1)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT schedule_id FROM hr.schedules
            WHERE  scheduled_date = %s AND status = 'scheduled'
        """, (yesterday,))
        ids = [r[0] for r in cur.fetchall()]

    if not ids:
        return

    with conn.cursor() as cur:
        for sched_id in ids:
            roll = random.random()
            if roll < P_COMPLETED:
                status = 'completed'
            elif roll < P_COMPLETED + P_CALLED_OUT:
                status = 'called_out'
            else:
                status = 'no_show'
            cur.execute(
                "UPDATE hr.schedules SET status = %s WHERE schedule_id = %s",
                (status, sched_id)
            )
    conn.commit()
