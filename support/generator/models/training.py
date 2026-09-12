"""
Training model — courses, assignments, completions.

Agents get assigned training when:
  * they are hired (onboarding)
  * they accumulate detractor surveys (qa_finding)
  * scenario events fire (new_feature on product_launch, recall_event on outage)
  * a manager requests (scheduled sweep)
Completions progress probabilistically; overdue detection runs daily.
"""
import random
import logging
from datetime import timedelta

from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)

COURSES = [
    ('ONB-101', 'CarePoint Onboarding: Systems & Tools', 'onboarding', 120, 85,
     True, 'agent,team_lead'),
    ('ONB-102', 'Onboarding: Empathy & Communication', 'onboarding', 90, 80,
     True, 'agent'),
    ('PRD-210', 'Product Deep-Dive: Billing Platform', 'product', 60, 80,
     False, 'agent'),
    ('PRD-215', 'Product Deep-Dive: Mobile Apps', 'product', 60, 80,
     False, 'agent'),
    ('CMP-301', 'Compliance: PCI-DSS Handling', 'compliance', 75, 90,
     True, 'agent,team_lead'),
    ('CMP-305', 'Compliance: Data Privacy (GDPR/CCPA)', 'compliance', 60, 90,
     True, 'agent,management'),
    ('SFT-410', 'De-escalating Angry Customers', 'soft_skills', 45, 75,
     False, 'agent'),
    ('SFT-420', 'Chat Conciseness & Multitasking', 'soft_skills', 40, 75,
     False, 'agent'),
    ('SYS-501', 'Ticket Queue Management', 'system', 50, 80,
     False, 'agent,team_lead'),
    ('SYS-505', 'ACD & Telephony Troubleshooting', 'system', 55, 80,
     False, 'agent,team_lead'),
]

TRIGGER_DUE_HOURS = {
    'onboarding': 7 * 24,
    'qa_finding': 5 * 24,
    'new_feature': 10 * 24,
    'recall_event': 3 * 24,
    'scheduled': 21 * 24,
    'manager_request': 14 * 24,
}


def seed_courses(conn, cfg: Config):
    """Idempotent course catalog seed. Returns list of course dicts."""
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO training.courses
                (code, name, category, duration_minutes, pass_score,
                 is_mandatory, target_departments)
            VALUES %s
            ON CONFLICT (code) DO NOTHING
        """, COURSES)
        conn.commit()
        cur.execute("""
            SELECT course_id::text, code, name, category, duration_minutes,
                   is_mandatory, target_departments
            FROM training.courses WHERE is_active = TRUE
        """)
        return [{'course_id': r[0], 'code': r[1], 'name': r[2], 'category': r[3],
                 'duration_minutes': r[4], 'is_mandatory': r[5],
                 'targets': (r[6] or '').split(',')} for r in cur.fetchall()]


def assign_onboarding(conn, cfg, sim_dt, agent_id):
    """New hires get mandatory onboarding + one compliance course."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT course_id::text, code, target_departments, duration_minutes
            FROM training.courses
            WHERE is_active AND is_mandatory AND category IN ('onboarding', 'compliance')
        """)
        rows = cur.fetchall()
        ins = []
        for cid, code, targets, dur in rows:
            if 'agent' not in (targets or ''):
                continue
            due = sim_dt + timedelta(hours=TRIGGER_DUE_HOURS['onboarding'])
            ins.append((agent_id, cid, sim_dt, due, 'assigned', 'onboarding'))
        if ins:
            execute_values(cur, """
                INSERT INTO training.assignments
                    (agent_id, course_id, assigned_dt, due_dt, status, trigger_reason)
                VALUES %s
            """, ins)
            conn.commit()
    return len(ins)


def assign_missing_onboarding(conn, cfg: Config, sim_dt):
    """Give every active agent lacking an onboarding assignment one (idempotent).

    Covers both the seeded cohort and hires made by maybe_hire_employee.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT e.employee_id::text FROM hr.employees e
            WHERE e.status = 'active' AND e.department = 'agent'
              AND NOT EXISTS (
                  SELECT 1 FROM training.assignments a
                  WHERE a.agent_id = e.employee_id
                    AND a.trigger_reason = 'onboarding')
        """)
        ids = [r[0] for r in cur.fetchall()]
    for eid in ids:
        assign_onboarding(conn, cfg, sim_dt, eid)
    return len(ids)


def generate_training(conn, cfg: Config, sim_dt, ctx, courses, agents,
                      detractor_agent_ids):
    """
    Daily-ish training model:
      * qa_finding assignments for detractor agents
      * scenario-triggered assignments (new_feature / recall_event)
      * progress: assigned -> in_progress -> completed with scores
      * overdue detection
    Returns (assigned, completed).
    """
    assigned = 0
    completed = 0
    soft = [c for c in courses if c['category'] in ('soft_skills', 'system', 'product')]
    active_agents = [a for a in agents if a['department'] == 'agent']

    with conn.cursor() as cur:
        # 1) QA remedial: agents with detractor clusters get De-escalation
        deesc = next((c for c in courses if c['code'] == 'SFT-410'), None)
        if deesc and detractor_agent_ids and random.random() < cfg.training.qa_assignment_rate * 8:
            target = random.choice(detractor_agent_ids)
            due = sim_dt + timedelta(hours=TRIGGER_DUE_HOURS['qa_finding'])
            cur.execute("""
                INSERT INTO training.assignments
                    (agent_id, course_id, assigned_dt, due_dt, status, trigger_reason)
                SELECT %s, %s, %s, %s, 'assigned', 'qa_finding'
                WHERE NOT EXISTS (
                    SELECT 1 FROM training.assignments
                    WHERE agent_id = %s::uuid AND course_id = %s::uuid
                      AND status IN ('assigned','in_progress')
                )
            """, (target, deesc['course_id'], sim_dt, due, target, deesc['course_id']))
            assigned += cur.rowcount if cur.rowcount > 0 else 0

        # 2) Scenario triggers
        tag = ctx.scenario_tag or ''
        if 'launch' in tag and random.random() < 0.4:
            feat = random.choice([c for c in courses if c['category'] == 'product'])
            agent = random.choice(active_agents)
            due = sim_dt + timedelta(hours=TRIGGER_DUE_HOURS['new_feature'])
            cur.execute("""
                INSERT INTO training.assignments
                    (agent_id, course_id, assigned_dt, due_dt, status, trigger_reason)
                SELECT %s, %s, %s, %s, 'assigned', 'new_feature'
                WHERE NOT EXISTS (
                    SELECT 1 FROM training.assignments
                    WHERE agent_id = %s::uuid AND course_id = %s::uuid
                      AND trigger_reason = 'new_feature'
                )
            """, (agent['employee_id'], feat['course_id'], sim_dt, due,
                  agent['employee_id'], feat['course_id']))
            assigned += max(0, cur.rowcount)
        if 'outage' in tag and random.random() < 0.5:
            refresher = random.choice([c for c in courses if c['category'] == 'system'])
            for _ in range(random.randint(1, 3)):
                agent = random.choice(active_agents)
                due = sim_dt + timedelta(hours=TRIGGER_DUE_HOURS['recall_event'])
                cur.execute("""
                    INSERT INTO training.assignments
                        (agent_id, course_id, assigned_dt, due_dt, status, trigger_reason)
                    SELECT %s, %s, %s, %s, 'assigned', 'recall_event'
                    WHERE NOT EXISTS (
                        SELECT 1 FROM training.assignments
                        WHERE agent_id = %s::uuid AND course_id = %s::uuid
                          AND status IN ('assigned','in_progress')
                    )
                """, (agent['employee_id'], refresher['course_id'], sim_dt, due,
                      agent['employee_id'], refresher['course_id']))
                assigned += max(0, cur.rowcount)

        # 3) Progress open assignments (only ones that exist as of sim_dt —
        #    seed-time onboarding carries real-now timestamps during backfill)
        cur.execute("""
            SELECT a.assignment_id::text, a.status, a.attempts, c.duration_minutes,
                   c.pass_score
            FROM training.assignments a
            JOIN training.courses c ON c.course_id = a.course_id
            WHERE a.status IN ('assigned','in_progress')
              AND a.assigned_dt <= %s
              AND random() < 0.20
            LIMIT 120
        """, (sim_dt,))
        rows = cur.fetchall()
        start_ids, done_rows = [], []
        for aid, status, attempts, dur, pass_score in rows:
            if status == 'assigned':
                start_ids.append(aid)
            else:
                study = random.gauss(0.8, 0.2)
                score = min(100, max(30, round(float(pass_score) * max(0.6, study)
                                               + random.uniform(-4, 14), 1)))
                if score >= float(pass_score):
                    done_rows.append((aid, score, attempts + 1, 'completed'))
                elif attempts + 1 >= 3:
                    done_rows.append((aid, score, attempts + 1, 'completed'))
                # else: stays in_progress, attempt recorded next pass
        if start_ids:
            execute_values(cur, """
                UPDATE training.assignments AS a
                SET status = 'in_progress', started_dt = v.st
                FROM (VALUES %s) AS v(id, st)
                WHERE a.assignment_id = v.id::uuid AND a.status = 'assigned'
            """, [(i, sim_dt) for i in start_ids])
        if done_rows:
            execute_values(cur, """
                UPDATE training.assignments AS a
                SET status = v.st, completed_dt = v.cd, score_pct = v.sc, attempts = v.at
                FROM (VALUES %s) AS v(id, sc, at, st, cd)
                WHERE a.assignment_id = v.id::uuid
            """, [(r[0], r[1], r[2], r[3], sim_dt) for r in done_rows])
            completed += len(done_rows)

        # 4) Overdue sweep
        cur.execute("""
            UPDATE training.assignments
            SET status = 'overdue'
            WHERE status IN ('assigned','in_progress')
              AND assigned_dt <= %s
              AND due_dt < %s
        """, (sim_dt, sim_dt))
        conn.commit()
    return assigned, completed
