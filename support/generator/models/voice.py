"""
Voice ACD model — phone call detail records.

Each offered call lands in a queue, waits (abandonment if too long), rings an
agent with the right skill, then talks (with holds), optionally transfers,
optionally creates a follow-up ticket, and ends with after-call work.
Timing is derived from queue-level averages x agent aht_factor x scenario.
"""
import random
import logging
import uuid
from datetime import datetime, timedelta

from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)

QUEUE_WAIT_BASE = {'escalations': 90, 'technical': 75, 'billing': 45,
                   'account': 40, 'shipping': 50, 'returns': 60}
QUEUE_TALK_BASE = {'technical': 540, 'escalations': 720, 'billing': 300,
                   'account': 260, 'shipping': 280, 'returns': 340}
QUEUE_CALL_WEIGHT = {'billing': 0.30, 'technical': 0.26, 'account': 0.14,
                     'shipping': 0.12, 'returns': 0.10, 'escalations': 0.08}


def generate_calls(conn, cfg: Config, sim_dt: datetime, n_calls: int, ctx,
                   queues, customers, agents):
    """Create n_calls ACD records. Returns (calls, follow_up_seeds)."""
    if n_calls <= 0 or not queues:
        return 0, 0

    active_agents = [a for a in agents if a['department'] == 'agent']
    cust_ids = [c['customer_id'] for c in customers]
    rows = []
    follow_ups = 0

    # Scenario pressure: outage -> longer queues, more abandonment, longer calls
    stress = getattr(ctx, 'call_stress', 1.0)

    for _ in range(n_calls):
        queue = _pick_queue(queues)
        offered = sim_dt - timedelta(seconds=random.randint(0,
            max(1, cfg.generator.simulation_minutes_per_tick * 60 - 1)))
        cust = random.choice(cust_ids) if (cust_ids and random.random() < 0.75) else None
        base_wait = QUEUE_WAIT_BASE.get(queue['code'], 50)
        wait = max(1, int(random.expovariate(1.0 / (base_wait * stress))))
        queued = offered + timedelta(seconds=wait)

        abandonment = cfg.queues.abandonment_rate * stress
        if wait > 300:
            abandonment += 0.25  # long-queue patience cliff

        if random.random() < abandonment:
            rows.append((queue['queue_id'], cust, None, None, 'inbound',
                         offered, queued, None, None, queued,
                         wait, 0, 0, 0, 0, True,
                         'abandoned_timeout' if wait > 180 else 'abandoned_customer',
                         False, None, ctx.scenario_tag))
            continue

        ring = queued + timedelta(seconds=random.randint(1, 20))
        eligible = [a for a in active_agents if queue['code'] in a['skill_groups']] or active_agents
        if not eligible:
            rows.append((queue['queue_id'], cust, None, None, 'inbound',
                         offered, queued, ring, None, ring + timedelta(seconds=25),
                         wait, 0, 0, 0, 0, True, 'abandoned_customer',
                         False, None, ctx.scenario_tag))
            continue

        agent = random.choice(eligible)
        connect = ring + timedelta(seconds=random.randint(1, 25))

        talk_base = QUEUE_TALK_BASE.get(queue['code'], 300)
        talk = max(30, int(random.expovariate(1.0 / (talk_base * agent['aht_factor']))))
        talk = int(talk * getattr(ctx, 'talk_multiplier', 1.0))

        n_holds = 0 if talk < 240 else random.choices([0, 1, 2, 3], weights=[0.5, 0.3, 0.15, 0.05])[0]
        hold = sum(random.randint(20, 120) for _ in range(n_holds))
        acw = random.randint(20, 90) if talk > 120 else random.randint(10, 45)

        transferred_to = None
        disposition = 'resolved'
        if random.random() < min(0.5, cfg.queues.transfer_rate * stress):
            tgt = _transfer_target(queue['code'], [q['code'] for q in queues])
            tq = _queue_by_code(queues, tgt)
            if tq:
                transferred_to = tq['queue_id']
                disposition = 'transferred'
        has_ticket = False
        if disposition == 'resolved':
            if random.random() < 0.08:
                disposition = 'follow_up_ticket'
                has_ticket = True
                follow_ups += 1
            elif random.random() < 0.05:
                disposition = 'voicemail'

        end = connect + timedelta(seconds=talk + hold + acw)
        recording = (f"/recordings/{offered:%Y/%m}/{uuid.uuid4().hex[:12]}.wav"
                     if disposition != 'voicemail' and random.random() < 0.6 else None)
        rows.append((queue['queue_id'], cust, agent['employee_id'], transferred_to, 'inbound',
                     offered, queued, ring, connect, end,
                     wait, talk, hold, n_holds, acw, False, disposition, has_ticket,
                     recording, ctx.scenario_tag))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO voice.calls
                (queue_id, customer_id, agent_id, transferred_to_queue_id,
                 direction, offered_dt, queued_dt, ring_dt, connect_dt, end_dt,
                 wait_seconds, talk_seconds, hold_seconds, hold_count,
                 after_call_work_seconds, abandoned, disposition, has_ticket,
                 recording_url, scenario_tag)
            VALUES %s
        """, rows)
        conn.commit()
    return len(rows), follow_ups


def _pick_queue(queues):
    vals = [QUEUE_CALL_WEIGHT.get(q['code'], 0.1) for q in queues]
    return random.choices(queues, weights=vals, k=1)[0]


def _queue_by_code(queues, code):
    for q in queues:
        if q['code'] == code:
            return q
    return None


def _transfer_target(from_code, codes):
    table = {
        'billing': ['account', 'returns'],
        'technical': ['escalations', 'billing'],
        'account': ['billing', 'technical'],
        'shipping': ['returns', 'billing'],
        'returns': ['billing'],
        'escalations': ['technical'],
    }
    opts = [c for c in table.get(from_code, []) if c in codes]
    return random.choice(opts) if opts else from_code
