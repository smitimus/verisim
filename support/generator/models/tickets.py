"""
Ticket model — the heart of the support industry.

A ticket is created on a channel, assigned to a skilled agent, accumulates
agent + customer comments and an audit trail of actions (assignments,
queue movements, escalations, status changes), then resolves and closes.
Some reopen; VIPs escalate; the scenario context warms sentiment and volume.

Design follows the grocery seed/generate/fetch contract:
  seed_*  — reference data (called from main.py seed_all)
  generate_tickets(conn, cfg, sim_dt, n, ...) — per-tick creation + lifecycle
                                                advancement
  fetch_* — cache refresh
"""
import random
import logging
from datetime import datetime, timedelta

from faker import Faker
from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)
fake = Faker('en_US')

# Comment banks — agent notes ("tickets should capture comments of agents
# and actions taken"), customer replies, system entries.
AGENT_ACK = [
    "Confirmed the issue on my side. Reviewing account history now.",
    "Thanks for the details — I can see what happened here.",
    "I've pulled up your order and I'm checking with the relevant team.",
    "Reproduced this in our test environment. Working on a fix path.",
    "Verified identity via support PIN. Continuing.",
]
AGENT_ACTION = [
    "Issued a {ref} credit to the original payment method.",
    "Reprocessed the payment — new confirmation {ref}.",
    "Created return label and emailed it; pickup scheduled.",
    "Applied account patch; asked customer to retry and confirm.",
    "Reset entitlements and forced re-sync on the backend.",
    "Escalated to Tier 2 with full case notes attached.",
    "Corrected the shipping address with the carrier before dispatch.",
    "Added a one-time goodwill credit of ${amt:.2f} to the account.",
    "Linked duplicate orders {ref} so refunds process together.",
    "Cleared the stuck job in the queue; sync resumed.",
]
AGENT_PENDING = [
    "Waiting on the carrier's trace response — set expectation to 48h.",
    "Awaiting engineering confirmation on the bug ticket.",
    "Requested a screenshot/repro steps from the customer.",
    "Payment provider investigation opened; holding case until response.",
]
AGENT_RESOLVE = [
    "Customer confirmed everything is working. Resolving.",
    "Refund verified on their statement — closing this out.",
    "Root cause fixed and verified with the customer. Resolved.",
    "Workaround accepted; filed follow-up for the permanent fix.",
]
CUSTOMER_REPLY = [
    "Still happening on my end, any update?",
    "That worked, thank you!",
    "I don't think that's right — the charge is different than quoted.",
    "When can I expect this to be done?",
    "Attaching the screenshot you asked for.",
    "Yes it's fixed now. Appreciate the quick turnaround.",
]
SYSTEM_NOTE = [
    "Auto-classified sentiment from latest customer reply.",
    "SLA timer paused — awaiting customer response.",
    "Customer viewed the case update in the portal.",
]

REASONS = {
    'escalations': ["prior unresolved contact", "VIP tier", "churn risk detected",
                    "negative sentiment streak", "executive flag"],
}


def _pick_agent(conn, queue_code, exclude=None):
    """Skill-based routing: active agent whose skill_groups include the queue."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT employee_id::text FROM hr.employees
            WHERE status = 'active' AND department = 'agent'
              AND %s = ANY(string_to_array(skill_groups, ','))
            ORDER BY RANDOM() LIMIT 1
        """, (queue_code,))
        row = cur.fetchone()
        return row[0] if row else None


def _queue_by_code(queues, code):
    for q in queues:
        if q['code'] == code:
            return q
    return None


def generate_tickets(conn, cfg: Config, sim_dt: datetime, n_new: int, ctx,
                     queues, customers):
    """
    Create n_new tickets and advance every open ticket's lifecycle by this
    tick. Returns (created, touched).
    """
    created = 0
    if n_new > 0:
        created = _create_tickets(conn, cfg, sim_dt, n_new, ctx, queues, customers)
    advanced = _advance_lifecycle(conn, cfg, sim_dt, ctx, queues)
    return created, advanced


def _create_tickets(conn, cfg, sim_dt, n_new, ctx, queues, customers):
    cust_ids = [c['customer_id'] for c in customers]
    if not cust_ids:
        return 0

    ticket_rows, comment_rows, action_rows = [], [], []
    # per-ticket runtime bookkeeping (resolved_dt computed later by lifecycle)
    for _ in range(n_new):
        qcode = _weighted_queue(cfg, ctx)
        queue = _queue_by_code(queues, qcode) or queues[0]
        cat = random.choice(queue['categories']) if queue['categories'] else None
        # VIP customers skew to escalations
        cust = random.choice(cust_ids)
        channel = random.choices(
            ['email', 'web', 'phone', 'chat', 'social'],
            weights=[cfg.queues.email_share, cfg.queues.web_share,
                     cfg.queues.phone_share, cfg.queues.chat_share,
                     cfg.queues.social_share])[0]
        subject = _subject_for(qcode)
        sentiment = _sample_sentiment(ctx)
        priority = _sample_priority(sentiment, qcode)
        agent_id = _pick_agent(conn, qcode)
        first_resp = sim_dt + timedelta(minutes=random.randint(2, 90)) \
            if agent_id and random.random() < 0.9 else None

        ticket_rows.append((
            cust, queue['queue_id'], cat['category_id'] if cat else None,
            agent_id, subject,
            f"Customer contacted via {channel}. Reported: {subject.lower()}.",
            channel, priority,
            'open' if agent_id else 'new',
            sentiment, sim_dt, first_resp,
            random.randint(1, 4) if agent_id else 0,
            ctx.scenario_tag,
        ))

    with conn.cursor() as cur:
        returned = execute_values(cur, """
            INSERT INTO support.tickets
                (customer_id, queue_id, category_id, assigned_agent_id, subject,
                 description, channel, priority, status, sentiment, created_dt,
                 first_response_dt, touch_count, scenario_tag)
            VALUES %s
            RETURNING ticket_id::text, ticket_number
        """, ticket_rows, fetch=True)

        for (tid, tnum), row in zip(returned, ticket_rows):
            subject = row[4]
            # Every ticket: system 'created' action + opening comment
            action_rows.append((tid, 'created', None, row[7], row[3], row[10]))
            comment_rows.append((tid, 'system', None,
                                 f"Case #{tnum} created via {row[6]} — subject: {subject}",
                                 False, row[10]))
            if row[3]:  # agent_id
                action_rows.append((tid, 'assigned', None, row[3], row[3], row[10]))
                comment_rows.append((tid, 'agent', row[3],
                                     random.choice(AGENT_ACK), False,
                                     row[10] + timedelta(minutes=random.randint(1, 15))))
            if random.random() < 0.35:  # internal working note
                note = random.choice(AGENT_ACTION + AGENT_PENDING)
                note = note.format(ref=f"REF-{random.randint(10000, 99999)}",
                                   amt=random.uniform(5, 120))
                comment_rows.append((tid, 'agent', row[3], note, True,
                                     row[10] + timedelta(minutes=random.randint(16, 120))))

        execute_values(cur, """
            INSERT INTO support.ticket_comments
                (ticket_id, author_type, author_id, body, is_internal, created_dt)
            VALUES %s
        """, [(c[0], c[1], c[2], c[3], c[4], c[5]) for c in comment_rows])
        execute_values(cur, """
            INSERT INTO support.ticket_actions
                (ticket_id, action_type, from_value, to_value, performed_by, created_dt)
            VALUES %s
        """, action_rows)
        conn.commit()
    return len(returned)


def _weighted_queue(cfg, ctx):
    """Queue choice, nudged toward escalations when scenario sentiment is hot."""
    weights = dict(cfg.queues.ticket_weight)
    if ctx.scenario_tag and ('outage' in ctx.scenario_tag or 'launch' in ctx.scenario_tag):
        weights['technical'] = weights.get('technical', 0.24) * 2.2
    if ctx.sentiment_shift > 0.3:
        weights['escalations'] = weights.get('escalations', 0.06) * 3
    codes = list(weights.keys())
    vals = list(weights.values())
    return random.choices(codes, weights=vals, k=1)[0]


def _sample_sentiment(ctx):
    neg = 0.18 + ctx.sentiment_shift
    r = random.random()
    if r < neg:
        return 'negative'
    if r < neg + 0.30:
        return 'neutral'
    return 'positive'


def _sample_priority(sentiment, qcode):
    if qcode == 'escalations':
        return random.choices(['high', 'urgent'], weights=[0.6, 0.4])[0]
    if sentiment == 'negative':
        return random.choices(['low', 'medium', 'high', 'urgent'],
                              weights=[0.10, 0.40, 0.38, 0.12])[0]
    return random.choices(['low', 'medium', 'high', 'urgent'],
                          weights=[0.28, 0.50, 0.18, 0.04])[0]


def _subject_for(queue_code):
    from models.customers import subject_for
    return subject_for(queue_code)


def _advance_lifecycle(conn, cfg, sim_dt, ctx, queues):
    """
    For every ticket still open/new/pending that was created before sim_dt:
      - progress: agent replies, queue moves, escalation, resolution, closure
    Each progression writes comments + audit actions. Bounded per tick so a
    day of ticks reads like a real queue draining.
    """
    touched = 0
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.ticket_id, t.ticket_number, t.assigned_agent_id, t.status,
                   t.queue_id, t.priority, t.touch_count,
                   t.created_dt::timestamp AS created_dt, q.code
            FROM support.tickets t
            JOIN support.queues q ON q.queue_id = t.queue_id
            WHERE t.status IN ('new','open','pending')
              AND t.created_dt < %s
              AND random() < 0.28
            LIMIT 400
        """, (sim_dt,))
        rows = cur.fetchall()

        updates, comments, actions = [], [], []
        queue_by_id = {q['queue_id']: q for q in queues}
        codes = [q['code'] for q in queues]

        for tid, tnum, agent_id, status, qid, priority, touches, created_dt, qcode in rows:
            age_h = (sim_dt - created_dt).total_seconds() / 3600
            q = queue_by_id.get(str(qid))
            sla_hours = q['sla_hours'] if q else 24
            resolve_hours = q['avg_resolve_hours'] if q else 8

            # overdue tickets are more likely to move
            urgency = min(0.95, age_h / max(1, sla_hours * 1.5))
            r = random.random()

            if r < 0.10 * urgency:
                # queue movement ("movements to different ticket queues")
                new_code = _transfer_target(qcode, codes)
                tq = _queue_by_code(queues, new_code) or q
                new_agent = _pick_agent(conn, new_code, exclude=str(agent_id) if agent_id else None)
                updates.append((str(new_agent) if new_agent else agent_id, 'open',
                                str(tq['queue_id']), touches + 1, tid))
                actions.append((tid, 'moved', qcode, new_code, agent_id, sim_dt))
                if new_agent:
                    actions.append((tid, 'reassigned', str(agent_id), new_agent, agent_id, sim_dt))
                comments.append((tid, 'agent', agent_id,
                                 f"Moved to {tq['name']} — issue is {new_code}-related.",
                                 True, sim_dt))
                touched += 1
            elif r < 0.14 * urgency:
                # escalation to escalations queue
                esc = _queue_by_code(queues, 'escalations')
                new_agent = _pick_agent(conn, 'escalations')
                reason = random.choice(REASONS['escalations'])
                updates.append((new_agent, 'open', str(esc['queue_id']),
                                touches + 1, tid))
                actions.append((tid, 'escalated', 'high' if priority != 'urgent' else 'urgent',
                                reason, agent_id, sim_dt))
                comments.append((tid, 'agent', agent_id,
                                 f"Escalated: {reason}. Handoff notes attached.",
                                 True, sim_dt))
                touched += 1
            elif r < 0.22:
                # back to pending (waiting on something) or customer reply seen
                note = random.choice(AGENT_PENDING)
                updates.append((agent_id, 'pending', qid, touches + 1, tid))
                actions.append((tid, 'status_change', status, 'pending', agent_id, sim_dt))
                comments.append((tid, 'agent', agent_id, note, False, sim_dt))
                touched += 1
            elif r < 0.30:
                # customer pushes back
                comments.append((tid, 'customer', None,
                                 random.choice(CUSTOMER_REPLY), False, sim_dt))
                updates.append((agent_id, 'open', qid, touches + 1, tid))
                touched += 1
            elif status == 'pending' or age_h > resolve_hours or r < 0.45 + urgency * 0.4:
                # resolve (and maybe close) — first-contact resolution vs. longer path
                resolved_dt = sim_dt
                close_later = random.random() < cfg.queues.reopen_rate
                updates.append((agent_id, 'resolved', qid, touches + 1, tid))
                actions.append((tid, 'resolved', status, 'resolved', agent_id, resolved_dt))
                comments.append((tid, 'agent', agent_id,
                                 random.choice(AGENT_RESOLVE), False, resolved_dt))
                if not close_later:
                    # auto-close after 48h — handled by the same branch on later ticks
                    pass
                touched += 1

        # SLA breach detection (audit action, no status change)
        cur.execute("""
            SELECT t.ticket_id, t.assigned_agent_id, q.code
            FROM support.tickets t
            JOIN support.queues q ON q.queue_id = t.queue_id
            WHERE t.status IN ('new','open','pending')
              AND EXTRACT(EPOCH FROM (%s - t.created_dt))/3600 > q.sla_hours * 1.5
              AND NOT EXISTS (
                  SELECT 1 FROM support.ticket_actions a
                  WHERE a.ticket_id = t.ticket_id AND a.action_type = 'sla_breached')
            LIMIT 200
        """, (sim_dt,))
        for tid, agent_id, qcode in cur.fetchall():
            actions.append((tid, 'sla_breached', None, qcode, agent_id, sim_dt))

        if updates:
            execute_values(cur, """
                UPDATE support.tickets AS t
                SET assigned_agent_id = v.agent::uuid,
                    status = v.status,
                    queue_id = v.qid::uuid,
                    touch_count = v.touches,
                    resolved_dt = CASE WHEN v.status = 'resolved' AND t.resolved_dt IS NULL
                                       THEN NOW() ELSE t.resolved_dt END,
                    first_response_dt = COALESCE(t.first_response_dt, t.created_dt +
                        (random() * interval '2 hours')),
                    updated_at = NOW()
                FROM (VALUES %s) AS v(agent, status, qid, touches, id)
                WHERE t.ticket_id = v.id::uuid
            """, updates)
        if comments:
            execute_values(cur, """
                INSERT INTO support.ticket_comments
                    (ticket_id, author_type, author_id, body, is_internal, created_dt)
                VALUES %s
            """, comments)
        if actions:
            execute_values(cur, """
                INSERT INTO support.ticket_actions
                    (ticket_id, action_type, from_value, to_value, performed_by, created_dt)
                VALUES %s
            """, actions)

        # Resolved tickets auto-close after ~2 days
        cur.execute("""
            UPDATE support.tickets
            SET status = 'closed', closed_dt = resolved_dt + interval '2 days',
                updated_at = NOW()
            WHERE status = 'resolved' AND resolved_dt < %s - interval '2 days'
        """, (sim_dt,))
        conn.commit()
    return touched


def _transfer_target(from_code, codes):
    table = {
        'billing': ['account', 'returns', 'escalations'],
        'technical': ['escalations', 'account'],
        'account': ['billing', 'technical'],
        'shipping': ['returns', 'billing'],
        'returns': ['billing', 'shipping'],
        'escalations': ['technical', 'billing'],
    }
    opts = [c for c in table.get(from_code, codes) if c in codes and c != from_code]
    return random.choice(opts) if opts else from_code


def maybe_reopen_tickets(conn, cfg, sim_dt, queues):
    """Some resolved tickets reopen (customer follow-up failed)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ticket_id, queue_id, assigned_agent_id, status
            FROM support.tickets
            WHERE status = 'resolved'
              AND random() < %s
            LIMIT 30
        """, (cfg.queues.reopen_rate,))
        rows = cur.fetchall()
        for tid, qid, agent_id, _ in rows:
            cur.execute("""
                UPDATE support.tickets
                SET status = 'open', reopen_count = reopen_count + 1,
                    resolved_dt = NULL, touch_count = touch_count + 1,
                    updated_at = NOW()
                WHERE ticket_id = %s
            """, (tid,))
            cur.execute("""
                INSERT INTO support.ticket_actions
                    (ticket_id, action_type, from_value, to_value, performed_by, created_dt)
                VALUES (%s, 'reopened', 'resolved', 'open', %s, %s)
            """, (tid, agent_id, sim_dt))
            cur.execute("""
                INSERT INTO support.ticket_comments
                    (ticket_id, author_type, body, is_internal, created_dt)
                VALUES (%s, 'customer', %s, FALSE, %s)
            """, (tid, "This isn't fixed — it started again last night.", sim_dt))
        conn.commit()
    return len(rows)
