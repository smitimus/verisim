"""
Chat model — live chat sessions + message transcripts.

A visitor opens a chat on web/ios/android, waits for an agent, exchanges
messages, and the session completes, abandons, or converts to a ticket.
"""
import random
import logging
from datetime import datetime, timedelta

from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)

OPENERS = [
    "Hi, I need help with my order.",
    "Why was I charged twice?",
    "The app won't load since this morning.",
    "I want to return an item, how?",
    "Can someone check my refund status?",
    "My account is locked, help!",
]
AGENT_CHAT = [
    "Hi there! Thanks for reaching out — let me take a look.",
    "Sorry about that. I can fix this for you right now.",
    "I see the issue on my end. Give me one moment.",
    "Thanks for waiting — I've applied the correction.",
    "Is there anything else I can help with today?",
]
CUST_CHAT = [
    "Sure, thank you.",
    "It's order #{{n}}.",
    "Still not working...",
    "Great, that fixed it!",
    "How long will that take?",
]
CLOSERS = [
    "Glad I could help! Have a great day.",
    "All set on my end — feel free to reach out again.",
]
CONVERT_NOTES = [
    "Issue needs engineering follow-up — converting to a ticket.",
    "Requires billing investigation beyond chat scope.",
]


def generate_chats(conn, cfg: Config, sim_dt: datetime, n_chats: int, ctx,
                   queues, customers, agents):
    """Create n_chats chat sessions with message transcripts.

    Returns (sessions, converted_to_ticket_count).
    """
    if n_chats <= 0 or not queues:
        return 0, 0

    active_agents = [a for a in agents if a['department'] == 'agent']
    cust_ids = [c['customer_id'] for c in customers]
    sess_rows, grouped_msgs = [], []
    converted = 0
    stress = getattr(ctx, 'call_stress', 1.0)

    for _ in range(n_chats):
        queue = random.choice(queues)
        started = sim_dt - timedelta(seconds=random.randint(0,
            max(1, cfg.generator.simulation_minutes_per_tick * 60 - 1)))
        cust = random.choice(cust_ids) if cust_ids else None
        platform = random.choices(['web', 'ios_app', 'android_app'],
                                  weights=[0.55, 0.25, 0.20])[0]
        wait = max(5, int(random.expovariate(1.0 / (25 * stress))))

        eligible = [a for a in active_agents if queue['code'] in a['skill_groups']]
        agent = random.choice(eligible) if eligible else None

        if not agent or random.random() < 0.10 * stress:
            # abandoned in the lobby before pickup
            sess_rows.append((queue['queue_id'], cust, None, started, None,
                              started + timedelta(seconds=wait), 'abandoned', None,
                              platform, False, 0, wait, 0, ctx.scenario_tag))
            grouped_msgs.append([])
            continue

        first_resp = started + timedelta(seconds=wait + random.randint(0, 8))
        n_msgs = random.choices([4, 6, 8, 10, 14, 18],
                                weights=[0.18, 0.22, 0.20, 0.16, 0.14, 0.10])[0]
        t = first_resp + timedelta(seconds=random.randint(5, 30))
        msgs = [('system', None,
                 f"Chat started — routed to agent via {queue['name']}.", first_resp)]
        msgs.append(('customer', None, random.choice(OPENERS), t))
        t += timedelta(seconds=random.randint(15, 60))
        for i in range(n_msgs - 2):
            if i % 2 == 0:
                msgs.append(('agent', agent['employee_id'], random.choice(AGENT_CHAT), t))
            else:
                body = random.choice(CUST_CHAT).replace(
                    "{{n}}", str(random.randint(100000, 999999)))
                msgs.append(('customer', None, body, t))
            t += timedelta(seconds=random.randint(15, 90))
        msgs.append(('agent', agent['employee_id'], random.choice(CLOSERS), t))

        status = 'completed' if random.random() > 0.06 else 'abandoned'
        is_converted = False
        if queue['code'] == 'technical' and random.random() < 0.18:
            status = 'transferred'
            is_converted = True
            converted += 1
            msgs.append(('system', None, random.choice(CONVERT_NOTES),
                         t + timedelta(seconds=10)))

        duration = int((t - started).total_seconds())
        sess_rows.append((queue['queue_id'], cust, agent['employee_id'], started,
                          first_resp, t + timedelta(seconds=10), status,
                          agent['employee_id'], platform, is_converted,
                          len(msgs), wait, duration, ctx.scenario_tag))
        grouped_msgs.append(msgs)

    with conn.cursor() as cur:
        sids = execute_values(cur, """
            INSERT INTO chat.sessions
                (queue_id, customer_id, agent_id, started_dt, first_response_dt,
                 ended_dt, status, visitor_agent, platform, transferred_to_ticket,
                 message_count, wait_seconds, duration_seconds, scenario_tag)
            VALUES %s
            RETURNING session_id::text
        """, sess_rows, fetch=True)
        sid_list = [r[0] for r in sids]

        flat = []
        for i, msgs in enumerate(grouped_msgs):
            for stype, sender, body, ts in msgs:
                flat.append((sid_list[i], stype, sender, body, ts))
        if flat:
            execute_values(cur, """
                INSERT INTO chat.messages (session_id, sender_type, sender_id, body, sent_dt)
                VALUES %s
            """, flat)
        conn.commit()
    return len(sess_rows), converted
