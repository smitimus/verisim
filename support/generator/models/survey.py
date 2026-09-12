"""
Survey model — sNPS system attached to closed interactions.

After a call/chat/ticket resolves, a survey is sent 1-2 days later; a
channel-dependent fraction responds with an NPS score (0-10), a correlated
CSAT (1-5), a reason tag and sometimes a free-text verbatim. Detractor
concentration per agent feeds the training model (qa_finding assignments).
"""
import random
import logging
from datetime import timedelta

from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)

REASONS = {
    'promoter': ['Friendly and knowledgeable agent', 'Solved my problem fast',
                 'Great follow-up', 'Easy to understand', 'Above and beyond'],
    'passive':  ['Issue resolved but took a while', 'Agent was fine',
                 'Got the answer eventually', 'Could be faster'],
    'detractor': ['Long wait times', 'Problem not solved', 'Had to repeat myself',
                  'Agent lacked product knowledge', 'Rude experience',
                  'Transferred too many times', 'Never heard back'],
}
VERBATIMS = {
    'promoter': ["Your agent was fantastic — fixed everything in one call!",
                 "Finally a company that answers the phone. Thank you!",
                 "Super quick chat resolution, appreciate it."],
    'passive':  ["It's okay. Took longer than I hoped.",
                 "Got sorted eventually.",
                 "Average experience, nothing special."],
    'detractor': ["Third time contacting about the same issue. Unacceptable.",
                  "Was on hold for 20 minutes then dropped.",
                  "Agent clearly didn't know the product.",
                  "Refund promised twice, still nothing."],
}

# Interaction source tables: channel -> (table, id column, agent column, completion column)
SOURCES = [
    ('voice',  'voice.calls',      'call_id',    'agent_id',   'end_dt',      'response_rate_voice'),
    ('chat',   'chat.sessions',    'session_id', 'agent_id',   'ended_dt',    'response_rate_chat'),
    ('ticket', 'support.tickets',  'ticket_id',  'assigned_agent_id', 'resolved_dt', 'response_rate_ticket'),
]


def generate_surveys(conn, cfg: Config, sim_dt, ctx):
    """Send + respond to surveys around this tick. Returns (sent, responded)."""
    sent = 0
    responded = 0
    sentiment_shift = getattr(ctx, 'sentiment_shift', 0.0)

    with conn.cursor() as cur:
        # 1) Send surveys for interactions that completed 1-2 days ago and
        #    have no survey yet.
        for channel, table, id_col, agent_col, done_col, rate_attr in SOURCES:
            cur.execute(f"""
                SELECT x.iid, x.customer_id, x.agent_id, x.done_dt
                FROM (
                    SELECT {id_col}::text AS iid, customer_id::text,
                           {agent_col}::text AS agent_id, {done_col}::timestamp AS done_dt
                    FROM {table}
                    WHERE {done_col} BETWEEN %s - interval '2 days'
                                         AND %s - interval '1 day'
                ) x
                WHERE NOT EXISTS (
                    SELECT 1 FROM survey.surveys s
                    WHERE s.interaction_id = x.iid::uuid
                )
                ORDER BY random()
                LIMIT 300
            """, (sim_dt, sim_dt))
            rows = cur.fetchall()
            if not rows:
                continue
            insert_rows = [(channel, iid, cust, agent,
                            done + timedelta(hours=random.randint(1, 6)))
                           for iid, cust, agent, done in rows]
            execute_values(cur, """
                INSERT INTO survey.surveys
                    (channel, interaction_id, customer_id, agent_id, sent_dt)
                VALUES %s
            """, insert_rows)
            sent += len(insert_rows)

        # 2) Respond to surveys that have been out for >= 6h. Response
        #    probability is per-channel; responses land 2-72h after send.
        rate_sql = """
            CASE channel
                WHEN 'voice'  THEN %s
                WHEN 'chat'   THEN %s
                ELSE %s
            END
        """
        cur.execute(f"""
            SELECT survey_id::text, sent_dt::timestamp AS sent_dt
            FROM survey.surveys
            WHERE is_complete = FALSE
              AND sent_dt <= %s - interval '6 hours'
              AND random() < {rate_sql}
            LIMIT 400
        """, (sim_dt,
              cfg.surveys.response_rate_voice * 0.55,
              cfg.surveys.response_rate_chat * 0.70,
              cfg.surveys.response_rate_ticket * 0.60))
        due = cur.fetchall()
        updates = []
        for sid, sent_dt in due:
            nps, csat, reason, bucket = _sample_score(sentiment_shift)
            responded_dt = sent_dt + timedelta(
                hours=random.randint(2, 72), minutes=random.randint(0, 59))
            if responded_dt > sim_dt:
                continue
            verbatim = (random.choice(VERBATIMS[bucket])
                        if random.random() < 0.4 else None)
            updates.append((sid, responded_dt, nps, csat, reason, verbatim))
        if updates:
            execute_values(cur, """
                UPDATE survey.surveys AS s
                SET responded_dt = v.rd, nps_score = v.nps, csat_score = v.csat,
                    reason_tag = v.reason, verbatim = v.vb, is_complete = TRUE
                FROM (VALUES %s) AS v(id, rd, nps, csat, reason, vb)
                WHERE s.survey_id = v.id::uuid
            """, updates)
            responded = len(updates)
        conn.commit()
    return sent, responded


def _sample_score(sentiment_shift):
    """
    Sample a 0-10 sNPS score from a healthy-center baseline (~+25 NPS),
    shift down with scenario sentiment, derive a correlated CSAT 1-5.
    """
    r = random.random()
    if r < 0.42:
        nps = random.randint(9, 10)
    elif r < 0.62:
        nps = random.choice([7, 8])
    elif r < 0.80:
        nps = random.randint(5, 6)
    else:
        nps = random.randint(0, 4)

    shift = int(round(sentiment_shift * 6))
    if shift > 0:
        nps = max(0, nps - shift)

    if nps >= 9:
        csat = random.choices([4, 5], weights=[0.25, 0.75])[0]
    elif nps >= 7:
        csat = random.choices([3, 4], weights=[0.5, 0.5])[0]
    elif nps >= 5:
        csat = random.choices([2, 3], weights=[0.55, 0.45])[0]
    else:
        csat = random.choices([1, 2], weights=[0.7, 0.3])[0]

    bucket = 'promoter' if nps >= 9 else ('passive' if nps >= 7 else 'detractor')
    reason = random.choice(REASONS[bucket])
    return nps, csat, reason, bucket


def detractor_agents_recent(conn, cfg, sim_dt, lookback_days=10):
    """Agents with >= 2 recent detractor surveys — remedial training targets."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT agent_id::text, COUNT(*)
            FROM survey.surveys
            WHERE agent_id IS NOT NULL
              AND is_complete = TRUE
              AND nps_score <= 3
              AND responded_dt >= %s - (%s * interval '1 day')
            GROUP BY 1
            HAVING COUNT(*) >= 2
        """, (sim_dt, lookback_days))
        return [r[0] for r in cur.fetchall()]
