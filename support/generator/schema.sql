-- =============================================================================
-- Verisim — Customer-Support Industry Database (Phase 3)
-- Database: support  (one DB per industry)
-- Schemas: hr, support, voice, chat, survey, training, control
--
-- Models a contact center:
--   * ticketing system with movements between queues/agents, agent comments
--     and an audit trail of actions taken (support.*)
--   * voice phone ACD with full call detail records (voice.*)
--   * live chat sessions + message transcripts (chat.*)
--   * sNPS survey system attached to closed interactions (survey.*)
--   * agent training system with assignments + completions (training.*)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS support;
CREATE SCHEMA IF NOT EXISTS voice;
CREATE SCHEMA IF NOT EXISTS chat;
CREATE SCHEMA IF NOT EXISTS survey;
CREATE SCHEMA IF NOT EXISTS training;
CREATE SCHEMA IF NOT EXISTS control;

-- ---------------------------------------------------------------------------
-- HR Schema — contact-center sites and agents (source of truth for people)
-- ---------------------------------------------------------------------------

CREATE TABLE hr.locations (
    location_id     UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(100) NOT NULL,
    address         VARCHAR(200) NOT NULL,
    city            VARCHAR(100) NOT NULL,
    state           CHAR(2)      NOT NULL,
    zip             VARCHAR(10)  NOT NULL,
    phone           VARCHAR(20),
    opened_date     DATE         NOT NULL,
    location_type   VARCHAR(20)  NOT NULL CHECK (location_type IN ('contact_center', 'satellite')),
    seat_capacity   INTEGER,
    timezone        VARCHAR(40)  NOT NULL DEFAULT 'America/New_York',
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE hr.employees (
    employee_id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id         UUID         NOT NULL REFERENCES hr.locations(location_id),
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100) NOT NULL,
    email               VARCHAR(255) NOT NULL UNIQUE,
    hire_date           DATE         NOT NULL,
    termination_date    DATE,
    department          VARCHAR(50)  NOT NULL CHECK (department IN (
                            'agent', 'team_lead', 'qa', 'training', 'management')),
    job_title           VARCHAR(100) NOT NULL,
    skill_groups        VARCHAR(200),          -- comma-separated queue codes
    aht_factor          NUMERIC(4,2) NOT NULL DEFAULT 1.00,  -- individual handle-time multiplier
    hourly_rate         NUMERIC(8,2) NOT NULL,
    status              VARCHAR(20)  NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active', 'terminated', 'on_leave', 'training')),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Support Schema — ticketing system
-- ---------------------------------------------------------------------------

CREATE TABLE support.queues (
    queue_id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    code              VARCHAR(30)  NOT NULL UNIQUE,
    name              VARCHAR(100) NOT NULL,
    description       VARCHAR(300),
    sla_hours         INTEGER      NOT NULL DEFAULT 24,
    avg_resolve_hours NUMERIC(6,2) NOT NULL DEFAULT 8.00,   -- mean time-to-resolve in this queue
    is_active         BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE support.categories (
    category_id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id      UUID         NOT NULL REFERENCES support.queues(queue_id),
    name          VARCHAR(100) NOT NULL,
    severity      VARCHAR(20)  NOT NULL DEFAULT 'normal'
                      CHECK (severity IN ('low', 'normal', 'high', 'critical')),
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (queue_id, name)
);

CREATE TABLE support.customers (
    customer_id     UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) NOT NULL UNIQUE,
    phone           VARCHAR(20),
    tier            VARCHAR(20)  NOT NULL DEFAULT 'standard'
                        CHECK (tier IN ('standard', 'plus', 'premium', 'vip')),
    signup_date     DATE         NOT NULL,
    lifetime_value  NUMERIC(10,2) NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE support.tickets (
    ticket_id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_number     BIGSERIAL    UNIQUE NOT NULL,
    customer_id       UUID         NOT NULL REFERENCES support.customers(customer_id),
    queue_id          UUID         NOT NULL REFERENCES support.queues(queue_id),
    category_id       UUID         REFERENCES support.categories(category_id),
    assigned_agent_id UUID         REFERENCES hr.employees(employee_id),
    subject           VARCHAR(300) NOT NULL,
    description       TEXT,
    channel           VARCHAR(20)  NOT NULL
                          CHECK (channel IN ('email', 'web', 'phone', 'chat', 'social')),
    priority          VARCHAR(20)  NOT NULL DEFAULT 'medium'
                          CHECK (priority IN ('low', 'medium', 'high', 'urgent')),
    status            VARCHAR(20)  NOT NULL DEFAULT 'new'
                          CHECK (status IN ('new', 'open', 'pending', 'resolved', 'closed', 'cancelled')),
    sentiment         VARCHAR(20)
                          CHECK (sentiment IN ('positive', 'neutral', 'negative')),
    created_dt        TIMESTAMPTZ  NOT NULL,
    first_response_dt TIMESTAMPTZ,
    resolved_dt       TIMESTAMPTZ,
    closed_dt         TIMESTAMPTZ,
    reopen_count      INTEGER      NOT NULL DEFAULT 0,
    touch_count       INTEGER      NOT NULL DEFAULT 0,   -- agent touches (drives handle time)
    scenario_tag      VARCHAR(80),
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Agent + customer comments on a ticket ("tickets should capture comments of agents")
CREATE TABLE support.ticket_comments (
    comment_id    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id     UUID         NOT NULL REFERENCES support.tickets(ticket_id),
    author_type   VARCHAR(20)  NOT NULL CHECK (author_type IN ('agent', 'customer', 'system')),
    author_id     UUID         REFERENCES hr.employees(employee_id),
    body          TEXT         NOT NULL,
    is_internal   BOOLEAN      NOT NULL DEFAULT FALSE,   -- agent-only note vs customer-visible
    created_dt    TIMESTAMPTZ  NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Audit trail of actions taken — status changes, re-assignments, queue moves
CREATE TABLE support.ticket_actions (
    action_id       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket_id       UUID         NOT NULL REFERENCES support.tickets(ticket_id),
    action_type     VARCHAR(30)  NOT NULL
                        CHECK (action_type IN ('created', 'assigned', 'reassigned', 'moved',
                                               'status_change', 'priority_change', 'escalated',
                                               'merged', 'reopened', 'resolved', 'closed',
                                               'sla_breached')),
    from_value      VARCHAR(120),
    to_value        VARCHAR(120),
    performed_by    UUID         REFERENCES hr.employees(employee_id),
    created_dt      TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Voice Schema — phone ACD call detail records
-- ---------------------------------------------------------------------------

CREATE TABLE voice.calls (
    call_id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id                UUID         NOT NULL REFERENCES support.queues(queue_id),
    customer_id             UUID         REFERENCES support.customers(customer_id),
    agent_id                UUID         REFERENCES hr.employees(employee_id),
    transferred_to_queue_id UUID         REFERENCES support.queues(queue_id),
    direction               VARCHAR(10)  NOT NULL DEFAULT 'inbound'
                                CHECK (direction IN ('inbound', 'outbound')),
    offered_dt              TIMESTAMPTZ  NOT NULL,
    queued_dt               TIMESTAMPTZ  NOT NULL,
    ring_dt                 TIMESTAMPTZ,
    connect_dt              TIMESTAMPTZ,
    end_dt                  TIMESTAMPTZ  NOT NULL,
    wait_seconds            INTEGER      NOT NULL DEFAULT 0,
    talk_seconds            INTEGER      NOT NULL DEFAULT 0,
    hold_seconds            INTEGER      NOT NULL DEFAULT 0,
    hold_count              INTEGER      NOT NULL DEFAULT 0,
    after_call_work_seconds INTEGER      NOT NULL DEFAULT 0,
    abandoned               BOOLEAN      NOT NULL DEFAULT FALSE,
    disposition             VARCHAR(30)  NOT NULL
                                CHECK (disposition IN ('resolved', 'follow_up_ticket',
                                                       'transferred', 'voicemail',
                                                       'abandoned_customer', 'abandoned_timeout')),
    has_ticket              BOOLEAN      NOT NULL DEFAULT FALSE,   -- post-call follow-up ticket exists
    recording_url           VARCHAR(200),
    scenario_tag            VARCHAR(80),
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Chat Schema — live chat sessions + transcripts
-- ---------------------------------------------------------------------------

CREATE TABLE chat.sessions (
    session_id        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id          UUID         NOT NULL REFERENCES support.queues(queue_id),
    customer_id       UUID         REFERENCES support.customers(customer_id),
    agent_id          UUID         REFERENCES hr.employees(employee_id),
    started_dt        TIMESTAMPTZ  NOT NULL,
    first_response_dt TIMESTAMPTZ,
    ended_dt          TIMESTAMPTZ,
    status            VARCHAR(20)  NOT NULL DEFAULT 'waiting'
                          CHECK (status IN ('waiting', 'active', 'completed', 'abandoned', 'transferred')),
    visitor_agent     VARCHAR(120),
    platform          VARCHAR(20)  NOT NULL DEFAULT 'web'
                          CHECK (platform IN ('web', 'ios_app', 'android_app')),
    transferred_to_ticket BOOLEAN  NOT NULL DEFAULT FALSE,
    message_count     INTEGER      NOT NULL DEFAULT 0,
    wait_seconds      INTEGER      NOT NULL DEFAULT 0,
    duration_seconds  INTEGER      NOT NULL DEFAULT 0,
    scenario_tag      VARCHAR(80),
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE chat.messages (
    message_id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id   UUID         NOT NULL REFERENCES chat.sessions(session_id),
    sender_type  VARCHAR(20)  NOT NULL CHECK (sender_type IN ('customer', 'agent', 'system')),
    sender_id    UUID         REFERENCES hr.employees(employee_id),
    body         TEXT         NOT NULL,
    sent_dt      TIMESTAMPTZ  NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Survey Schema — sNPS surveys attached to closed interactions
-- ---------------------------------------------------------------------------

CREATE TABLE survey.surveys (
    survey_id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    channel            VARCHAR(20)  NOT NULL CHECK (channel IN ('voice', 'chat', 'ticket')),
    interaction_id     UUID         NOT NULL,             -- call_id / session_id / ticket_id
    customer_id        UUID         REFERENCES support.customers(customer_id),
    agent_id           UUID         REFERENCES hr.employees(employee_id),
    sent_dt            TIMESTAMPTZ  NOT NULL,
    responded_dt       TIMESTAMPTZ,
    nps_score          INTEGER      CHECK (nps_score BETWEEN 0 AND 10),
    csat_score         INTEGER      CHECK (csat_score BETWEEN 1 AND 5),
    reason_tag         VARCHAR(60),   -- picklist reason for the score
    verbatim           TEXT,          -- free-text comment
    is_complete        BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Training Schema — courses, assignments, completions
-- ---------------------------------------------------------------------------

CREATE TABLE training.courses (
    course_id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    code               VARCHAR(30)  NOT NULL UNIQUE,
    name               VARCHAR(200) NOT NULL,
    category           VARCHAR(60)  NOT NULL
                           CHECK (category IN ('onboarding', 'product', 'compliance',
                                               'soft_skills', 'system')),
    duration_minutes   INTEGER      NOT NULL,
    pass_score         NUMERIC(5,2) NOT NULL DEFAULT 80.00,
    is_mandatory       BOOLEAN      NOT NULL DEFAULT FALSE,
    target_departments VARCHAR(200),   -- comma-separated departments
    is_active          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE training.assignments (
    assignment_id  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id       UUID         NOT NULL REFERENCES hr.employees(employee_id),
    course_id      UUID         NOT NULL REFERENCES training.courses(course_id),
    assigned_dt    TIMESTAMPTZ  NOT NULL,
    due_dt         TIMESTAMPTZ  NOT NULL,
    started_dt     TIMESTAMPTZ,
    completed_dt   TIMESTAMPTZ,
    status         VARCHAR(20)  NOT NULL DEFAULT 'assigned'
                       CHECK (status IN ('assigned', 'in_progress', 'completed',
                                         'overdue', 'expired')),
    score_pct      NUMERIC(5,2),
    attempts       INTEGER      NOT NULL DEFAULT 0,
    trigger_reason VARCHAR(60)  NOT NULL DEFAULT 'scheduled'
                       CHECK (trigger_reason IN ('scheduled', 'onboarding', 'qa_finding',
                                                 'new_feature', 'recall_event', 'manager_request')),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Control Schema — generator state and stats
-- ---------------------------------------------------------------------------

CREATE TABLE control.generator_state (
    state_id                SERIAL       PRIMARY KEY,
    is_running              BOOLEAN      NOT NULL DEFAULT FALSE,
    is_paused               BOOLEAN      NOT NULL DEFAULT FALSE,
    mode                    VARCHAR(20)  NOT NULL DEFAULT 'stopped'
                                CHECK (mode IN ('realtime', 'backfill', 'stopped')),
    active_scenario         VARCHAR(50)  NOT NULL DEFAULT 'normal',
    volume_multiplier       NUMERIC(5,2) NOT NULL DEFAULT 1.0,
    backfill_start_date     DATE,
    backfill_end_date       DATE,
    backfill_current_date   DATE,
    tick_interval_seconds   INTEGER      NOT NULL DEFAULT 30,
    last_tick_at            TIMESTAMPTZ,
    started_at              TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE control.generation_stats (
    stat_id                     BIGSERIAL    PRIMARY KEY,
    recorded_at                 TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    tickets_generated           INTEGER      NOT NULL DEFAULT 0,
    calls_generated             INTEGER      NOT NULL DEFAULT 0,
    chat_sessions_generated     INTEGER      NOT NULL DEFAULT 0,
    surveys_generated           INTEGER      NOT NULL DEFAULT 0,
    scenario_tag                VARCHAR(80),
    simulation_dt               TIMESTAMPTZ,
    wall_clock_ms               INTEGER
);

CREATE TABLE control.active_scenarios (
    scenario_id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_name VARCHAR(50)  NOT NULL UNIQUE,
    activated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE control.scenario_schedules (
    schedule_id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_name VARCHAR(50)  NOT NULL,
    start_date    DATE         NOT NULL,
    end_date      DATE         NOT NULL,
    label         VARCHAR(100),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_scenario_schedules_dates ON control.scenario_schedules (start_date, end_date);

INSERT INTO control.generator_state (is_running, is_paused, mode)
VALUES (FALSE, FALSE, 'stopped');

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

CREATE INDEX idx_hr_emp_location   ON hr.employees (location_id, status);
CREATE INDEX idx_hr_emp_dept       ON hr.employees (department, status);

CREATE INDEX idx_cat_queue         ON support.categories (queue_id);
CREATE INDEX idx_cust_email        ON support.customers (email);

CREATE INDEX idx_ticket_created    ON support.tickets (created_dt);
CREATE INDEX idx_ticket_status     ON support.tickets (status, queue_id);
CREATE INDEX idx_ticket_agent      ON support.tickets (assigned_agent_id, status);
CREATE INDEX idx_ticket_customer   ON support.tickets (customer_id);
CREATE INDEX idx_ticket_channel    ON support.tickets (channel, created_dt);
CREATE INDEX idx_ticket_scenario   ON support.tickets (scenario_tag);

CREATE INDEX idx_tcom_ticket       ON support.ticket_comments (ticket_id, created_dt);
CREATE INDEX idx_tact_ticket       ON support.ticket_actions (ticket_id, created_dt);
CREATE INDEX idx_tact_type         ON support.ticket_actions (action_type, created_dt);

CREATE INDEX idx_call_offered      ON voice.calls (offered_dt);
CREATE INDEX idx_call_queue        ON voice.calls (queue_id, offered_dt);
CREATE INDEX idx_call_agent        ON voice.calls (agent_id, offered_dt);
CREATE INDEX idx_call_abandoned    ON voice.calls (abandoned, offered_dt);

CREATE INDEX idx_chat_started      ON chat.sessions (started_dt);
CREATE INDEX idx_chat_agent        ON chat.sessions (agent_id, started_dt);
CREATE INDEX idx_chat_status       ON chat.sessions (status);
CREATE INDEX idx_msg_session       ON chat.messages (session_id, sent_dt);

CREATE INDEX idx_srv_channel       ON survey.surveys (channel, sent_dt);
CREATE INDEX idx_srv_customer      ON survey.surveys (customer_id);
CREATE INDEX idx_srv_agent         ON survey.surveys (agent_id);
CREATE INDEX idx_srv_interaction   ON survey.surveys (interaction_id);

CREATE INDEX idx_trn_agent         ON training.assignments (agent_id, status);
CREATE INDEX idx_trn_course        ON training.assignments (course_id, status);
CREATE INDEX idx_trn_due           ON training.assignments (due_dt, status);
