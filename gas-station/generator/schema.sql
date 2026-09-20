-- =============================================================================
-- Verisim — Gas Station Industry Database
-- Database: gas_station  (one DB per industry in the verisim-base postgres)
-- Schemas: hr, pos, fuel, inv   (source data written by verisim-gas-station)
--          control               (platform-level: generator state + stats)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schemas
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS pos;
CREATE SCHEMA IF NOT EXISTS fuel;
CREATE SCHEMA IF NOT EXISTS inv;
CREATE SCHEMA IF NOT EXISTS control;

-- ---------------------------------------------------------------------------
-- HR Schema — source of truth for locations and employees
-- ---------------------------------------------------------------------------

CREATE TABLE hr.locations (
    location_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(100) NOT NULL,
    address         VARCHAR(200) NOT NULL,
    city            VARCHAR(100) NOT NULL,
    state           CHAR(2)      NOT NULL,
    zip             VARCHAR(10)  NOT NULL,
    phone           VARCHAR(20),
    opened_date     DATE         NOT NULL,
    type            VARCHAR(20)  NOT NULL CHECK (type IN ('store', 'fuel_only', 'combo')),
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE hr.employees (
    employee_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id         UUID        NOT NULL REFERENCES hr.locations(location_id),
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100) NOT NULL,
    email               VARCHAR(255) NOT NULL UNIQUE,
    hire_date           DATE         NOT NULL,
    termination_date    DATE,
    department          VARCHAR(50)  NOT NULL CHECK (department IN ('store', 'fuel', 'management')),
    job_title           VARCHAR(100) NOT NULL,
    hourly_rate         NUMERIC(8,2) NOT NULL,
    status              VARCHAR(20)  NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'terminated', 'on_leave')),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- POS Schema — point-of-sale system
-- ---------------------------------------------------------------------------

CREATE TABLE pos.employees (
    pos_employee_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    employee_id     UUID        NOT NULL REFERENCES hr.employees(employee_id),
    location_id     UUID        NOT NULL REFERENCES hr.locations(location_id),
    pin             VARCHAR(6),
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE pos.loyalty_members (
    member_id       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) NOT NULL UNIQUE,
    phone           VARCHAR(20),
    signup_date     DATE         NOT NULL,
    points_balance  INTEGER      NOT NULL DEFAULT 0,
    tier            VARCHAR(20)  NOT NULL DEFAULT 'bronze' CHECK (tier IN ('bronze', 'silver', 'gold', 'platinum')),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE pos.products (
    product_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    sku             VARCHAR(50)  NOT NULL UNIQUE,
    name            VARCHAR(200) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    subcategory     VARCHAR(100),
    cost            NUMERIC(8,4) NOT NULL,
    current_price   NUMERIC(8,2) NOT NULL,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE pos.price_history (
    price_history_id UUID       PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id      UUID        NOT NULL REFERENCES pos.products(product_id),
    old_price       NUMERIC(8,2) NOT NULL,
    new_price       NUMERIC(8,2) NOT NULL,
    changed_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    changed_by      UUID        REFERENCES hr.employees(employee_id)
);

CREATE TABLE pos.transactions (
    transaction_id  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id     UUID        NOT NULL REFERENCES hr.locations(location_id),
    employee_id     UUID        REFERENCES hr.employees(employee_id),
    member_id       UUID        REFERENCES pos.loyalty_members(member_id),
    transaction_dt  TIMESTAMPTZ  NOT NULL,
    subtotal        NUMERIC(10,2) NOT NULL,
    tax             NUMERIC(10,2) NOT NULL,
    total           NUMERIC(10,2) NOT NULL,
    payment_method  VARCHAR(30)  NOT NULL CHECK (payment_method IN ('cash', 'credit', 'debit', 'mobile_pay', 'loyalty_points')),
    scenario_tag    VARCHAR(50),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE pos.transaction_items (
    item_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id  UUID        NOT NULL REFERENCES pos.transactions(transaction_id),
    product_id      UUID        NOT NULL REFERENCES pos.products(product_id),
    quantity        INTEGER      NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(8,2) NOT NULL,
    discount        NUMERIC(8,2) NOT NULL DEFAULT 0,
    line_total      NUMERIC(10,2) NOT NULL
);

-- ---------------------------------------------------------------------------
-- Fuel Schema — fuel dispensing system
-- ---------------------------------------------------------------------------

CREATE TABLE fuel.grades (
    grade_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(50)  NOT NULL UNIQUE,
    octane_rating   VARCHAR(10),
    current_price   NUMERIC(8,4) NOT NULL,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE fuel.price_history (
    price_history_id UUID       PRIMARY KEY DEFAULT gen_random_uuid(),
    grade_id        UUID        NOT NULL REFERENCES fuel.grades(grade_id),
    old_price       NUMERIC(8,4) NOT NULL,
    new_price       NUMERIC(8,4) NOT NULL,
    changed_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE fuel.pumps (
    pump_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id     UUID        NOT NULL REFERENCES hr.locations(location_id),
    pump_number     INTEGER      NOT NULL,
    num_sides       INTEGER      NOT NULL DEFAULT 2,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (location_id, pump_number)
);

CREATE TABLE fuel.transactions (
    transaction_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    pump_id             UUID        NOT NULL REFERENCES fuel.pumps(pump_id),
    location_id         UUID        NOT NULL REFERENCES hr.locations(location_id),
    employee_id         UUID        REFERENCES hr.employees(employee_id),
    member_id           UUID        REFERENCES pos.loyalty_members(member_id),
    transaction_dt      TIMESTAMPTZ  NOT NULL,
    grade_id            UUID        NOT NULL REFERENCES fuel.grades(grade_id),
    gallons             NUMERIC(8,4) NOT NULL,
    price_per_gallon    NUMERIC(8,4) NOT NULL,
    total_amount        NUMERIC(10,2) NOT NULL,
    payment_method      VARCHAR(30)  NOT NULL CHECK (payment_method IN ('cash', 'credit', 'debit', 'mobile_pay', 'loyalty_points', 'pay_at_pump')),
    scenario_tag        VARCHAR(50),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Inventory Schema — inventory management system
-- ---------------------------------------------------------------------------

CREATE TABLE inv.products (
    inv_product_id  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id      UUID        NOT NULL REFERENCES pos.products(product_id) UNIQUE,
    reorder_point   INTEGER      NOT NULL DEFAULT 20,
    reorder_qty     INTEGER      NOT NULL DEFAULT 100,
    unit_of_measure VARCHAR(20)  NOT NULL DEFAULT 'each',
    supplier_name   VARCHAR(200),
    lead_time_days  INTEGER      NOT NULL DEFAULT 2,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE inv.stock_levels (
    stock_id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id          UUID        NOT NULL REFERENCES pos.products(product_id),
    location_id         UUID        NOT NULL REFERENCES hr.locations(location_id),
    quantity_on_hand    INTEGER      NOT NULL DEFAULT 0 CHECK (quantity_on_hand >= 0),
    quantity_reserved   INTEGER      NOT NULL DEFAULT 0,
    last_updated        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (product_id, location_id)
);

CREATE TABLE inv.receipts (
    receipt_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id     UUID        NOT NULL REFERENCES hr.locations(location_id),
    received_by     UUID        REFERENCES hr.employees(employee_id),
    received_dt     TIMESTAMPTZ  NOT NULL,
    supplier_name   VARCHAR(200),
    po_number       VARCHAR(50),
    total_cost      NUMERIC(12,2),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE inv.receipt_items (
    receipt_item_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    receipt_id      UUID        NOT NULL REFERENCES inv.receipts(receipt_id),
    product_id      UUID        NOT NULL REFERENCES pos.products(product_id),
    quantity        INTEGER      NOT NULL CHECK (quantity > 0),
    unit_cost       NUMERIC(8,4) NOT NULL,
    line_total      NUMERIC(12,2) NOT NULL
);

-- ---------------------------------------------------------------------------
-- Control Schema — generator state and stats
-- ---------------------------------------------------------------------------

CREATE TABLE control.generator_state (
    state_id                SERIAL      PRIMARY KEY,
    is_running              BOOLEAN      NOT NULL DEFAULT FALSE,
    is_paused               BOOLEAN      NOT NULL DEFAULT FALSE,
    mode                    VARCHAR(20)  NOT NULL DEFAULT 'stopped' CHECK (mode IN ('realtime', 'backfill', 'stopped')),
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
    stat_id                         BIGSERIAL   PRIMARY KEY,
    recorded_at                     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    pos_transactions_generated      INTEGER      NOT NULL DEFAULT 0,
    fuel_transactions_generated     INTEGER      NOT NULL DEFAULT 0,
    inventory_receipts_generated    INTEGER      NOT NULL DEFAULT 0,
    scenario_tag                    VARCHAR(50),
    simulation_dt                   TIMESTAMPTZ,
    wall_clock_ms                   INTEGER
);

-- Seed single generator state row
INSERT INTO control.generator_state (is_running, is_paused, mode)
VALUES (FALSE, FALSE, 'stopped');

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

CREATE INDEX idx_pos_transactions_dt       ON pos.transactions (transaction_dt);
CREATE INDEX idx_pos_transactions_location ON pos.transactions (location_id);
CREATE INDEX idx_pos_transactions_scenario ON pos.transactions (scenario_tag);
CREATE INDEX idx_pos_items_transaction     ON pos.transaction_items (transaction_id);
CREATE INDEX idx_pos_items_product         ON pos.transaction_items (product_id);
CREATE INDEX idx_pos_members_email         ON pos.loyalty_members (email);

CREATE INDEX idx_fuel_transactions_dt      ON fuel.transactions (transaction_dt);
CREATE INDEX idx_fuel_transactions_location ON fuel.transactions (location_id);
CREATE INDEX idx_fuel_transactions_pump    ON fuel.transactions (pump_id);
CREATE INDEX idx_fuel_price_history_grade  ON fuel.price_history (grade_id, changed_at DESC);

CREATE INDEX idx_inv_stock_location        ON inv.stock_levels (location_id);
CREATE INDEX idx_inv_stock_product         ON inv.stock_levels (product_id);
CREATE INDEX idx_inv_receipts_dt           ON inv.receipts (received_dt);

CREATE INDEX idx_hr_employees_location     ON hr.employees (location_id, status);
CREATE INDEX idx_control_stats_recorded    ON control.generation_stats (recorded_at DESC);

-- ---------------------------------------------------------------------------
-- Seed Data — fuel grades
-- ---------------------------------------------------------------------------

INSERT INTO fuel.grades (name, octane_rating, current_price) VALUES
    ('Regular',  '87',   3.2990),
    ('Plus',     '89',   3.5990),
    ('Premium',  '93',   3.8990),
    ('Diesel',   NULL,   3.7990);
