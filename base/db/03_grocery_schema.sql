-- =============================================================================
-- Verisim — Grocery Industry Database
-- Database: grocery  (one DB per industry in the verisim-base postgres)
-- Schemas: hr, pos, timeclock, ordering, fulfillment, transport, inv, control
--          hr/pos/inv follow gas_station patterns; new schemas model the
--          full supply chain from store ordering through warehouse fulfillment
--          and truck delivery.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schemas
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS pos;
CREATE SCHEMA IF NOT EXISTS timeclock;
CREATE SCHEMA IF NOT EXISTS ordering;
CREATE SCHEMA IF NOT EXISTS fulfillment;
CREATE SCHEMA IF NOT EXISTS transport;
CREATE SCHEMA IF NOT EXISTS inv;
CREATE SCHEMA IF NOT EXISTS control;

-- ---------------------------------------------------------------------------
-- HR Schema — source of truth for locations and employees
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
    location_type   VARCHAR(20)  NOT NULL CHECK (location_type IN ('store', 'warehouse', 'dc')),
    store_sqft      INTEGER,
    num_aisles      INTEGER,
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
                            'store', 'produce', 'deli', 'bakery', 'meat',
                            'warehouse', 'management', 'transport')),
    job_title           VARCHAR(100) NOT NULL,
    hourly_rate         NUMERIC(8,2) NOT NULL,
    status              VARCHAR(20)  NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active', 'terminated', 'on_leave')),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- POS Schema — point-of-sale system
-- ---------------------------------------------------------------------------

CREATE TABLE pos.departments (
    department_id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(100) NOT NULL UNIQUE,
    code            VARCHAR(10)  NOT NULL UNIQUE,
    manager_id      UUID         REFERENCES hr.employees(employee_id),
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE pos.products (
    product_id      UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    sku             VARCHAR(50)  NOT NULL UNIQUE,
    upc             VARCHAR(14)  UNIQUE,
    name            VARCHAR(200) NOT NULL,
    brand           VARCHAR(100),
    department_id   UUID         NOT NULL REFERENCES pos.departments(department_id),
    category        VARCHAR(100) NOT NULL,
    subcategory     VARCHAR(100),
    unit_size       VARCHAR(50),
    unit_of_measure VARCHAR(20)  NOT NULL DEFAULT 'each'
                        CHECK (unit_of_measure IN ('each', 'lb', 'oz', 'kg', 'pack', 'case')),
    cost            NUMERIC(8,4) NOT NULL,
    current_price   NUMERIC(8,2) NOT NULL,
    is_organic      BOOLEAN      NOT NULL DEFAULT FALSE,
    is_local        BOOLEAN      NOT NULL DEFAULT FALSE,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE pos.price_history (
    price_history_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id      UUID         NOT NULL REFERENCES pos.products(product_id),
    old_price       NUMERIC(8,2) NOT NULL,
    new_price       NUMERIC(8,2) NOT NULL,
    changed_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    changed_by      UUID         REFERENCES hr.employees(employee_id)
);

CREATE TABLE pos.coupons (
    coupon_id       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    code            VARCHAR(50)  NOT NULL UNIQUE,
    description     VARCHAR(200) NOT NULL,
    coupon_type     VARCHAR(30)  NOT NULL
                        CHECK (coupon_type IN ('percent_off', 'dollar_off', 'bogo', 'free_item')),
    discount_value  NUMERIC(8,2) NOT NULL,
    min_purchase    NUMERIC(8,2),
    department_id   UUID         REFERENCES pos.departments(department_id),
    product_id      UUID         REFERENCES pos.products(product_id),
    max_uses        INTEGER,
    uses_count      INTEGER      NOT NULL DEFAULT 0,
    valid_from      DATE         NOT NULL,
    valid_until     DATE         NOT NULL,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE pos.combo_deals (
    deal_id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name                VARCHAR(200) NOT NULL,
    description         VARCHAR(300),
    deal_type           VARCHAR(30)  NOT NULL
                            CHECK (deal_type IN ('x_for_price', 'bogo', 'percent_off_second', 'mix_and_match')),
    trigger_qty         INTEGER      NOT NULL DEFAULT 2,
    trigger_product_id  UUID         REFERENCES pos.products(product_id),
    trigger_department_id UUID       REFERENCES pos.departments(department_id),
    deal_price          NUMERIC(8,2) NOT NULL,
    valid_from          DATE         NOT NULL,
    valid_until         DATE         NOT NULL,
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE pos.loyalty_members (
    member_id       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) NOT NULL UNIQUE,
    phone           VARCHAR(20),
    signup_date     DATE         NOT NULL,
    points_balance  INTEGER      NOT NULL DEFAULT 0,
    tier            VARCHAR(20)  NOT NULL DEFAULT 'bronze'
                        CHECK (tier IN ('bronze', 'silver', 'gold', 'platinum')),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE pos.transactions (
    transaction_id  UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id     UUID          NOT NULL REFERENCES hr.locations(location_id),
    employee_id     UUID          REFERENCES hr.employees(employee_id),
    member_id       UUID          REFERENCES pos.loyalty_members(member_id),
    transaction_dt  TIMESTAMPTZ   NOT NULL,
    subtotal        NUMERIC(10,2) NOT NULL,
    coupon_savings  NUMERIC(10,2) NOT NULL DEFAULT 0,
    deal_savings    NUMERIC(10,2) NOT NULL DEFAULT 0,
    tax             NUMERIC(10,2) NOT NULL,
    total           NUMERIC(10,2) NOT NULL,
    payment_method  VARCHAR(30)   NOT NULL
                        CHECK (payment_method IN ('cash', 'credit', 'debit', 'ebt', 'mobile_pay', 'loyalty_points')),
    scenario_tag    VARCHAR(50),
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE pos.transaction_items (
    item_id         UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id  UUID          NOT NULL REFERENCES pos.transactions(transaction_id),
    product_id      UUID          NOT NULL REFERENCES pos.products(product_id),
    quantity        NUMERIC(8,3)  NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(8,2)  NOT NULL,
    discount        NUMERIC(8,2)  NOT NULL DEFAULT 0,
    coupon_id       UUID          REFERENCES pos.coupons(coupon_id),
    deal_id         UUID          REFERENCES pos.combo_deals(deal_id),
    line_total      NUMERIC(10,2) NOT NULL
);

-- ---------------------------------------------------------------------------
-- Timeclock Schema — employee time tracking
-- ---------------------------------------------------------------------------

CREATE TABLE timeclock.events (
    event_id        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    employee_id     UUID         NOT NULL REFERENCES hr.employees(employee_id),
    location_id     UUID         NOT NULL REFERENCES hr.locations(location_id),
    event_type      VARCHAR(20)  NOT NULL
                        CHECK (event_type IN ('clock_in', 'clock_out', 'break_start', 'break_end')),
    event_dt        TIMESTAMPTZ  NOT NULL,
    notes           VARCHAR(200),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Ordering Schema — store order requests to warehouse
-- ---------------------------------------------------------------------------

CREATE TABLE ordering.store_orders (
    order_id                UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    store_location_id       UUID         NOT NULL REFERENCES hr.locations(location_id),
    warehouse_location_id   UUID         NOT NULL REFERENCES hr.locations(location_id),
    created_by              UUID         REFERENCES hr.employees(employee_id),
    order_dt                TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    requested_delivery_dt   DATE,
    approved_by             UUID         REFERENCES hr.employees(employee_id),
    approved_dt             TIMESTAMPTZ,
    status                  VARCHAR(20)  NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending','approved','picking','shipped','delivered','cancelled')),
    notes                   VARCHAR(300),
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE ordering.store_order_items (
    item_id             UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id            UUID    NOT NULL REFERENCES ordering.store_orders(order_id),
    product_id          UUID    NOT NULL REFERENCES pos.products(product_id),
    quantity_requested  INTEGER NOT NULL CHECK (quantity_requested > 0),
    quantity_approved   INTEGER,
    notes               VARCHAR(200)
);

-- ---------------------------------------------------------------------------
-- Fulfillment Schema — warehouse picks and packs orders
-- ---------------------------------------------------------------------------

CREATE TABLE fulfillment.orders (
    fulfillment_id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    store_order_id          UUID        NOT NULL REFERENCES ordering.store_orders(order_id),
    warehouse_location_id   UUID        NOT NULL REFERENCES hr.locations(location_id),
    assigned_to             UUID        REFERENCES hr.employees(employee_id),
    status                  VARCHAR(20) NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending','picking','packed','loaded','cancelled')),
    started_at              TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE fulfillment.items (
    item_id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    fulfillment_id      UUID        NOT NULL REFERENCES fulfillment.orders(fulfillment_id),
    product_id          UUID        NOT NULL REFERENCES pos.products(product_id),
    quantity_requested  INTEGER     NOT NULL,
    quantity_picked     INTEGER     NOT NULL DEFAULT 0,
    pick_status         VARCHAR(20) NOT NULL DEFAULT 'pending'
                            CHECK (pick_status IN ('pending','picked','short','cancelled'))
);

-- ---------------------------------------------------------------------------
-- Transport Schema — truck delivery tracking
-- ---------------------------------------------------------------------------

CREATE TABLE transport.trucks (
    truck_id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    license_plate       VARCHAR(20) NOT NULL UNIQUE,
    make                VARCHAR(50),
    model               VARCHAR(50),
    year                INTEGER,
    capacity_pallets    INTEGER     NOT NULL DEFAULT 24,
    is_active           BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE transport.loads (
    load_id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    truck_id                UUID        NOT NULL REFERENCES transport.trucks(truck_id),
    driver_id               UUID        REFERENCES hr.employees(employee_id),
    warehouse_location_id   UUID        NOT NULL REFERENCES hr.locations(location_id),
    destination_location_id UUID        NOT NULL REFERENCES hr.locations(location_id),
    departed_at             TIMESTAMPTZ,
    arrived_at              TIMESTAMPTZ,
    status                  VARCHAR(20) NOT NULL DEFAULT 'loading'
                                CHECK (status IN ('loading','in_transit','delivered','cancelled')),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE transport.load_items (
    item_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    load_id             UUID NOT NULL REFERENCES transport.loads(load_id),
    fulfillment_id      UUID NOT NULL REFERENCES fulfillment.orders(fulfillment_id),
    store_order_id      UUID NOT NULL REFERENCES ordering.store_orders(order_id)
);

-- ---------------------------------------------------------------------------
-- Inventory Schema — stock management
-- ---------------------------------------------------------------------------

CREATE TABLE inv.products (
    inv_product_id  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id      UUID         NOT NULL REFERENCES pos.products(product_id) UNIQUE,
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
    quantity_on_hand    INTEGER     NOT NULL DEFAULT 0 CHECK (quantity_on_hand >= 0),
    quantity_reserved   INTEGER     NOT NULL DEFAULT 0,
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (product_id, location_id)
);

CREATE TABLE inv.receipts (
    receipt_id      UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id     UUID          NOT NULL REFERENCES hr.locations(location_id),
    received_by     UUID          REFERENCES hr.employees(employee_id),
    received_dt     TIMESTAMPTZ   NOT NULL,
    supplier_name   VARCHAR(200),
    po_number       VARCHAR(50),
    load_id         UUID          REFERENCES transport.loads(load_id),
    total_cost      NUMERIC(12,2),
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE inv.receipt_items (
    receipt_item_id UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    receipt_id      UUID          NOT NULL REFERENCES inv.receipts(receipt_id),
    product_id      UUID          NOT NULL REFERENCES pos.products(product_id),
    quantity        INTEGER       NOT NULL CHECK (quantity > 0),
    unit_cost       NUMERIC(8,4)  NOT NULL,
    line_total      NUMERIC(12,2) NOT NULL
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
    stat_id                         BIGSERIAL    PRIMARY KEY,
    recorded_at                     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    pos_transactions_generated      INTEGER      NOT NULL DEFAULT 0,
    timeclock_events_generated      INTEGER      NOT NULL DEFAULT 0,
    orders_generated                INTEGER      NOT NULL DEFAULT 0,
    scenario_tag                    VARCHAR(50),
    simulation_dt                   TIMESTAMPTZ,
    wall_clock_ms                   INTEGER
);

INSERT INTO control.generator_state (is_running, is_paused, mode)
VALUES (FALSE, FALSE, 'stopped');

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

CREATE INDEX idx_pos_txn_dt           ON pos.transactions (transaction_dt);
CREATE INDEX idx_pos_txn_location     ON pos.transactions (location_id);
CREATE INDEX idx_pos_txn_member       ON pos.transactions (member_id);
CREATE INDEX idx_pos_items_txn        ON pos.transaction_items (transaction_id);
CREATE INDEX idx_pos_items_product    ON pos.transaction_items (product_id);
CREATE INDEX idx_pos_members_email    ON pos.loyalty_members (email);
CREATE INDEX idx_pos_products_dept    ON pos.products (department_id);
CREATE INDEX idx_pos_coupons_active   ON pos.coupons (is_active, valid_until);
CREATE INDEX idx_pos_deals_active     ON pos.combo_deals (is_active, valid_until);

CREATE INDEX idx_tc_events_emp        ON timeclock.events (employee_id, event_dt DESC);
CREATE INDEX idx_tc_events_loc        ON timeclock.events (location_id, event_dt DESC);

CREATE INDEX idx_ord_orders_store     ON ordering.store_orders (store_location_id, status);
CREATE INDEX idx_ord_orders_wh        ON ordering.store_orders (warehouse_location_id, status);
CREATE INDEX idx_ord_items_order      ON ordering.store_order_items (order_id);

CREATE INDEX idx_ful_orders_status    ON fulfillment.orders (status);
CREATE INDEX idx_ful_items_order      ON fulfillment.items (fulfillment_id);

CREATE INDEX idx_trn_loads_status     ON transport.loads (status);
CREATE INDEX idx_trn_loads_dest       ON transport.loads (destination_location_id, status);

CREATE INDEX idx_inv_stock_location   ON inv.stock_levels (location_id);
CREATE INDEX idx_inv_stock_product    ON inv.stock_levels (product_id);
CREATE INDEX idx_inv_receipts_dt      ON inv.receipts (received_dt);

CREATE INDEX idx_hr_emp_location      ON hr.employees (location_id, status);
CREATE INDEX idx_control_stats        ON control.generation_stats (recorded_at DESC);
