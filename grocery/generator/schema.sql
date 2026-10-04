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
    latitude        NUMERIC(9,6),  -- added for transport distance computation (verisim#13)
    longitude       NUMERIC(9,6),  -- added for transport distance computation (verisim#13)
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
    -- The price this SKU is normally sold at: the pivot of its demand curve.
    -- current_price is the price of record, which may be a weekly-ad
    -- promoted price; reference_price is the unshelved everyday price. NULL
    -- on a row that predates t_08deeddf means "same as current_price".
    reference_price NUMERIC(8,2),
    -- This SKU's own price sensitivity: units ~ (price/reference_price) **
    -- price_elasticity. Negative = ordinary retail demand (raise the price,
    -- sell fewer). NULL falls back to pricing.default_price_elasticity.
    price_elasticity NUMERIC(4,3),
    is_organic      BOOLEAN      NOT NULL DEFAULT FALSE,
    is_local        BOOLEAN      NOT NULL DEFAULT FALSE,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    is_perishable   BOOLEAN      NOT NULL DEFAULT FALSE,
    shelf_life_days SMALLINT,
    is_on_ad        BOOLEAN      NOT NULL DEFAULT FALSE,
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

-- The customer / household master dimension. `pos.loyalty_members` is a
-- *card*: `pos.transactions.member_id` resolves to it or is NULL, and nothing
-- on either side carried a demographic or household attribute, so the marts a
-- grocery warehouse actually wants (RFM cohorts, basket affinity, segment
-- penetration, household-size vs basket size) had no conformed dimension to
-- join to.
--
-- The link is `loyalty_members.customer_id`, nullable and NOT unique: one
-- household may hold several cards (two adults, a card each) and a card-less
-- shopper belongs to no household at all. `household_size` is the household's
-- size, so it is legitimately larger than its card count — the children in it
-- never signed up. Populated by `models/customers.py`, which carries its own
-- `IF NOT EXISTS` copy of this DDL for a data dir generated before it (a
-- schema.sql change only reaches a fresh bootstrap).
--
-- Deliberately NOT stored here: loyalty-card count and first-signup date.
-- Both are a LEFT JOIN from `pos.loyalty_members`, and a stored copy would go
-- stale the moment a second card joined the household or a back-dated signup
-- landed out of order. Likewise there is no `pos.transactions.customer_id`:
-- denormalising the snowflake into the fact table would re-attribute every
-- anonymous shopper. The mart joins transactions -> loyalty_members ->
-- customers.
CREATE TABLE pos.customers (
    customer_id     UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    age_band        VARCHAR(20)  NOT NULL
                        CHECK (age_band IN ('under_25', '25_34', '35_44',
                                            '45_54', '55_64', '65_plus')),
    household_size  SMALLINT     NOT NULL CHECK (household_size BETWEEN 1 AND 12),
    segment         VARCHAR(30)  NOT NULL
                        CHECK (segment IN ('value_seeker', 'family_stock_up',
                                           'convenience', 'health_conscious',
                                           'premium_enthusiast', 'budget_constrained')),
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
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
    -- The household this card belongs to. NULL on a card created before this
    -- column existed and not yet picked up by the backfill; NULL for a
    -- walk-in shopper who never signed up.
    customer_id     UUID         REFERENCES pos.customers(customer_id),
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
    distance_miles          NUMERIC(8,2),  -- haversine distance at dispatch time (verisim#13)
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
    expiry_date         DATE,
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

CREATE INDEX idx_pos_txn_dt           ON pos.transactions (transaction_dt);
CREATE INDEX idx_pos_txn_location     ON pos.transactions (location_id);
CREATE INDEX idx_pos_txn_member       ON pos.transactions (member_id);
CREATE INDEX idx_pos_items_txn        ON pos.transaction_items (transaction_id);
CREATE INDEX idx_pos_items_product    ON pos.transaction_items (product_id);
CREATE INDEX idx_pos_members_email    ON pos.loyalty_members (email);
CREATE INDEX idx_pos_members_customer  ON pos.loyalty_members (customer_id);
CREATE INDEX idx_pos_customers_segment ON pos.customers (segment, age_band);
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

-- ---------------------------------------------------------------------------
-- Phase 2: Shrinkage / perishables
-- ---------------------------------------------------------------------------
CREATE TABLE inv.shrinkage_events (
    shrinkage_id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id     UUID         NOT NULL REFERENCES pos.products(product_id),
    location_id    UUID         NOT NULL REFERENCES hr.locations(location_id),
    quantity       INTEGER      NOT NULL CHECK (quantity > 0),
    reason         VARCHAR(30)  NOT NULL
                       CHECK (reason IN ('expired','damaged','theft','spoilage','markdown_waste')),
    estimated_cost NUMERIC(10,2),
    recorded_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    recorded_by    UUID         REFERENCES hr.employees(employee_id)
);
CREATE INDEX idx_shrinkage_product  ON inv.shrinkage_events (product_id);
CREATE INDEX idx_shrinkage_location ON inv.shrinkage_events (location_id);
CREATE INDEX idx_shrinkage_date     ON inv.shrinkage_events (recorded_at);

-- ---------------------------------------------------------------------------
-- Phase 3: Weekly ads / promotions
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS pricing;

CREATE TABLE pricing.weekly_ads (
    ad_id      UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    ad_name    VARCHAR(100) NOT NULL,
    start_date DATE         NOT NULL,
    end_date   DATE         NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE pricing.ad_items (
    ad_item_id     UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    ad_id          UUID         NOT NULL REFERENCES pricing.weekly_ads(ad_id),
    product_id     UUID         NOT NULL REFERENCES pos.products(product_id),
    promoted_price NUMERIC(8,2),
    discount_pct   NUMERIC(5,2),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE(ad_id, product_id)
);
CREATE INDEX idx_ad_items_ad      ON pricing.ad_items (ad_id);
CREATE INDEX idx_ad_items_product ON pricing.ad_items (product_id);

-- ---------------------------------------------------------------------------
-- Phase 4: Labor scheduling
-- ---------------------------------------------------------------------------
CREATE TABLE hr.schedules (
    schedule_id    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id    UUID        NOT NULL REFERENCES hr.locations(location_id),
    employee_id    UUID        NOT NULL REFERENCES hr.employees(employee_id),
    scheduled_date DATE        NOT NULL,
    department     VARCHAR(50),
    shift_start    TIME        NOT NULL,
    shift_end      TIME        NOT NULL,
    status         VARCHAR(20) NOT NULL DEFAULT 'scheduled'
                               CHECK (status IN ('scheduled','confirmed','completed','no_show','called_out','adjusted')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_schedules_date     ON hr.schedules (scheduled_date);
CREATE INDEX idx_schedules_employee ON hr.schedules (employee_id);
CREATE INDEX idx_schedules_location ON hr.schedules (location_id);

-- ---------------------------------------------------------------------------
-- Phase 5: Loyalty point transactions
-- ---------------------------------------------------------------------------
CREATE TABLE pos.loyalty_point_transactions (
    pt_id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    member_id       UUID        NOT NULL REFERENCES pos.loyalty_members(member_id),
    transaction_id  UUID        REFERENCES pos.transactions(transaction_id),
    points_earned   INTEGER     NOT NULL DEFAULT 0,
    points_redeemed INTEGER     NOT NULL DEFAULT 0,
    reason          VARCHAR(50) NOT NULL DEFAULT 'purchase'
                                CHECK (reason IN ('purchase','redemption','bonus','tier_upgrade','adjustment','expiry')),
    balance_after   INTEGER     NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_loyalty_pts_member ON pos.loyalty_point_transactions (member_id);
CREATE INDEX idx_loyalty_pts_txn    ON pos.loyalty_point_transactions (transaction_id);
CREATE INDEX idx_loyalty_pts_date   ON pos.loyalty_point_transactions (created_at);

-- ---------------------------------------------------------------------------
-- Phase 6: Customer returns & refunds (t_2382c671)
-- A return references the original transaction; refund amounts are prorated
-- from the transaction total so SUM(refunds) <= transactions.total always
-- holds (dbt reconcilable). is_restocked drives stock reintegration.
-- ---------------------------------------------------------------------------

CREATE TABLE pos.returns (
    return_id       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id  UUID         NOT NULL REFERENCES pos.transactions(transaction_id),
    location_id     UUID         NOT NULL REFERENCES hr.locations(location_id),
    member_id       UUID         REFERENCES pos.loyalty_members(member_id),
    return_dt       TIMESTAMPTZ  NOT NULL,
    reason          VARCHAR(30)  NOT NULL
                        CHECK (reason IN ('defective','wrong_item','changed_mind',
                                          'damaged_in_transit','price_found_lower','other')),
    refund_method   VARCHAR(20)  NOT NULL
                        CHECK (refund_method IN ('original_payment','cash','store_credit')),
    refund_amount   NUMERIC(10,2) NOT NULL CHECK (refund_amount >= 0),
    is_restocked    BOOLEAN      NOT NULL DEFAULT FALSE,
    scenario_tag    VARCHAR(50),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE pos.return_items (
    return_item_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    return_id           UUID        NOT NULL REFERENCES pos.returns(return_id),
    transaction_item_id UUID        NOT NULL REFERENCES pos.transaction_items(item_id),
    product_id          UUID        NOT NULL REFERENCES pos.products(product_id),
    quantity            NUMERIC(8,3) NOT NULL CHECK (quantity > 0),
    refund_amount       NUMERIC(10,2) NOT NULL CHECK (refund_amount >= 0)
);

CREATE INDEX idx_returns_txn      ON pos.returns (transaction_id);
CREATE INDEX idx_returns_dt       ON pos.returns (return_dt);
CREATE INDEX idx_returns_location ON pos.returns (location_id, return_dt);
CREATE INDEX idx_return_items_ret ON pos.return_items (return_id);
CREATE INDEX idx_return_items_ti  ON pos.return_items (transaction_item_id);

-- ---------------------------------------------------------------------------
-- Phase 7: Online orders — e-commerce channel (pickup + delivery) (t_24fae529)
-- Mirrors a real online-order system as its own source schema: order header,
-- line items, and an append-only lifecycle event stream (placed→confirmed→
-- picking→ready/out_for_delivery→completed|no_show|cancelled).
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS online;

CREATE TABLE online.orders (
    order_id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number        BIGSERIAL    UNIQUE NOT NULL,
    location_id         UUID         NOT NULL REFERENCES hr.locations(location_id),
    member_id           UUID         REFERENCES pos.loyalty_members(member_id),
    placed_dt           TIMESTAMPTZ  NOT NULL,
    fulfillment_type    VARCHAR(20)  NOT NULL
                            CHECK (fulfillment_type IN ('pickup', 'delivery')),
    status              VARCHAR(30)  NOT NULL DEFAULT 'placed'
                            CHECK (status IN ('placed', 'confirmed', 'picking',
                                              'ready', 'out_for_delivery',
                                              'completed', 'no_show', 'cancelled')),
    subtotal            NUMERIC(10,2) NOT NULL,
    service_fee         NUMERIC(10,2) NOT NULL DEFAULT 0,
    tax                 NUMERIC(10,2) NOT NULL,
    total               NUMERIC(10,2) NOT NULL,
    payment_method      VARCHAR(20)  NOT NULL
                            CHECK (payment_method IN ('credit', 'debit', 'mobile_pay')),
    pickup_window_start TIMESTAMPTZ,
    pickup_window_end   TIMESTAMPTZ,
    promised_delivery_dt TIMESTAMPTZ,
    completed_dt        TIMESTAMPTZ,
    customer_count      SMALLINT,          -- party size for no-show realism
    scenario_tag        VARCHAR(50),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE online.order_items (
    item_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID        NOT NULL REFERENCES online.orders(order_id),
    product_id  UUID        NOT NULL REFERENCES pos.products(product_id),
    quantity    NUMERIC(8,3) NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(8,2) NOT NULL,
    line_total  NUMERIC(10,2) NOT NULL
);

CREATE TABLE online.order_events (
    event_id    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID         NOT NULL REFERENCES online.orders(order_id),
    event_type  VARCHAR(30)  NOT NULL
                    CHECK (event_type IN ('placed', 'confirmed', 'picking_started',
                                          'ready_for_pickup', 'driver_assigned',
                                          'out_for_delivery', 'delivered',
                                          'picked_up', 'no_show', 'cancelled',
                                          'reminder_sent')),
    event_dt    TIMESTAMPTZ  NOT NULL,
    note        VARCHAR(200),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_online_orders_placed   ON online.orders (placed_dt);
CREATE INDEX idx_online_orders_status   ON online.orders (status, fulfillment_type);
CREATE INDEX idx_online_orders_location ON online.orders (location_id, placed_dt);
CREATE INDEX idx_online_orders_member   ON online.orders (member_id);
CREATE INDEX idx_online_items_order     ON online.order_items (order_id);
CREATE INDEX idx_online_items_product   ON online.order_items (product_id);
CREATE INDEX idx_online_events_order    ON online.order_events (order_id, event_dt);

CREATE INDEX idx_hr_emp_location      ON hr.employees (location_id, status);
CREATE INDEX idx_control_stats        ON control.generation_stats (recorded_at DESC);

-- ---------------------------------------------------------------------------
-- Stockouts / lost sales (t_959cd040)
-- ---------------------------------------------------------------------------
-- One row per line a shopper asked for that the shelf could not cover. Before
-- this table a shortage was absorbed silently: depletion floored at
-- GREATEST(0, on_hand - qty) while the sale was written at FULL requested
-- quantity, so the feed showed revenue for items that were not on the shelf
-- and 16.8% of store-SKU rows sat pinned at zero with sales still running
-- against them (t_959cd040, measured on the dev DB).
--
-- The sale itself is now capped at what was on hand, so
-- pos.transaction_items.quantity is the quantity actually rung up and
-- lost_quantity is demand the store refused. lost_value is priced at the
-- price of record (the same unit_price the fulfilled line would have used),
-- which is what "lost sales" means for a safety-stock analyst.
--
-- Declared AFTER online.orders because a stockout references either a POS
-- transaction or an online order, and exactly one of the two.
CREATE TABLE inv.stockout_events (
    stockout_id        UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id         UUID         NOT NULL REFERENCES pos.products(product_id),
    location_id        UUID         NOT NULL REFERENCES hr.locations(location_id),
    channel            VARCHAR(10)  NOT NULL
                          CHECK (channel IN ('pos','online')),
    pos_transaction_id UUID         REFERENCES pos.transactions(transaction_id),
    online_order_id    UUID         REFERENCES online.orders(order_id),
    requested_quantity NUMERIC(8,3) NOT NULL CHECK (requested_quantity > 0),
    fulfilled_quantity NUMERIC(8,3) NOT NULL CHECK (fulfilled_quantity >= 0),
    lost_quantity      NUMERIC(8,3) NOT NULL CHECK (lost_quantity > 0),
    unit_price         NUMERIC(8,2) NOT NULL,
    lost_value         NUMERIC(10,2) NOT NULL,
    event_dt           TIMESTAMPTZ  NOT NULL,
    scenario_tag       VARCHAR(50),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- Exactly one sale parent, or neither. A POS line and an online line are
    -- different sales, so both can never be set. Neither is legitimate and is
    -- NOT an oversight: when a shopper's whole basket was out of stock the
    -- model writes no sale row at all (pos.transaction_items carries
    -- CHECK (quantity > 0), so an empty basket is unwritable), yet the demand
    -- is still a lost sale worth recording. Those rows carry no parent, and the
    -- walk-away is exactly the signal a safety-stock analyst wants: the basket
    -- was abandoned because the shelf was empty.
    CONSTRAINT stockout_single_parent CHECK (
        NOT (pos_transaction_id IS NOT NULL AND online_order_id IS NOT NULL)
    ),
    -- The whole point of the table: the split must reconcile to the request.
    CONSTRAINT stockout_quantities_balance CHECK (
        lost_quantity = requested_quantity - fulfilled_quantity
    )
);

CREATE INDEX idx_stockout_location_product ON inv.stockout_events (location_id, product_id, event_dt);
CREATE INDEX idx_stockout_event_dt        ON inv.stockout_events (event_dt);
CREATE INDEX idx_stockout_product         ON inv.stockout_events (product_id, event_dt);
CREATE INDEX idx_stockout_pos_txn         ON inv.stockout_events (pos_transaction_id);
CREATE INDEX idx_stockout_online_order    ON inv.stockout_events (online_order_id);

-- ---------------------------------------------------------------------------
-- Per-store-SKU daily demand ledger (t_959cd040)
-- ---------------------------------------------------------------------------
-- The replenishment input. `ordering.check_and_create_orders` used a seeded
-- reorder_qty that never moved, which was harmless while depletion floored at
-- zero and sales ran regardless of the shelf. Once a sale is capped at what is
-- on hand, a fixed reorder_qty against measured demand is what would let a
-- fast SKU sit at zero forever — the sim would degenerate into an empty shop.
-- So the generator records what was ASKED FOR, every tick, accumulated per
-- store-SKU-day:
--
--   requested_units = what shoppers wanted (POS + online)
--   fulfilled_units = what the shelves could cover
--   lost_units      = the difference; the per-tick detail is inv.stockout_events
--   lost_value      = those units priced at the price of record
--
-- One row per (location, product, day), upserted by the tick, so ordering can
-- size a reorder off real demand: lead-time demand plus the configured safety
-- fraction (inventory.restock_threshold_pct), which is what makes the seeded
-- reorder_point/reorder_qty tunable rather than decorative.
CREATE TABLE inv.sku_demand_daily (
    location_id     UUID         NOT NULL REFERENCES hr.locations(location_id),
    product_id      UUID         NOT NULL REFERENCES pos.products(product_id),
    demand_date     DATE         NOT NULL,
    requested_units NUMERIC(12,3) NOT NULL DEFAULT 0,
    fulfilled_units NUMERIC(12,3) NOT NULL DEFAULT 0,
    lost_units      NUMERIC(12,3) NOT NULL DEFAULT 0,
    lost_value      NUMERIC(14,2) NOT NULL DEFAULT 0,
    line_count      INTEGER      NOT NULL DEFAULT 0,
    last_updated    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (location_id, product_id, demand_date),
    -- The ledger must reconcile: you cannot lose more than was asked for.
    CONSTRAINT sku_demand_quantities_balance CHECK (
        requested_units >= fulfilled_units
        AND lost_units = requested_units - fulfilled_units
    ),
    CONSTRAINT sku_demand_nonneg CHECK (
        requested_units >= 0 AND fulfilled_units >= 0 AND lost_units >= 0
    )
);

CREATE INDEX idx_sku_demand_date     ON inv.sku_demand_daily (demand_date);
CREATE INDEX idx_sku_demand_product  ON inv.sku_demand_daily (product_id, demand_date);

-- ---------------------------------------------------------------------------
-- Synthetic weather — the continuous external covariate (t_2ab1fb0a)
-- ---------------------------------------------------------------------------
-- Before this table weather reached the simulation only when a human switched
-- the `severe_weather` scenario on (`scenario_engine`, the manual-only branch
-- at lines 209-211); the automatic calendar covered holidays alone. So grocery
-- demand had no weather covariate a forecasting model could regress on —
-- only a scenario_tag that was either `normal` or `severe_weather`.
--
-- ONE ROW PER STORE PER DAY, and the day's weather is a PURE FUNCTION of
-- (location, date, config) — `models/weather.py`. That purity is the same
-- requirement that made `main.daily_volume_target()` a pure function of the
-- date: the backfill replays a day an hour at a time and realtime writes it
-- 2880 times a day, so a series that redrew per call would give one date
-- different weather depending on who wrote it, and a re-seeded backfill would
-- not reproduce the series it replaced.
--
-- The series is SYNTHETIC on purpose (generator ADR: stdlib + psycopg2 +
-- pyyaml, no third-party weather client): a seasonal cosine in temperature
-- scaled by latitude, seeded synoptic fronts shared across a region, and a
-- per-store local deviation.
--
-- `demand_modifier` / `attendance_modifier` are persisted WITH the row rather
-- than recomputed at read time, so the number that scales a tick is the number
-- a downstream analyst joins on. The tick reads the row back
-- (`weather.day_effect`), it does not re-derive it.
--
-- The two loss terms in the demand law are DERIVED from the `severe_weather`
-- scenario constants (`weather.modifiers_for`), so a full-severity day
-- reproduces exactly `scenarios.severe_weather.volume_multiplier` (0.7) and
-- `attendance_modifier` (0.75). A second, independently tuned set of weather
-- coefficients would let the automatic path and the manual scenario disagree
-- about what a total storm does to the shop — the two-sources-of-truth defect
-- in a new costume.
--
-- Declared last: it references hr.locations, and nothing references it.
CREATE SCHEMA IF NOT EXISTS weather;

CREATE TABLE weather.daily (
    location_id         UUID         NOT NULL REFERENCES hr.locations(location_id),
    weather_date        DATE         NOT NULL,
    temp_high_f         NUMERIC(5,1) NOT NULL,
    temp_low_f          NUMERIC(5,1) NOT NULL,
    precipitation_in    NUMERIC(5,2) NOT NULL CHECK (precipitation_in >= 0),
    cloud_cover_pct     NUMERIC(5,1) NOT NULL CHECK (cloud_cover_pct BETWEEN 0 AND 100),
    severity_index      NUMERIC(5,3) NOT NULL CHECK (severity_index >= 0 AND severity_index <= 1),
    is_severe           BOOLEAN      NOT NULL,
    condition_code      VARCHAR(20)  NOT NULL
                           CHECK (condition_code IN ('clear', 'cloudy', 'rain',
                                                     'snow', 'severe_storm')),
    demand_modifier     NUMERIC(7,4) NOT NULL CHECK (demand_modifier > 0),
    attendance_modifier NUMERIC(7,4) NOT NULL CHECK (attendance_modifier > 0
                                                     AND attendance_modifier <= 1),
    -- The seed that produced this row. Persisted so an analyst can re-derive
    -- the series (and prove it is reproducible) without the generator's code.
    seed_key            VARCHAR(120) NOT NULL,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (location_id, weather_date),
    -- A day's low cannot sit above its high, whatever the generator drew.
    CONSTRAINT weather_temp_ordering CHECK (temp_low_f <= temp_high_f)
);

CREATE INDEX idx_weather_date     ON weather.daily (weather_date);
CREATE INDEX idx_weather_location ON weather.daily (location_id, weather_date);
