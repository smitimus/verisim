# Cross-schema Relationship Map & Integrity Assertions

**Ticket:** Verisim #11 — Cross-schema data connectivity & referential integrity
**Status:** API exposure ✅ · Validation harness ✅ · Map doc ✅ · Data-lab re-ingest ⏳
**Last updated:** 2026-10-03 (t_2ffb43a0 — customer dimension, §3a)

This is the canonical map of how the grocery generator's schemas join to one
another, plus the integrity assertions that prove those joins resolve. It is the
"relationship map doc" acceptance criterion for #11 and the foundation that
Verisim #12 (scenario engine) and data-lab #26–#31 (intermediate models) build
on.

---

## 1. Single-source hubs

Three keys are the anchors of every cross-schema join. Each has exactly **one**
authoritative table; there is no per-schema divergence, so joins are clean:

| Hub key | Source table | Referenced by |
|---|---|---|
| `location_id` | `hr.locations` | POS, Timeclock, Ordering, Fulfillment, Transport, Inventory, HR schedules |
| `product_id` | `pos.products` | POS, Inventory, Ordering, Fulfillment, Pricing, Loyalty |
| `employee_id` | `hr.employees` | POS, Timeclock, Ordering, Fulfillment, Transport, Inventory, HR schedules |

The API exposes these raw keys (and joined names) on every endpoint data-lab
joins on — see §4.

---

## 2. Declared cross-schema FK graph

Postgres FK constraints enforce the *existence* of the parent for every link
below. The integrity harness (§5) re-checks these as "hard FK" assertions, and
additionally checks the *semantic type* of the parent (§3), which FKs do **not**
enforce.

- **`hr.locations`** is the hub. Referenced by: `pos.transactions`,
  `timeclock.events`, `ordering.store_orders` (`store_location_id` +
  `warehouse_location_id`), `fulfillment.orders` (`warehouse_location_id`),
  `transport.loads` (`warehouse_location_id` + `destination_location_id`),
  `inv.stock_levels`, `inv.receipts`, `inv.shrinkage_events`, `hr.schedules`.
- **`hr.employees`** referenced by: `pos.departments` (`manager_id`),
  `pos.price_history` (`changed_by`), `pos.transactions` (`employee_id`),
  `timeclock.events` (`employee_id`), `ordering.store_orders` (`created_by` +
  `approved_by`), `fulfillment.orders` (`assigned_to`), `transport.loads`
  (`driver_id`), `inv.receipts` (`received_by`), `inv.shrinkage_events`
  (`recorded_by`), `hr.schedules` (`employee_id`).
- **`pos.products`** referenced by: `pos.price_history`, `pos.coupons`,
  `pos.combo_deals` (`trigger_product_id`), `pos.transaction_items`,
  `inv.products`, `inv.stock_levels`, `inv.receipt_items`,
  `inv.shrinkage_events`, `inv.stockout_events`, `inv.sku_demand_daily`,
  `ordering.store_order_items`, `fulfillment.items`, `pricing.ad_items`.
- **`pos.loyalty_members`** referenced by: `pos.transactions` (`member_id`),
  `pos.loyalty_point_transactions` (`member_id`), and **`pos.customers`**
  (`customer_id`, inverted — the card points at its household).
- **`pos.customers`** referenced by: `pos.loyalty_members` (`customer_id`).
  This is the customer/household master dimension (see §3a). It is the *parent*
  side of the only link a grocery mart needs to segment transactions by
  household, and the join runs `transactions → loyalty_members → customers`
  rather than through a denormalised `transactions.customer_id`.
- **`pos.transactions`** referenced by: `pos.transaction_items`,
  `pos.loyalty_point_transactions`, `inv.stockout_events`
  (`pos_transaction_id`, when `channel = 'pos'`).
- **`online.orders`** referenced by: `online.order_items`,
  `online.order_events`, `inv.stockout_events` (`online_order_id`, when
  `channel = 'online'`).
- **`ordering.store_orders`** referenced by: `ordering.store_order_items`,
  `fulfillment.orders` (`store_order_id`), `transport.load_items`
  (`store_order_id`).
- **`fulfillment.orders`** referenced by: `fulfillment.items`,
  `transport.load_items` (`fulfillment_id`).
- **`transport.trucks`** referenced by: `transport.loads` (`truck_id`).
- **`transport.loads`** referenced by: `transport.load_items`,
  `inv.receipts` (`load_id`).

---

## 3. Semantic-type constraints (NOT FK-enforced — the real risk)

The FK guarantees the key *exists*, not that it has the right *kind*. These are
the highest-risk gaps and are asserted explicitly in the harness:

| Assertion | Rule |
|---|---|
| `ordering.store_orders.store_location_id` | resolves to a `store` location |
| `ordering.store_orders.warehouse_location_id` | resolves to `warehouse` / `dc` |
| `transport.loads.warehouse_location_id` | resolves to `warehouse` / `dc` |
| `transport.loads.destination_location_id` | resolves to a `store` location |
| `fulfillment.orders.warehouse_location_id` | resolves to `warehouse` / `dc` |
| `transport.loads.driver_id` | employee with `department = 'transport'` at a `warehouse` location |
| `fulfillment.orders.assigned_to` | employee with `department = 'warehouse'` at a `warehouse` location |
| `ordering.store_orders.created_by` / `approved_by` | employee with `department = 'management'` |

### 3a. The customer dimension (`pos.customers`)

`pos.loyalty_members` is a **card**. `pos.transactions.member_id` resolves to
it or is NULL, and nothing on either side carried a demographic or household
attribute — so the marts a grocery warehouse actually wants (RFM cohorts,
basket affinity, segment penetration, household-size vs basket size) had no
conformed dimension to join to. `pos.customers` is that dimension: one row per
**household**, with `age_band`, `household_size` and `segment`.

The link is `pos.loyalty_members.customer_id`, **nullable and NOT unique**:

| Fact | Why |
|---|---|
| Nullable | a card predating the dimension, or one written mid-boot before its household exists, is legitimately NULL for an instant |
| Not unique | a household holds several cards in the real world (two adults, a card each). A `UNIQUE` here would make `household_size > loyalty_member_count` unrepresentable |

`household_size` is the household's **size**, not its card count, so it is
legitimately larger: the children and students in the household never signed
up. Both directions of that relationship are cross-table and therefore not
covered by any FK or CHECK, so the harness asserts them explicitly.

**Deliberately NOT columns** (both are a `LEFT JOIN` from
`pos.loyalty_members`, and a stored copy would go stale the moment a second
card joined the household): `loyalty_member_count`, `first_signup_date`. The API
computes them in the query.

**There is no `pos.transactions.customer_id`.** Denormalising the snowflake into
the fact table would re-attribute every anonymous shopper — a behavioural change
far beyond adding a dimension — and would make the dimension the same size as
the fact table it was supposed to describe. The mart joins
`transactions → loyalty_members → customers`.

Two further properties the harness pins because the generator, not the
database, is what makes them true:

* **Attributes are drawn conditionally on the segment.** Every segment carries
  its own age-band and household-size distribution (`SEGMENTS` in
  `models/customers.py`), so a `family_stock_up` household skews 35–44 and 4–6
  people. Three independent draws would give every segment identical behaviour
  and the dimension would be decoration.
* **The draw is deterministic per household**, seeded from the household's
  first `member_id`. A restart or a re-seed reproduces the same profile; a
  segment that shifts between loads cannot support a cohort at all.

| Assertion | Rule |
|---|---|
| `HARD-29` | `pos.loyalty_members.customer_id` resolves to a `pos.customers` row |
| `SEMA-14` | a household's `household_size` is **not less than** the number of loyalty cards it holds |
| `SEMA-15` | every loyalty card resolves to a household — a dimension with holes is not a dimension |

Verified against a real postgres (400 seeded cards): **289 households**, 82 of
them holding more than one card, mean `household_size` 2.57, none over the cap,
all six segments present, and all three assertions returning zero rows. A
second backfill creates nothing; re-planning the same cards reproduces the
dimension exactly.

**Migration note.** A `schema.sql` change only reaches a *fresh* bootstrap, so
`models/customers.py` carries an `IF NOT EXISTS` copy of the DDL and an
`ALTER TABLE … ADD COLUMN` for the FK, run from `seed_all`. On the standalone
image that ALTER is **refused**: `entrypoint.sh` applies `schema.sql` through
`su … postgres -c "$PSQL -f"`, so every table is owned by `postgres` while the
generator connects as `$POSTGRES_USER` with GRANT ALL and no ownership.
Verified on CT106 2026-10-03, where `ALTER TABLE pos.loyalty_members ADD COLUMN`
raises `InsufficientPrivilege` — and `IF NOT EXISTS` does **not** rescue it,
because Postgres checks ownership before noticing there is nothing to do. So
the generator probes the column first and only ALTERs when it is genuinely
missing; when the ALTER is refused it logs, degrades to a no-op, and leaves the
supported fix (re-bootstrap the data dir from a current image) in the message.
The API serves an empty result with `customers_dimension_present: false` on such
a data dir rather than 500ing.

### Stockouts: the sales feed must not outrun the shelf (t_959cd040)

Before t_959cd040, depletion floored at `GREATEST(0, on_hand - qty)` while the
sale was written at the **full** requested quantity. The shortfall was absorbed
silently, so the feed booked revenue for stock the store did not have — measured
on the dev EDW as 249 of 1485 store-SKU rows (16.8%) pinned at zero with ~600k
transactions still selling against them. `reorder_point`, `reorder_qty` and
`restock_threshold_pct` were therefore decorative: a shortage could not be
observed, so nothing could respond to one.

The invariant now enforced, all of it cross-table and therefore **not** covered
by any FK or CHECK:

| Assertion | Rule |
|---|---|
| `SEMA-11` | `inv.stockout_events.channel` agrees with which parent is set — `pos` ⇔ `pos_transaction_id`, `online` ⇔ `online_order_id` |
| `SEMA-12` | a POS stockout's parent line exists at exactly `fulfilled_quantity` — the till never rang up more than it could hand over |
| `SEMA-13` | `inv.sku_demand_daily` reconciles: `requested_units = fulfilled_units + lost_units` |

The per-line facts live in `inv.stockout_events` (one row per short line, priced
at the price of record) and the per-store-SKU-day running total in
`inv.sku_demand_daily`, which is what `ordering.check_and_create_orders` sizes a
reorder from — lead-time demand plus `restock_threshold_pct` safety, clamped to
`reorder_qty_max_multiple` × the seeded `reorder_qty`.

Both channels resolve against ONE `inventory.StockAllowance` per tick, because
POS and online draw on the same shelf; a line the shelf cannot cover at all is
dropped rather than written at zero (`pos.transaction_items` and
`online.order_items` both `CHECK (quantity > 0)`), and the shopper's lost
demand is recorded instead.

### Promotion validity windows (temporal — also NOT FK-enforced)

The FK guarantees `pos.transaction_items.coupon_id` / `deal_id` resolves to a
promo row, not that the promo was *valid* on that day. A promotion may only be
applied inside its own window, so every attributed line item must satisfy:

    transaction_dt::date BETWEEN valid_from AND valid_until

Enforced at the source in `models/pos.py`:

* `_promo_applies_on()` — application-time guard; a promo is never tagged onto
  a transaction dated outside its window (the backfill fetches promos against
  *today*, so without this a back-dated transaction inherits a window that
  opens later — 24k coupon / 7k deal items on the dev EDW, verisim
  t_01b4fe4f);
* seeding (`seed_named_coupons` / `seed_coupons` / `seed_combo_deals`) back-dates
  `valid_from` over `generator.backfill_lookback_days`, so a promo created at
  the end of the horizon covers the history that references it;
* `reconcile_promotions()` (startup, each backfill day-end, end of backfill, and
  each simulated midnight) widens any window that still does not cover recorded
  usage, and derives `pos.coupons.uses_count` from the redemptions on disk
  (one per coupon-tagged transaction).

Asserted by harness checks `TIME-01` / `TIME-02`; data-lab mirrors them with
`assert_coupon_dates_valid` / `assert_deal_dates_valid`.

### Generation-order guarantee

`grocery/generator/main.py::run_tick` runs the supply-chain block
(POS → timeclock → store orders → fulfillment → truck dispatch → delivery
receipts → shrinkage → promotions → scheduling) **once per simulated day, at the
hour-0 midnight boundary**. Parents therefore always exist before their children
within a completed day.

**Partial-day tolerance.** A backfill day that is still in progress (today) only
runs up to the current hour, so downstream rows may legitimately be absent. The
harness scopes every time-bounded assertion to *completed* days
(`<column>::date < CURRENT_DATE`), so an in-flight day never produces a false
orphan.

---

## 4. API exposure — endpoint → join keys (data-lab #26–#31)

All keys data-lab's intermediate models join on are exposed by the Verisim API.
No further serializer changes are required for #26–#31.

| data-lab join chain | Endpoint(s) | Linking keys |
|---|---|---|
| POS → HR | `pos/transactions` | `location_id`, `employee_id`, `member_id` |
| HR → Timeclock → HR | `grocery/timeclock/events` | `employee_id`, `location_id` |
| Ordering → HR | `grocery/ordering/orders` | `store_location_id`, `warehouse_location_id`, `created_by`, `approved_by` |
| Fulfillment → Ordering, HR | `grocery/fulfillment/orders` | `store_order_id`, `warehouse_location_id`, `assigned_to` |
| Transport → Fulfillment/HR/Ordering | `grocery/transport/loads`, `grocery/transport/load-items` | `truck_id`, `driver_id`, `warehouse_location_id`, `destination_location_id`, `load_id`, `fulfillment_id`, `store_order_id` |
| Inventory ↔ POS | `inventory/stock-levels`, `inventory/products` | `product_id`, `location_id` |
| Inventory receipts ↔ Transport | `inventory/receipts` | `location_id`, `load_id` |
| Inventory shrinkage | `grocery/inventory/shrinkage-events` | `product_id`, `location_id`, `reason`, `recorded_at` |
| POS → Customers (household dimension) | `grocery/pos/loyalty-members`, `grocery/pos/customers` | `member_id` → `customer_id`; plus `segment`, `age_band`, `household_size` |
| Customers → summary / sizing | `grocery/pos/customers/summary` | `segment`, `age_band` (grouping grain) |

> **`customer_id` on the loyalty route (t_2ffb43a0):** `/grocery/pos/loyalty-members`
> now serves `customer_id` (and accepts `?customer_id=`), which completes the
> mart join chain `pos.transactions → loyalty_members → customers`. Without it
> the dimension existed but nothing served the key that reaches it. The column
> is absent on a data dir predating the card, so the route probes for it and
> omits it from the projection rather than 500ing; `/grocery/pos/customers`
> returns `{"data": [], "customers_dimension_present": false}` in that case.

> **API gap for t_959cd040:** `inv.stockout_events` and `inv.sku_demand_daily`
> are **not yet exposed** by the API. data-lab cannot read the lost-sales feed
> or the per-store-SKU-day demand ledger until a
> `grocery/inventory/stockout-events` endpoint (and ideally
> `.../sku-demand-daily`) lands. Tracked on the companion data-lab ticket
> referenced by this card; the generator-side tables and their integrity
> assertions are in place.

> **API change made for #11:** `inventory/receipts` now returns `load_id`
> (the receipt→transport.load FK), so data-lab can join
> `inv.receipts → transport.loads` without a second round-trip. Committed in
> verisim `2cb9bed`.

---

## 5. Integrity harness

**Location:** `grocery/generator/tests/`

- `cross_schema_integrity.py` — the single source of truth: a list of
  `AssertionSpec` objects (id, dimension, title, SQL) plus `run_all()` /
  `summarize()` and a CLI entry point.
- `test_cross_schema_integrity.py` — pytest runner:
  - **DB-free contract tests** (always run, no DB): every assertion is
    well-formed; the committed `.sql` spec documents every assertion id.
  - **Live assertions** — parametrized over all specs, run against a real
    grocery DB. Skipped automatically when no DB is reachable (set
    `GROCERY_TEST_DB` to enable).
- `sql/check_cross_schema_integrity.sql` — generated, human-readable copy of all
  assertions for manual `psql` runs / DBAs. Regenerate with
  `python -m grocery.generator.tests.cross_schema_integrity <dsn>` (no, that runs
  them — regenerate via the module's generator snippet, or just re-run the
  pytest which reuses the module).

**Assertion inventory:** 29 hard-FK checks + 15 semantic-type checks + 2
temporal = 46 (t_2ffb43a0 added `HARD-29`, `SEMA-14`, `SEMA-15`).

**Run after a fresh backfill:**

```bash
# Full suite (DB-free contract tests always run; live tests need a DB)
GROCERY_TEST_DB=postgresql://verisim:verisim@127.0.0.1:5499/grocery \
    python -m pytest grocery/generator/tests/test_cross_schema_integrity.py

# Or just the live checks against a database
python -m grocery.generator.tests.cross_schema_integrity \
    postgresql://verisim:verisim@127.0.0.1:5499/grocery
```

Exit code is non-zero if any assertion returns orphan rows.

---

## 6. Open items

- **Pre-existing, NOT introduced by t_2ffb43a0:** the "add a column to an
  existing data dir" migration this codebase relies on **cannot run on an
  existing data dir**, because the generator's role does not own the tables
  `entrypoint.sh` created (see §3a). Measured on CT106 2026-10-03:
  `elasticity.seed_elasticity_columns` (t_08deeddf) issues an unguarded
  `ALTER TABLE pos.products ADD COLUMN reference_price` on every boot, and that
  column is **still absent** from a data dir holding 525,704 transactions — so
  the price→demand elasticity loop is not actually reading elasticity columns
  there, and the `mart_product_price_elasticity` regression it exists to make
  measurable is measuring something else. t_2ffb43a0 guards its own ALTER
  (probe first, then attempt, then degrade to a no-op with the fix in the log);
  `elasticity.py` still does not, so it raises on every boot of an old data dir.
  **Fix:** either apply `schema.sql` as `$POSTGRES_USER` in `entrypoint.sh`, or
  `ALTER TABLE … OWNER TO $POSTGRES_USER` for every table after applying it.
  Until then, no card that migrates an existing data dir can rely on that path.
- **Data-lab re-ingest (acceptance criterion 3):** after the `load_id` API
  change, data-lab should re-ingest and confirm 0 cross-schema orphans. Tracked
  under data-lab #26–#31; no Verisim generator change is pending for this.
- **Inventory adjustments (#27):** Verisim emits **no** `inventory_adjustments`
  table/endpoint. `inv.stock_levels.quantity_on_hand` is mutated in place.
  data-lab derives `unexplained_variance` as period-end on-hand − period-start
  on-hand − receipts + shrinkage. No Verisim change required.
