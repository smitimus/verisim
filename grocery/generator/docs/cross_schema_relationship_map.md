# Cross-schema Relationship Map & Integrity Assertions

**Ticket:** Verisim #11 — Cross-schema data connectivity & referential integrity
**Status:** API exposure ✅ · Validation harness ✅ · Map doc ✅ · Data-lab re-ingest ⏳

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
  `pos.loyalty_point_transactions` (`member_id`).
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

**Assertion inventory:** 22 hard-FK checks + 10 semantic-type checks = 32.

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

- **Data-lab re-ingest (acceptance criterion 3):** after the `load_id` API
  change, data-lab should re-ingest and confirm 0 cross-schema orphans. Tracked
  under data-lab #26–#31; no Verisim generator change is pending for this.
- **Inventory adjustments (#27):** Verisim emits **no** `inventory_adjustments`
  table/endpoint. `inv.stock_levels.quantity_on_hand` is mutated in place.
  data-lab derives `unexplained_variance` as period-end on-hand − period-start
  on-hand − receipts + shrinkage. No Verisim change required.
