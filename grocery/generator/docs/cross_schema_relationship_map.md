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
- **`inv.suppliers`** (new hub, t_57b1a1ab) referenced by: `inv.products`
  (`supplier_id`), `inv.receipts` (`supplier_id`),
  `inv.supplier_delivery_schedules` (`supplier_id`),
  `inv.short_ship_events` (`supplier_id`),
  `inv.supplier_credit_memos` (`supplier_id`), `inv.dsd_deliveries`
  (`supplier_id`).
- **`inv.supplier_delivery_schedules`** referenced by: `inv.dsd_deliveries`
  (`schedule_id`) — the vendor-window a delivery's on-time rate is measured
  against (`SEMA-26` asserts a delivery lands on a weekday the schedule visits).
- **`inv.short_ship_events`** referenced by: `inv.supplier_credit_memos`
  (`short_ship_id`, UNIQUE — one claim per shortfall, which is what makes
  `open_credit_memo` idempotent). Its own two parents — `fulfillment_item_id`
  and `fulfillment_id` — are **nullable and conditional on
  `detected_source`** (t_d74d67a3): a `receiving` short came off one of our
  fulfillment lines and must carry both; a `dsd_delivery` short came off the
  vendor's own truck, has no fulfillment behind it, and must carry neither.
  `short_ship_requires_its_parents` is the CHECK, restated as harness assertion
  `SEMA-28`, and `HARD-32` (the item FK anti-join) skips the NULL rows — a
  consumer joining either column must expect NULLs on the DSD half.
- **`inv.dsd_deliveries`** referenced by: `inv.dsd_delivery_items`
  (`dsd_delivery_id`). Its `scheduled_window` is the **promise the drop is
  measured against**, copied from its schedule's `delivery_window_start` at
  write time (t_120b25b0) — a delivery and a schedule together are what answer
  "did the truck arrive inside the window it promised". The column is nullable
  and NULL means "no window recorded", not "arrived outside every window";
  `SEMA-29` asserts that a window which IS recorded is the one the schedule
  names.

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

### Vendors, short-ships and credit memos (t_57b1a1ab)

Before this card a supplier was a free-text `supplier_name VARCHAR(200)` chosen
by a module-level literal inside `models/inventory.py`, with no id to join on and
no behaviour attached — so "which vendor keeps shorting us" was not merely
unbuilt, it was unbuildable. `inv.products.lead_time_days` was a single static
integer per product, so a vendor had no *promise* and therefore no arrival could
be late against it.

The short-ship side was worse than missing: `fulfillment.items` did record
`pick_status = 'short'`, and `transport.receive_delivered_loads` then filtered
`WHERE fi.pick_status = 'picked'` — so the short lines were excluded from the
receipt and went nowhere. A vendor who could not fill an order left no trace.

**`inv.stockout_events` and `inv.short_ship_events` are different events.** The
first is what the **shop** could not sell because the shelf was empty; the second
is what the **vendor** could not deliver. Different owner, different remedy
(reorder vs credit claim), different clock. A mart that joins them measures
vendor fill rate against shelf availability and blames the vendor for our own
stockouts.

| Assertion | Rule |
|---|---|
| `SEMA-14` | a short-ship's `supplier_id` is the vendor that supplies that **product** (`inv.products.supplier_id`) — the vendor is derived from the goods, not from the load |
| `SEMA-15` | a short-ship's `quantity_requested` / `quantity_picked` mirror the `fulfillment.items` line it came from |
| `SEMA-16` | `detected_source` matches the vendor's `fulfillment_model` — a DSD vendor's short is found at the shelf (`dsd_delivery`), a warehouse vendor's at the dock (`receiving`) |
| `SEMA-17` | a credit memo exists only against a short-ship flagged `is_creditable` — the vendor's terms decide, not the model's convenience |
| `SEMA-18` | a memo's vendor, product and location all match the short-ship's |
| `SEMA-19` | a memo's `credit_quantity` / `credit_amount` are the shortfall and its value — the claim is the shortfall priced at the same unit cost the receipt used for the goods that did arrive |
| `SEMA-20` | `submitted_dt` is on or before `claim_deadline`; a late claim must be recorded `expired` instead |
| `SEMA-21` | a resolved claim resolves **after** it was submitted |
| `SEMA-22` | a short-ship is never recorded against a line the warehouse fully picked |
| `SEMA-23` | `realized_lead_time_days` is never negative (a gaussian can be, a lead time cannot) |
| `SEMA-24` | a DSD delivery's `total_units` / `total_value` / `line_count` reconcile with its item lines |
| `SEMA-25` | a DSD delivery's supplier matches its schedule's supplier |
| `SEMA-26` | a DSD delivery lands on a weekday its schedule actually visits |
| `SEMA-27` | `inv.products.supplier_name` agrees with `inv.suppliers.supplier_name` — the denormalised display copy can never disagree with the row it came from |
| `SEMA-29` | a DSD delivery's recorded `scheduled_window` is the window its schedule promised (a recorded window cannot contradict the schedule; NULL reads as "no window recorded") |
| `SEMA-30` | a store order never carries a SKU a DSD vendor delivers direct — the upstream half of `SEMA-16`, checked before the goods reach a dock |
| `SEMA-31` | a rejected claim carries the text explaining why the vendor disputed it — `rejection_reason` is the only explanation the analyst deciding whether to escalate ever gets |

**A rejection must say why (t_ec78f608).** `REASON_PAY_PROFILE` pairs each
`short_reason` with `(pay_probability, rejection_text)`, and
`advance_credit_memos` writes that second element straight into
`rejection_reason` on the reject branch. Three of the five profiles carried
`None` — `warehouse_shortage`, `out_of_stock`, `weather_carrier_delay` — so 8%,
12% and 30% of those claims rejected with a NULL explanation: 60 of 119
rejections on the CT107 reseed, and data-lab's
`assert_credit_memo_timeline_is_coherent` counted every one. The pairing is now
enforced at three levels, because each catches what the others cannot:
`suppliers._validate_reason_pay_profile` at import (a disputable profile with no
text stops the generator from starting, rather than writing NULL at midnight),
`credit_rejected_requires_reason` in `schema.sql` on the row (blank text is as
useless as NULL), and `SEMA-31` across a seed. The reverse pairing is a rule
too: an always-paid profile must NOT carry the text, or the row reads as both
paid and disputed next to `memo_status` in an aging mart.
`grocery/standalone/migrations/002_credit_memo_rejection_reason.sql` repairs the
rows already written that way — derived from each row's own `short_reason`, a
closed set — before it adds the CHECK, so an existing data dir converges on one
boot instead of crash-looping on a constraint it cannot satisfy.

**One shelf, ONE replenishment channel (t_d59b551f).** A SKU's shelf is restocked by
exactly one channel, decided by its vendor's `fulfillment_model`: a `warehouse` vendor
(and an unassigned SKU) through `ordering` → `fulfillment` → `transport` →
`inv.receipts`, a `dsd` vendor through `suppliers.generate_dsd_deliveries`. So
`ordering.check_and_create_orders` skips a DSD vendor's SKUs, and the vendor's own drop
is their only replenishment — which is why the drop restocks the store's lines that are
below their reorder point (`dsd_visit_quantity` sizes each line for the gap until the
next visit, `7 / visits_per_week` days of measured demand) instead of a random 2–6 line
slice of the catalogue.

Before the split both channels fed the same shelf: a DSD vendor's perishables were
ordered from our warehouse, rode one of our trucks, and a pick short at the dock was
recorded `detected_source = 'receiving'` against a vendor whose own row says
direct-store-delivery only. That is `SEMA-16` red on every fresh seed (20–37 rows), and
a fill-rate/on-time mart attributing one vendor's failures to two channels, one of which
it does not use. `SEMA-30` is the assertion that keeps the split honest, and
`ordering.check_and_create_orders` is the only place it can be enforced because that is
the only place a warehouse order is raised.

Two data-lab consequences, both intentional: `ordering.store_order_items` no longer
carries a DSD vendor's SKUs at all, and a DSD drop that finds nothing below its reorder
point writes **no** `inv.dsd_deliveries` row (an empty delivery is what data-lab's
`assert_dsd_delivery_reconciles_with_items` reports as an `empty_delivery` anomaly, so a
visit with a full shelf is not a delivery).

`HARD-29`..`HARD-39` cover the new foreign keys themselves, plus the one that is
also a type check: a DSD delivery's supplier must actually be
`fulfillment_model = 'dsd'`.

**A memo that never resolves is a finding, not a missing value.** `memo_status`
is a lifecycle — `open` → `submitted` → `paid` | `rejected`, with `expired` when
the vendor's claim window closed first and `written_off` when the store decided
the paperwork was not worth it — because "how long did this vendor take to pay,
and did it ever" is the question a claims-aging mart exists to answer. The
`CHECK`s enforce that a terminal status carries its timestamp, that a claim was
filed inside the window, and that an `expired` / `written_off` memo never also
claims a vendor resolution.

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

### Discount money lives on the line (t_0a72c6a5)

The promotion *reference* (`coupon_id` / `deal_id`) and the promotion *money*
used to live on different tables: the header carried `coupon_savings` and
`deal_savings`, the lines carried the ids, and `transaction_items.discount` was
0 on every one of 7.67 M rows — so `mart_product_performance.total_discounts`
and three other data-lab marts published a structural zero that read as "there
were no promotions".

The contract now is:

    transaction_items.discount   = the DOLLAR amount discounted off that LINE
    transaction_items.line_total = quantity * unit_price - discount
    transactions.subtotal        = sum(line_total) + coupon_savings + deal_savings
    transactions.total           = subtotal + tax - coupon_savings - deal_savings

Three things worth knowing:

* **`discount` is a line amount, not a per-unit amount.** `unit_price` stays the
  price of record (gross) and the line's money is `discount`, so a weighed line
  (0.2 kg … 3.5 kg) and a 3-unit line carry a figure that actually sums to the
  transaction's savings. Summing `discount` over lines is therefore a real
  dollar measure, which is what the marts do.
* **`subtotal` did not change meaning.** It is still the line net *before* the
  transaction-level coupon/deal, which is why data-lab's
  `total = subtotal + tax - coupon - deal` is untouched. The consequence is
  `sum(line_total) = subtotal - coupon_savings - deal_savings`, and the gross is
  `sum(quantity * unit_price)`.
* **A promotion only applies when a line can carry it.** The saving is computed
  against the promo's own scope (a "10% off produce" coupon discounts the produce
  lines, not the basket) and is then spread pro-rata over the room each scoped
  line has left, in integer cents with a largest-remainder pass, so
  `sum(discount)` equals the header to the cent and no line goes negative. A
  redemption that reaches no line is not booked at all: a coupon whose scope is
  not in the basket does not appear on the header, and a line never carries a
  promo id with a zero discount. `bogo` coupons have no line amount and are not
  applied.

Line arithmetic uses `Decimal` with `ROUND_HALF_UP`, because a weighed line can
land exactly on a half cent (2.06 lb x $7.75 = $15.965): Postgres rounds that up
and Python's float `round()` rounds it down, which left 39 lines of a 10-day
backfill 1 ¢ out of balance.

Asserted by harness checks `SEMA-32` (the line balances and a tagged line carries
money) and `SEMA-33` (the header and its lines agree to the cent per
transaction). data-lab's staging model derives `line_total` from
`quantity * unit_price - discount` — it used to recompute
`(unit_price - discount) * quantity`, i.e. the old per-unit reading — so the two
repos have to move together.

### Loyalty points pay out, and a coupon has a usage cap (t_74280120)

Two source columns existed, were read by marts whose whole purpose they are, and
were never written:

* **`pos.loyalty_point_transactions.points_redeemed`.** The ledger only ever
  credited — 534,250 `purchase` / `tier_upgrade` / `bonus` rows, zero
  `redemption` rows, `sum(points_redeemed) = 0` — so
  `int_loyalty_enriched.points_redeemed`,
  `mart_loyalty_rfm.lifetime_points_redeemed` and
  `mart_loyalty_engagement.redemption_rate_pct` published a structural zero.
  `_record_loyalty_points` now draws a redemption for
  `loyalty.redemption_rate` (default 3 %) of member tickets, scaled by the
  `redemption_rush` scenario's `loyalty_redemption_modifier` for A/B runs.
  The amount is a whole multiple of 100 points (100 points = $1 of basket) and
  is capped by `min(balance at that instant, ticket value,
  loyalty.redemption_points_max)`, so `balance_after` can never go negative and
  a ticket can never pay out more than it is worth.
  The redemption row is written **before** the earn row for the same ticket:
  data-lab's `assert_loyalty_points_balance` orders same-instant rows by
  `balance_after`, and in the other order the redeem row lands lower and reads
  as a chain break. A redemption never demotes a tier.
* **`pos.coupons.max_uses`.** It is the denominator of
  `mart_promotion_redemption.redemption_rate_pct`
  (`uses_count / max_uses`) and was NULL on 100 % of coupons, so that rate was
  uncomputable even where `uses_count` was real. Seeding now writes a cap
  (named coupons from their own catalogue terms, generated coupons from
  `coupons.max_uses`), and `backfill_coupon_max_uses()` repairs every legacy
  NULL row in place — it runs at the top of `seed_coupons`, i.e. at boot and
  every simulated midnight, because a reseed alone never touches rows that
  already exist.

Neither change alters the DDL: `max_uses` and `points_redeemed` were already
there, they simply had no writer.

### Generation-order guarantee

`grocery/generator/main.py::run_tick` runs the supply-chain block
(ordering → store orders → fulfillment → truck dispatch → delivery receipts)
with **ordering once per simulated day, at the hour-0 midnight boundary**, and
**the warehouse working every tick**:

* `ordering.check_and_create_orders` — daily (one replenishment cycle per
  simulated day);
* `fulfillment.process_pending_orders` → `transport.dispatch_loads` →
  `transport.receive_delivered_loads` — every tick (t_5a408514). A realtime
  tick is an *instant*, and a pick that comes due at 03:20 has to be recorded at
  03:20; while these ran inside the midnight block the next pass that could
  advance a row was 24 h away, which put a day of dwell on every
  replenishment lead. In the backfill they still run once per simulated day,
  in the end-of-day pass, with `day_wide_pass=True`.

Parents therefore always exist before their children within a completed day.

**Partial-day tolerance.** A backfill day that is still in progress (today) only
runs up to the current hour, so downstream rows may legitimately be absent. The
harness scopes every time-bounded assertion to *completed* days
(`<column>::date < CURRENT_DATE`), so an in-flight day never produces a false
orphan.

**Today is shared with the realtime loop, so a restart may not rewrite it
(t_9951b23c).** The boot gap scan and `run_backfill` used to answer "is today
complete?" the past-day way — does its last transaction reach hour 23? — which
is never true for a day in progress, so every mid-day container restart replayed
the day from hour 0 on top of the rows realtime had already written. On dev/106
through 2026-11-15 two restarts (15:43 and 19:19) put the day's per-hour counts
at ~3x the prior day's (hour 10: 1,507 vs 440) and drained the shelf
(`Stockout: 3416 line(s) short — 4742.7 units`). Both writers now ask one
function, `main.todays_unwritten_hours()`:

* a day with no rows at all (fresh seed, data dir restored over night) is the
  backfill's — hours 0 → the hour in progress, the last one stamped at `now` so
  realtime continues seamlessly;
* a day that already holds rows only gets the **whole hours after its newest
  row and before the hour in progress**. The hour in progress belongs to
  realtime: it writes it 120 times an hour from a per-tick volume law and cannot
  see how much of that hour a backfill has already rung, so a backfill that
  wrote it would double it. An hour *below* the newest row was written by
  realtime however sparse it looks — its empty minutes are sampling, not
  missing writing.

The same rule as a past day, i.e. `main.plan_day_write()` is the single place a
day's write plan is decided (`start_hour`, `end_hour`, skip, punch-repair-only).
The one limitation inherited from the past-day path: a hole in the *middle* of
the day (realtime rows at hours 0–9, an outage, rows again at 14+) is not
noticed, because the resume point is the newest row. It leaves an under-volume
day, never a doubled one.

### Fulfillment lifecycle and its two clocks (t_5a408514)

`fulfillment.orders` carries a real state machine, advanced by the passes above:

```
received ──pick starts──▶ picking ──pick+pack done──▶ packed ──load departs──▶ shipped ──load arrives──▶ delivered
```

* `created_at` is the instant the **warehouse received the order** — the store
  order's `approved_dt`, i.e. a *business* clock, not the INSERT wall clock.
  That is what `mart_fulfillment_operations.order_received_at` and
  `hours_to_pick_start` already assume. Nothing watermarks on this column:
  data-lab loads `fulfillment.orders` with a FULL refresh, and the API
  paginates on it with a primary-key tiebreak. (The force-backfill clear in
  `base/api/main.py` keys on it too — so a forced backfill of a past window now
  clears these rows, which it never did while `created_at` was the wall clock.)
* `started_at` / `completed_at` are the pick start and the end of pick+pack.
  `hours_to_fulfill = completed_at - started_at`,
  `hours_to_pick_start = started_at - created_at`; both are non-negative by
  construction, and `hours_to_fulfill` is capped at `MAX_PICK_MINUTES` (4 h) —
  one warehouse shift, not a day and a half.
* **No instant is ever stamped after `sim_dt`.** A business timestamp in the
  future is what data-lab's `column_profile.py` fails (`future_dates`,
  tolerance 0.001), so a row whose pick has not finished stays `picking` with
  `completed_at` NULL, and one whose pick has not started stays `received` with
  both NULL.
* The per-row schedule (how long the order waited, how long the pick takes,
  whether it is one of the ~10 % queued orders that miss the day's truck) is
  **derived from the row's own `fulfillment_id`**, not stored: every pass, and
  every process restart, recomputes the same plan without a plan column and
  without writing a future timestamp.
* `transport.dispatch_loads` moves a row to `shipped` (and its store order with
  it); `transport.receive_delivered_loads` moves it to `delivered`. A `packed`
  row with no load behind it is swept onto the next pass's truck — the sweep is
  why a missed truck delays an order by one pass instead of orphaning it.
* `pending` / `loaded` / `cancelled` are still accepted by the CHECK: rows
  written by earlier images carry them and must keep loading.

### The online order clock (t_c9ea180f)

`online.orders.completed_dt` is a **clock**, not a sampled duration: it is the
`sim_dt` of the pass that completed the order. That makes
`placed_dt <= completed_dt` a physical invariant, and it was broken — 988 of
510,405 completed orders on dev, worst **-20 h 46 m**, 2 of them already in the
EDW. Every downstream duration / SLA / on-time metric reads negative or
nonsense for those rows, and nothing rejected them.

The cause was a replay, not a distribution. `advance_online_lifecycle` selected
every open order with **no bound on `placed_dt`**; `placed`/`confirmed`/`picking`
are age-gated and were safe, but `ready` and `out_for_delivery` completed on a
bare dice roll. A backfill replaying hour 0 of a day whose later hours were
already written — a restart, or `today`'s partial-day replay, which always
starts at hour 0 — therefore stamped `completed_dt = 00:00` on orders placed at
11:00–13:00. The signature in the source is unmissable: hour-boundary
`completed_dt`, same date, `updated_at` ~11 minutes after `created_at`.

Three layers now hold it:

| Layer | Where | What it does |
|---|---|---|
| selection | `models/online.advance_online_lifecycle` | `AND placed_dt <= :sim_dt` — a pass can only move rows that already exist on its own clock |
| row guard | same function | `sim_dt < placed` skips and logs, so a future caller with a bad clock cannot poison the batch |
| DDL | `schema.sql` + `migrations/005_online_order_clock.sql` | `online_orders_completed_after_placed CHECK (completed_dt IS NULL OR completed_dt >= placed_dt)` |

The long tail extends **forward**. A delivery completes no earlier than
`promised_delivery_dt - delivery_early_grace_minutes` (default 30 min) and a
pickup no earlier than `pickup_window_start - pickup_early_grace_minutes`
(15 min) — both `config.yaml` knobs, so "when may a customer be told their order
is ready" is tunable without a code change. Before this, deliveries completed
~2.5 h after placement against a 4–6 h promise (3,577 of 4,048 early against the
promise, 1,166 more than 4 h early) and 1,797 of 3,093 pickups were collected
before their window opened — an on-time metric that is ~100 % on-time by
construction is not a metric.

Assertions: `TIME-03` in the integrity sweep (any slot, no boot migration
needed) and `tests/test_online_order_clock.py` (the invariant over a generated
day, both promise gates, the CHECK, and the migration against a pre-card table).
The `data-lab` raw copy needs no transformation — this is a source-clock fix,
but its `assert_online_orders_reconcile` is the natural place for the same
predicate on the EDW side.

### ...and the band has a ceiling (t_e1278564)

`completed_dt = sim_dt` is only a completion while the pass that sees the row is
near the row's own time. The test slot's timeline had a **34-day hole** (the
source retention window, partly re-created by a backfill), so the first pass over
a 2026-10-08 23:00 order was 2026-11-15 00:00 — and that became its completion:
**37 days 02 h**, 56 rows over 24 h, and the only reason
`mart_online_fulfillment`'s two duration bounds (`avg_hours_to_fulfil`,
`avg_minutes_late`) were red. A contiguous 10-day backfill never does this
(longest span 17 h), so the class is the clock jump, not a duration
distribution.

`online.max_fulfillment_hours` (24 h, 4× the longest promise) closes the other
end:

| Layer | Where | What it does |
|---|---|---|
| the law | `models/online._lifecycle_instant` | inside `[placed_dt, placed_dt + ceiling]` the pass clock is used unchanged; past it every instant this pass writes for that row is derived from the order (`_late_instant`, seeded on `order_id`) inside `[floor, placed_dt + ceiling]` |
| events | same function | the terminal event is stamped with the completion it records, and no stage of a recovered order is written after it |
| repair | `migrations/006_online_order_ceiling.sql` | clamps rows an earlier image wrote and pulls their events back with the completion; idempotent, no CHECK (the ceiling is a config knob, so a table constraint could not read it) |
| assertion | `TIME-05` | `completed_dt <= placed_dt + 24 h`, plus `tests/test_online_order_clock.py` (a 37-day gap, a generated day followed by a 34-day gap, and the migration against a pre-card table) |

The generator logs one warning per pass that finds orders past their window, so
a gap in the timeline is visible in `docker logs` instead of only in the data.
The `data-lab` side (t_114dc03f) owns the wide-window re-ingest — `online.orders`
loads incrementally on `placed_dt`, so the repaired rows sit behind the
watermark.


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

> **`/inventory/receipts` is paged AND counted (t_2ab8e903).** It used to be the one
> receipt-shaped route a client could not page: `limit` capped at 1000, no `offset`,
> and a **bare JSON array** with no `total`, so a window holding 6,804 receipts and
> its first 1,000 rows were the same answer — data-lab landed that page as the whole
> 32-day window and orphaned ~297,000 `receipt_items` on the `receipt_items →
> receipts` edge on every nightly load (data-lab t_6b215e70, where the loader now
> proves a count-less route complete from a short page). It now returns the
> `{data, total, limit, offset}` envelope its siblings use, and `total` counts
> **receipts**, not joined rows: the row query LEFT JOINs `inv.receipt_items` to
> compute `line_items` and groups by `r.receipt_id`, so the count takes no join at
> all. The `ORDER BY` also grew the primary-key tiebreaker it never had
> (`received_dt DESC, receipt_id DESC`) — the route was missed by t_d7892e10 because
> it had no `offset` to page with, and its sort key is the least unique of the
> family: on the test data dir all 7,232 receipts share their `received_dt` with at
> least one other across 85 distinct values (one day holds 711), and paging it at
> `limit=1000` was measured returning 191 rows twice and 191 never while the count
> still matched. Consumers that expected a bare list must unwrap `data`; data-lab's
> loader already accepts either shape.
>
> **`/pos/coupons` is the last of them, and it has the same envelope now (t_e3008391).**
> With `/{industry}/inventory/receipts` fixed, `GET /grocery/pos/coupons` was the only
> route left in the API whose `limit` could silently truncate a read: `limit` capped at
> 1000, **no `offset`**, and a **bare JSON array** with no `total`. Every other count-less
> route in the family (`/{industry}/hr/locations`, `pos/departments`,
> `transport/trucks`, `pos/combo-deals`) returns the whole relation and has no `limit`
> at all — there is nothing to page — so after this change **no route caps a read without
> also advertising `total` and accepting `offset`**. That is what lets data-lab retire its
> read-time short-page rule (`docs/RETENTION.md` §3b): a rule about *no route at all*
> rather than about this one. `total` counts `pos.coupons` rows under the same filter
> (`active_only` included) and takes no join — the row query LEFT JOINs `pos.departments`
> only to name the department, on its primary key, so the join cannot multiply rows.
> The `ORDER BY` ends on `coupon_id`: `valid_until` is the worst sort key in the API, not
> merely a tied one — on the test data dir all 8 coupons share a single `valid_until`
> (2027-09-21), so the entire relation is one tie cluster and every page boundary falls
> inside it. `active_only` is unchanged: the ingest calls `active_only=false` and counts
> every advertised row, so a retired-but-referenced coupon stays resolvable. Consumers
> that expected a bare list must unwrap `data`; the UI's table loader already did, and
> data-lab's loader accepts either shape.

> **A receipt header has NO vendor, and the route must not pretend otherwise
> (t_7d966a37).** Every grocery receipt is one of *our own warehouse loads*, so a
> receipt's goods are the PRODUCTS' vendors', not one vendor's: on a fresh 30-day
> backfill 75 of 78 receipts carried goods from 2–5 vendors (1 vendor on the other
> 3 — a coincidence of that day's pick list, not a rule). `inv.receipts.supplier_id`
> and `inv.receipts.supplier_name` are both therefore **unwritten**: the only
> grocery writer is `transport.receive_delivered_loads`, whose INSERT is
> `(receipt_id, location_id, received_dt, po_number, load_id, total_cost)` — no
> vendor column at all, id *or* name — and a DSD drop deliberately writes no
> receipt (`suppliers.generate_dsd_deliveries`; its vendor lives on
> `inv.dsd_deliveries`). Measured: **0 of 78** receipts with a non-NULL
> `supplier_id` or `supplier_name`, and 0 of 9 on each of four independent 8-day
> seeds. `supplier_id` is a reserved slot for a future single-vendor receipt path,
> not a write someone forgot.
>
> So `/inventory/receipts` does not serve `supplier_id` — and must not: the route
> SQL in `base/api/main.py` is shared across industries, and
> `gas-station/generator/schema.sql`, `base/db/init.sql` and
> `base/db/03_grocery_schema.sql` all define `inv.receipts` **without** that
> column, so a shared projection would 500 those data dirs in order to serve a
> column of NULLs here. (`supplier_name` stays in the projection for gas-station,
> whose `check_and_restock` DOES populate it — grouped per vendor, one receipt per
> vendor per location. Grocery leaves it NULL.)
>
> **Vendor attribution lives on the LINE, and is complete.** Every
> `inv.receipt_items` row reaches a vendor through `product_id →
> inv.products.supplier_id`: 18,539 of 18,539 lines on the same backfill. A
> vendor-level receipts measure joins `stg_inv_receipt_items → stg_inv_products`
> on `product_id`; it must not wait for a header column, and a NULL
> `stg_inv_receipts.supplier_id` is not an ingest gap.

> **A shared route must PROBE an industry-specific column, not assume it
> (t_04e1e8be).** `base/api/main.py` runs **one SQL string per path for every
> industry**, and the schemas disagree about columns as well as about tables:
>
> | column | grocery | gas-station / `base/db/init.sql` / `base/db/03_grocery_schema.sql` |
> |---|---|---|
> | `inv.receipts.load_id` | declared (FK → `transport.loads`) | absent (gas-station has no `transport` schema at all) |
> | `inv.products.supplier_id` | declared (FK → `inv.suppliers`) | absent (gas-station has no `inv.suppliers`) |
>
> Two shared routes projected them unconditionally — `/{industry}/inventory/receipts`
> (`r.load_id`, from the `#11` change above) and `/{industry}/inventory/products`
> (`ip.supplier_id`, from t_ed802fa6) — so on a data dir generated from
> `gas-station/generator/schema.sql` both answered **HTTP 500 `42703`**: measured
> `column r.load_id does not exist` and `column ip.supplier_id does not exist`, on a
> scratch data dir built from that schema with the entrypoint's grants. Note the
> shape of the failure: a *parse-time* error, so the whole route dies for every page,
> not just the rows that would have used the column.
>
> Both projections are now guarded with `_has_column(industry, schema, table,
> column)` — a per-process-cached `information_schema.columns` probe, the column
> analogue of `_has_vendor_tables` / `_products_has_elasticity_columns`. Where the
> data dir lacks the column the key is **omitted from the payload**, not returned as
> NULL: the key set is what data-lab's `raw_inv.receipts` / `raw_inv.products` are
> built from, so an always-NULL column would be a landzone column of nulls. Grocery
> data dirs are unchanged (both keys still served).
>
> `tools/check_api_schema_agreement.py` now audits this class: for every route that
> serves all industries with one projection, each `alias.column` it names must exist
> in **every** industry's `schema.sql` unless the route guards it with
> `_has_column(industry, ...)`. The union-level table check that already existed could
> not see it — those tables exist everywhere, only the columns do not.
>
> Rule of thumb: when adding a column to a shared `/{industry}/` route, `git grep` it
> across every `schema.sql` first, and if the sets differ, probe it. "Symmetry with the
> neighbouring route" has now caused this bug twice (receipts `load_id`, products
> `supplier_id`).

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

**Assertion inventory:** 39 hard-FK checks + 37 semantic-type checks + 5
temporal = 81 (t_2ffb43a0 added `HARD-29`, `SEMA-14`, `SEMA-15`; t_30410dea
added `SEMA-34`, `SEMA-35`).

### Day completeness is two streams, not one (t_30410dea)

`run_backfill` decided a past day was done from its POS stream alone
(`max(transaction_dt).hour >= 23`) and skipped it. `timeclock.events` is a
*separate* stream and it can be lost or half-written on a day whose sales
finished, so such a day was never revisited by anything:

* **half a punch stream** — test/107 2026-10-15 held 16 `clock_in` against 63
  `clock_out`; 47 employee-days had a `clock_out` whose `clock_in` did not exist
  anywhere, and data-lab's `assert_timeclock_pairs` (severity=error) was red on
  rows no later pass could reach. `SEMA-34` now restates that test at the source.
* **no punch stream at all** — after the 2026-11-16 reset the same slot held
  2026-10-12 … 10-15 with 766–1,789 sales rows and zero punches. Nothing was
  red: `assert_timeclock_pairs` groups the rows that exist, so it passes
  vacuously on a day with none. `SEMA-35` is the assertion that sees it — a
  completed day with sales owes its roster a clock.

The generator now asks **both** questions before calling a day complete
(`main._punch_stream_verdict` → `timeclock.punch_stream_gap`) at the boot gap
scan and in `run_backfill`. When the sales are complete but the clock is not,
the day is not skipped and its sales are not rewritten: only
`timeclock.generate_day_events()` runs, which completes an employee-day it finds
half-written (the *planned* `clock_in` for a `clock_out` that lost its partner,
never an invented instant) and writes the shift plan for a day it holds nothing
for. Untraded days are left alone — `pos.transactions` is the trade test, so an
empty day is never "repaired" into a full roster.

**The re-ingest half is data-lab's.** A repaired day is stamped at its business
instants, i.e. *below* the warehouse's `event_dt` cursor, so an incremental
ingest can never see it. The repair logs a WARNING naming the day; re-read it
with an explicit window (`grocery_ingest_api` `start_dt`/`end_dt`) or the EDW
keeps the old shape while the source is now correct.

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

- **Resolved in source by t_b17da778 (2026-10-04), still not converged on
  existing data dirs:** the "add a column to an existing data dir" migration
  this codebase relies on **cannot ALTER an existing data dir**, because the
  generator's role does not own the tables `entrypoint.sh` created (see §3a).
  Measured on CT106 2026-10-04 and re-verified on the dev data dir the same
  day: `ALTER TABLE pos.loyalty_members ADD COLUMN ...` raises
  `InsufficientPrivilege` **even with `IF NOT EXISTS` and even when the column
  is already present**, because Postgres checks table ownership before it
  discovers there is nothing to do. `CREATE TABLE` and `CREATE INDEX IF NOT
  EXISTS`, by contrast, *do* work: entrypoint.sh grants `verisim=UC/postgres`
  on each schema, and `CREATE INDEX` needs ownership of the table, not the
  schema. So the split is: CREATE lands, ALTER does not.

  `t_b17da778` fixed the two halves that were in Verisim's control:
  `elasticity.seed_elasticity_columns` no longer raises on a refused ALTER
  (it was crashing `seed_all()` before the main loop — a green container and
  45+ minutes of zero writes), `customers.ensure_tables` commits its CREATE
  before attempting its ALTER and verifies the CREATE landed instead of
  assuming it, and `bootstrap_database` now runs an idempotent reconcile pass
  over an existing volume (`schema_reconcile.py`) that adds each missing
  relation, column and index individually, each in its own transaction.

  **What that cannot do:** on a data dir where the ALTER is refused, the
  *missing columns* still do not land — the pass converges everything the
  generator's privileges allow and reports the rest. The remaining remedy is
  still the one named below: apply `schema.sql` as `$POSTGRES_USER` in
  `entrypoint.sh`, or `ALTER TABLE … OWNER TO $POSTGRES_USER` for every table
  after applying it. `t_28b047b8` tracks the `inv.*` readiness side.
- **Data-lab re-ingest (acceptance criterion 3):** after the `load_id` API
  change, data-lab should re-ingest and confirm 0 cross-schema orphans. Tracked
  under data-lab #26–#31; no Verisim generator change is pending for this.
- **Inventory adjustments (#27):** Verisim emits **no** `inventory_adjustments`
  table/endpoint. `inv.stock_levels.quantity_on_hand` is mutated in place.
  data-lab derives `unexplained_variance` as period-end on-hand − period-start
  on-hand − receipts + shrinkage. No Verisim change required.
