# grocery served routes

<!-- GENERATED FILE — do not edit by hand.
     Regenerate: python3 tools/gen_api_docs.py --industry grocery
     Verified in CI by tools/gen_api_docs.py --check (gas-station/generator/tests/test_build_checks.py). -->

Produced by running `grocery/standalone/strip_gas_station.py` over `base/api/main.py` and parsing the surviving
`@app.<verb>(...)` decorators — i.e. the surface the **image** serves, not the
source file (which also carries every other industry's routes).

**74 routes.** Every path is served relative to the API root, port 8010.
`/{industry}/...` routes are shared by every industry; the rest are this one's own.

## Platform

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | health |
| GET | `/industries` | industries |

## Status

| Method | Path | Description |
|--------|------|-------------|
| GET | `/{industry}/status` | reads `control.generator_state`, `control.generation_stats` |

## Generator Control

| Method | Path | Description |
|--------|------|-------------|
| POST | `/{industry}/generator/start` | reads `control.generator_state` |
| POST | `/{industry}/generator/stop` | reads `control.generator_state` |
| POST | `/{industry}/generator/pause` | reads `control.generator_state` |
| POST | `/{industry}/generator/resume` | reads `control.generator_state` |
| PATCH | `/{industry}/generator/config` | reads `control.generator_state` |

## HR

| Method | Path | Description |
|--------|------|-------------|
| GET | `/{industry}/hr/locations` | reads `hr.locations` |
| GET | `/{industry}/hr/employees` | reads `hr.employees` |

## POS

| Method | Path | Description |
|--------|------|-------------|
| GET | `/{industry}/pos/transactions` | POS transaction headers, with **two** independent time windows. |
| GET | `/{industry}/pos/transactions/summary` | reads `pos.transactions` |
| GET | `/{industry}/pos/transaction-items` | POS transaction lines, with the same two windows as the header route. |
| GET | `/{industry}/pos/products` | reads `pos.products`, `pos.departments` |
| GET | `/{industry}/pos/loyalty-members` | Loyalty *cards*, with the household each belongs to. |
| GET | `/{industry}/pos/price-history` | reads `pos.price_history`, `pos.products` |

## Grocery — POS

| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/pos/departments` | reads `pos.departments` |
| GET | `/grocery/pos/coupons` | Coupons, paged and counted (t_e3008391). |
| GET | `/grocery/pos/combo-deals` | Combo deals. |
| GET | `/grocery/pos/loyalty-point-transactions` | reads `pos.loyalty_point_transactions` |
| GET | `/grocery/pos/returns` | Customer returns, with **two** independent time windows. |
| GET | `/grocery/pos/return-items` | Return lines, with the same two windows as ``/grocery/pos/returns``. |
| POST | `/grocery/pos/coupons` | reads `pos.coupons` |
| PATCH | `/grocery/pos/coupons/{coupon_id}` | reads `pos.coupons` |
| DELETE | `/grocery/pos/coupons/{coupon_id}` | Delete a coupon, unless the sales history still depends on it. |

## Grocery — Customers

| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/pos/customers` | The customer / household master dimension, with its loyalty rollup. |
| GET | `/grocery/pos/customers/summary` | The dimension's shape: household count and mix by segment and age band. |

## Grocery — Timeclock

| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/timeclock/events` | Clock events, with **two** independent time windows. |
| GET | `/grocery/timeclock/summary` | reads `timeclock.events` |

## Grocery — Ordering

| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/ordering/orders` | reads `ordering.store_orders`, `hr.locations`, `ordering.store_order_items` |
| GET | `/grocery/ordering/summary` | reads `ordering.store_orders` |
| GET | `/grocery/ordering/order-items` | reads `ordering.store_order_items` |

## Grocery — Fulfillment

| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/fulfillment/orders` | reads `fulfillment.orders`, `hr.locations`, `hr.employees`, `fulfillment.items` |
| GET | `/grocery/fulfillment/items` | reads `fulfillment.items` |

## Grocery — Transport

| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/transport/trucks` | reads `transport.trucks` |
| GET | `/grocery/transport/loads` | reads `transport.loads`, `transport.trucks`, `hr.employees`, `hr.locations` |
| GET | `/grocery/transport/load-items` | reads `transport.load_items` |

## Inventory

| Method | Path | Description |
|--------|------|-------------|
| GET | `/{industry}/inventory/stock-levels` | reads `inv.stock_levels`, `pos.products`, `inv.products` |
| GET | `/{industry}/inventory/stockout-events` | Sale lines the shelf could not cover (t_959cd040) — the lost-sales feed. |
| GET | `/{industry}/inventory/suppliers` | Vendors, with the behaviour a vendor-performance mart measures against (t_57b1a1ab): the promised lead time and its spread, the short-ship rate, the credit terms, and the fulfilment model. |
| GET | `/{industry}/inventory/suppliers/{supplier_id}` | One vendor and its performance: fill rate, dollars short-shipped, dollars credited, and days-to-pay. |
| GET | `/{industry}/inventory/short-ship-events` | Lines a vendor could not fill (t_57b1a1ab) — the inbound-shortage feed. |
| GET | `/{industry}/inventory/supplier-credit-memos` | Credit claims against vendors, and whether they were paid (t_57b1a1ab). |
| GET | `/{industry}/inventory/dsd-deliveries` | Direct Store Delivery drops (t_57b1a1ab) — the vendor's own truck at the store. |
| GET | `/{industry}/inventory/dsd-delivery-items` | DSD line items. |
| GET | `/{industry}/inventory/supplier-delivery-schedules` | When each DSD vendor visits each store (t_57b1a1ab) — the commitment a delivery's on-time rate is measured against. |
| GET | `/{industry}/inventory/sku-demand-daily` | Per store-SKU-day requested / fulfilled / lost totals (t_959cd040). |
| GET | `/{industry}/inventory/receipts` | Receipt headers, newest first, paged and counted (t_2ab8e903). |
| GET | `/{industry}/inventory/products` | reads `inv.products`, `pos.products` |
| GET | `/{industry}/inventory/receipt-items` | reads `inv.receipt_items`, `inv.receipts`, `pos.products` |

## Stats

| Method | Path | Description |
|--------|------|-------------|
| GET | `/{industry}/stats/generation` | The per-tick generation ledger — paged, and windowed on both clocks (t_ac80c514). |
| GET | `/{industry}/stats/today` | reads `control.generation_stats` |
| GET | `/{industry}/stats/backfill-progress` | reads `control.generator_state` |
| GET | `/{industry}/stats/recent` | reads `control.generation_stats`, `support.tickets`, `support.queues`, `pos.transactions`, `hr.locations` |
| GET | `/{industry}/stats/distributions` | Returns record-count distributions grouped by day and key classification columns for the given look-back window (default 30 days). |

## Grocery — HR

| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/hr/schedules` | reads `hr.schedules` |

## Grocery — Inventory

| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/inventory/shrinkage-events` | reads `inv.shrinkage_events` |

## Grocery — Pricing

| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/pricing/weekly-ads` | reads `pricing.weekly_ads` |
| GET | `/grocery/pricing/ad-items` | reads `pricing.ad_items` |
| POST | `/grocery/pricing/weekly-ads` | reads `pricing.weekly_ads` |
| PATCH | `/grocery/pricing/weekly-ads/{ad_id}` | reads `pricing.weekly_ads` |
| DELETE | `/grocery/pricing/weekly-ads/{ad_id}` | reads `pricing.weekly_ads` |
| POST | `/grocery/pricing/ad-items` | reads `pricing.ad_items` |
| DELETE | `/grocery/pricing/ad-items/{ad_item_id}` | reads `pricing.ad_items` |

## Grocery — Online

| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/online/orders` | Online orders, with **three** independent windows. |
| GET | `/grocery/online/order-items` | Order lines, with the same two windows as ``/grocery/online/orders``. |
| GET | `/grocery/online/order-events` | Lifecycle events, with the same two windows as ``/grocery/online/orders``. |
| GET | `/grocery/online/summary` | reads `online.orders` |

## Grocery — Scenarios

| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/generator/scenarios` | reads `control.active_scenarios` |
| POST | `/grocery/generator/scenarios` | reads `control.active_scenarios` |
| DELETE | `/grocery/generator/scenarios/{scenario_name}` | reads `control.active_scenarios` |
| GET | `/grocery/generator/scenario-schedules` | reads `control.scenario_schedules` |
| POST | `/grocery/generator/scenario-schedules` | reads `control.scenario_schedules` |
| DELETE | `/grocery/generator/scenario-schedules/{schedule_id}` | reads `control.scenario_schedules` |
