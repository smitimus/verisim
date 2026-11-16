# gas-station served routes

<!-- GENERATED FILE — do not edit by hand.
     Regenerate: python3 tools/gen_api_docs.py --industry gas-station
     Verified in CI by tools/gen_api_docs.py --check (gas-station/generator/tests/test_build_checks.py). -->

Produced by running `gas-station/standalone/strip_grocery.py` over `base/api/main.py` and parsing the surviving
`@app.<verb>(...)` decorators — i.e. the surface the **image** serves, not the
source file (which also carries every other industry's routes).

**39 routes.** Every path is served relative to the API root, port 8010.
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

## Fuel

| Method | Path | Description |
|--------|------|-------------|
| GET | `/gas-station/fuel/transactions` | reads `fuel.transactions`, `fuel.grades` |
| GET | `/gas-station/fuel/transactions/summary` | reads `fuel.transactions`, `fuel.grades` |
| GET | `/gas-station/fuel/grades` | reads `fuel.grades` |
| GET | `/gas-station/fuel/price-history` | reads `fuel.price_history`, `fuel.grades` |
| GET | `/gas-station/fuel/pumps` | reads `fuel.pumps`, `hr.locations` |

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
