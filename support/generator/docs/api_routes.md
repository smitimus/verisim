# support served routes

<!-- GENERATED FILE — do not edit by hand.
     Regenerate: python3 tools/gen_api_docs.py --industry support
     Verified in CI by tools/gen_api_docs.py --check (gas-station/generator/tests/test_build_checks.py). -->

Produced by running `support/standalone/strip_support.py` over `base/api/main.py` and parsing the surviving
`@app.<verb>(...)` decorators — i.e. the surface the **image** serves, not the
source file (which also carries every other industry's routes).

**56 routes.** Every path is served relative to the API root, port 8010.
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

## Support — Queues

| Method | Path | Description |
|--------|------|-------------|
| GET | `/support/queues` | reads `support.queues` |
| GET | `/support/categories` | reads `support.categories`, `support.queues` |

## Support — Customers

| Method | Path | Description |
|--------|------|-------------|
| GET | `/support/customers` | reads `support.customers` |

## Support — Tickets

| Method | Path | Description |
|--------|------|-------------|
| GET | `/support/tickets` | reads `support.tickets`, `support.queues` |
| GET | `/support/tickets/{ticket_id}` | reads `support.tickets`, `support.queues`, `support.customers`, `support.ticket_comments`, `support.ticket_actions` |
| GET | `/support/ticket-comments` | reads `support.ticket_comments` |
| GET | `/support/ticket-actions` | reads `support.ticket_actions` |

## Support — Voice ACD

| Method | Path | Description |
|--------|------|-------------|
| GET | `/support/voice/calls` | reads `voice.calls`, `support.queues` |
| GET | `/support/voice/summary` | reads `voice.calls` |

## Support — Chat

| Method | Path | Description |
|--------|------|-------------|
| GET | `/support/chat/sessions` | reads `chat.sessions`, `support.queues` |
| GET | `/support/chat/messages` | reads `chat.messages` |

## Support — Surveys

| Method | Path | Description |
|--------|------|-------------|
| GET | `/support/surveys` | reads `survey.surveys` |
| GET | `/support/surveys/scorecard` | reads `survey.surveys` |

## Support — Training

| Method | Path | Description |
|--------|------|-------------|
| GET | `/support/training/courses` | reads `training.courses` |
| GET | `/support/training/assignments` | reads `training.assignments`, `training.courses` |

## Support — Agents

| Method | Path | Description |
|--------|------|-------------|
| GET | `/support/agents/performance` | Per-agent rollup: tickets handled, calls, chats, sNPS. |

## Support — Scenarios

| Method | Path | Description |
|--------|------|-------------|
| GET | `/support/generator/scenarios` | reads `control.active_scenarios` |
| POST | `/support/generator/scenarios` | reads `control.active_scenarios` |
| DELETE | `/support/generator/scenarios/{scenario_name}` | reads `control.active_scenarios` |
| GET | `/support/generator/scenario-schedules` | reads `control.scenario_schedules` |
| POST | `/support/generator/scenario-schedules` | reads `control.scenario_schedules` |
| DELETE | `/support/generator/scenario-schedules/{schedule_id}` | reads `control.scenario_schedules` |
