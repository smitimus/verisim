"""📖 Documentation tab.

The per-industry prose guide: source systems, endpoints, notes.

Split out of `base/ui/app.py` (t_c2eca5dd). The body below is the original,
unchanged; only the `render(ctx)` wrapper is new. It still closes over this
module's globals, which is why the imports above enumerate every name it reads
— a missed import surfaces as a NameError the first time a user opens this tab,
never at import time.
"""
from ui_lib.context import Context  # noqa: F401  (documents the render signature)
import streamlit as st


def render(ctx):
    industry = ctx.industry
    if industry == "gas-station":
        st.markdown("""
## Gas Station / Convenience Store

Continuous mock data platform for a **Gas Station / Convenience Store** operation.
Simulates 4 linked enterprise source systems backed by a single PostgreSQL database (`gas_station`).

---

### Source Systems

| System | Schema | Description |
|--------|--------|-------------|
| **HR** | `hr` | Employees and store locations |
| **POS** | `pos` | Point-of-sale transactions, products, loyalty members |
| **Fuel** | `fuel` | Fuel pump transactions, grades, price history |
| **Inventory** | `inv` | Stock levels, restocking events |

### Scenarios

| Scenario | Effect |
|----------|--------|
| `normal` | Baseline traffic with hourly + day-of-week patterns |
| `rush_hour` | 2.5× volume during hours 7–9am and 4–7pm |
| `weekend` | 1.3× baseline volume |
| `promotion` | 15% discount on Snacks & Beverages |
| `fuel_spike` | Fuel prices increased ~12% |

### API Reference

The FastAPI service exposes a full Swagger UI at `/docs`.

**Key endpoints (prefix: `/gas-station/`):**
- `GET /gas-station/status` — generator state
- `POST /gas-station/generator/start` — start realtime or backfill
- `PATCH /gas-station/generator/config` — change volume_multiplier, scenario, tick_interval
- `GET /gas-station/pos/transactions?start_dt=...&end_dt=...`
- `GET /gas-station/fuel/transactions?start_dt=...&end_dt=...`
- `GET /gas-station/fuel/grades` / `GET /gas-station/fuel/price-history`
- `GET /gas-station/hr/employees` / `GET /gas-station/hr/locations`
- `GET /gas-station/inventory/stock-levels`
- `GET /gas-station/stats/generation` — per-tick stats
- `GET /industries` — list all available industries
""")
    elif industry == "support":
        st.markdown("""
## Customer Support (Contact Center)

Continuous mock data platform for a **customer-support contact center**.
Simulates the ticketing system, voice phone ACD, live chat, sNPS survey
system, and agent training system backed by the `support` PostgreSQL database.

---

### Source Systems

| System | Schema | Description |
|--------|--------|-------------|
| **HR** | `hr` | Contact-center sites, agents with skill groups + handle-time profiles |
| **Queues** | `support.queues` | Ticket queues (billing, technical, account, shipping, returns, escalations) with SLA targets |
| **Customers** | `support.customers` | Customer master with tier + lifetime value |
| **Ticketing** | `support.tickets` | Full lifecycle: create → assign → move/escalate → resolve → close, with reopen |
| **Comments** | `support.ticket_comments` | Agent + customer + system comments (internal notes supported) |
| **Audit** | `support.ticket_actions` | Immutable action trail: assigned, moved, escalated, status_change, sla_breached |
| **Voice ACD** | `voice.calls` | Call detail records: wait, ring, talk, hold, ACW, abandonment, disposition |
| **Live Chat** | `chat.sessions` / `chat.messages` | Chat sessions with full transcripts |
| **Surveys** | `survey.surveys` | sNPS attached to closed interactions: 0–10 NPS, CSAT, reason, verbatim |
| **Training** | `training.courses` / `training.assignments` | Courses + assignments triggered by onboarding, QA findings, launches |

### Contact Flow

```
Customer contact (phone / chat / email / web / social)
    → voice.calls or chat.sessions (ACD handling)
    → support.tickets (follow-up / direct creation)
    → assignment by skill group → queue movement / escalation
    → comments + actions accumulate → resolved → closed (or reopened)
    → survey.surveys (sNPS 1–2 days later)
    → detractor clusters → training.assignments (qa_finding)
```

### Scenarios

| Scenario | Effect |
|----------|--------|
| `normal` | Baseline contact volume (business-hours curve) |
| `rush_hour` | 1.6× during 9–11am / 2–4pm peaks |
| `weekend` | 0.65× baseline |
| `service_outage` | 4× surge, negative sentiment, longer calls, abandonment ↑ |
| `weather_outage` | 2.5× surge, queue stress ↑ |
| `product_launch` | 1.8× how-to contacts, training triggers |
| `marketing_blast` | 2.2× billing/account contacts |
| `holiday_week` | 1.4× shipping/returns pressure |

### API Reference

The FastAPI service exposes a full Swagger UI at `/docs`.

**Key endpoints (prefix: `/support/`):**
- `GET /support/status` — generator state
- `POST /support/generator/start` — start realtime or backfill
- `POST /support/generator/scenarios` — activate a scenario
- `GET /support/queues` / `GET /support/categories`
- `GET /support/tickets?status=...&queue_id=...` — plus `/support/tickets/{id}` with comments + actions
- `GET /support/voice/calls` / `GET /support/voice/summary`
- `GET /support/chat/sessions` / `GET /support/chat/messages`
- `GET /support/surveys` / `GET /support/surveys/scorecard`
- `GET /support/training/courses` / `GET /support/training/assignments`
- `GET /support/agents/performance` — per-agent tickets/calls/chats/sNPS
- `GET /industries` — list all available industries
""")
    else:
        st.markdown("""
## Grocery Store

Continuous mock data platform for a **Grocery Store** operation.
Simulates 8 linked enterprise source systems backed by the `grocery` PostgreSQL database.

---

### Source Systems

| System | Schema | Description |
|--------|--------|-------------|
| **HR** | `hr` | Employees at stores and warehouses |
| **POS** | `pos` | Transactions, products, departments, coupons, combo deals |
| **Timeclock** | `timeclock` | Employee shift clock-in/clock-out events |
| **Ordering** | `ordering` | Store replenishment orders placed to warehouse |
| **Fulfillment** | `fulfillment` | Warehouse picks and packs orders |
| **Transport** | `transport` | Trucks and delivery loads to stores |
| **Inventory** | `inv` | Stock levels per product per store |

### Supply Chain Flow

```
Low stock detected → ordering.store_orders created
    → fulfillment.orders (warehouse picks)
    → transport.loads (truck dispatched)
    → inv.receipts (store receives, stock replenished)
```

### Scenarios

| Scenario | Effect |
|----------|--------|
| `normal` | Baseline grocery shopping patterns |
| `rush_hour` | 2.0× volume after-work and weekend mornings |
| `weekend` | 1.3× baseline volume |
| `promotion` | 15% discount on featured departments |
| `holiday_week` | 1.6× volume, heavy produce and meat |
| `double_coupons` | Coupon values doubled, higher loyalty attach |

### API Reference

The FastAPI service exposes a full Swagger UI at `/docs`.

**Key endpoints (prefix: `/grocery/`):**
- `GET /grocery/status` — generator state
- `POST /grocery/generator/start` — start realtime or backfill
- `PATCH /grocery/generator/config` — change volume_multiplier, scenario
- `GET /grocery/pos/transactions?start_dt=...&end_dt=...`
- `GET /grocery/pos/departments` / `GET /grocery/pos/coupons` / `GET /grocery/pos/combo-deals`
- `GET /grocery/timeclock/events?start_dt=...&end_dt=...`
- `GET /grocery/ordering/orders` / `GET /grocery/fulfillment/orders`
- `GET /grocery/transport/trucks` / `GET /grocery/transport/loads`
- `GET /grocery/hr/employees` / `GET /grocery/hr/locations`
- `GET /grocery/inventory/stock-levels`
- `GET /grocery/stats/generation`
- `GET /industries` — list all available industries
""")
