# Claude Context — Verisim

Multi-industry mock data generation platform. Primary product: `smiti/verisim-grocery` on Docker Hub.

## Directory Layout

| Directory | Purpose |
|-----------|---------|
| `base/` | Shared platform: postgres + FastAPI + Streamlit UI |
| `grocery/` | Grocery store generator — **active, primary product** |
| `gas-station/` | Gas station generator — **paused, source preserved** |
| `*/standalone/` | All-in-one Docker build (postgres + api + ui + generator via supervisord) |
| `*/generator/` | Data generation logic + models |
| `*/api/` | FastAPI endpoints (grocery has its own stripped-down API) |

## switch.sh — Dev Mode Management

Use from the repo root:

```bash
./switch.sh dev      # Multi-container from source (postgres + api + ui + generator)
./switch.sh test     # Build standalone → verisim-grocery:local, run as single container
./switch.sh release  # Pull Docker Hub image (stacks/verisim-grocery/)
./switch.sh status   # Show current mode
```

- **Dev stack** (`grocery/compose.yaml`) is self-contained — no dependency on verisim-base
- After code changes in dev, rebuild just the affected service:
  `docker compose -f grocery/compose.yaml build <service> && docker compose -f grocery/compose.yaml up -d <service>`
- Config changes (`grocery/config.yaml`) take effect next tick — no rebuild needed

## build-and-push.sh — Docker Hub Releases

```bash
bash build-and-push.sh grocery            # builds smiti/verisim-grocery:latest
bash build-and-push.sh grocery 1.1.0      # tags 1.1.0 + latest
bash build-and-push.sh gas-station        # builds smiti/verisim-gas-station:latest
```

- Platform: `linux/amd64`
- Build context is `verisim/` (needs access to both `base/` and industry source)

## Route Stripping at Build Time

The base API (`base/api/main.py`) contains routes for all industries. At build time:
- `grocery/standalone/strip_gas_station.py` removes gas-station routes for the grocery image
- `gas-station/standalone/strip_grocery.py` removes grocery routes for the gas-station image

## Generator Config (`config.yaml`)

Most settings live in `grocery/config.yaml` (or `gas-station/config.yaml`). Key sections:
- `tick_interval_seconds` — wall-clock interval (30s = 15min simulated)
- `locations` — store + warehouse counts
- `daily_volumes` — transaction counts with hourly + day-of-week weights
- `scenarios` — named event presets (rush_hour, weekend, holiday_week, etc.)

**Config changes take effect on the next tick — no container restart needed.**

## Backfill / Realtime Behavior

- On a fresh (empty) DB, the generator auto-backfills the last 30 days, then switches to realtime
- If `backfill_end_date = today`, the current day is backfilled hour-by-hour up to the current hour
- Re-running backfill over the same range is safe — existing days are skipped
- Force re-generate: `POST /grocery/generator/start` with `{"mode":"backfill","force":true}`

## Postgres Access (Dev Stack)

```bash
docker exec verisim-grocery-dev-postgres psql -U verisim -d grocery -c "SELECT ..."
```

Credentials: `verisim` / `verisim` / db: `grocery` / port: `5499`

## Grocery Data Model — Non-Obvious Business Logic

These formulas are **wrong** in the naive form — use these:

- **Transaction total**: `total = subtotal + tax - coupon_savings - deal_savings`
- **Line item total**: `line_total = (unit_price - discount) * quantity` — `discount` is per-unit
- **Timeclock events**: 4 types — `clock_in`, `clock_out`, `break_start`, `break_end`
- **`mart_loyalty_cohort.total_spend`**: nullable for members who signed up but never purchased

## Known Generator Bugs (Fixed)

Both are confirmed fixed on fresh backfill data:
- **Timeclock pairing**: `generate_events()` queries existing events before inserting; `generate_day_events()` clamps clock_out to 23:30 same day
- **Loyalty points balance**: `_record_loyalty_points()` uses `FOR UPDATE` row lock; dbt test orders by `(transaction_dt, points_balance_after)` for same-tick tiebreaks

## Gas Station Status

Gas station source is fully preserved in `gas-station/`. It requires `verisim-base` running (the shared postgres + api + ui platform in `base/`). Not the active development path — grocery standalone is the primary product.

## Streamlit UI Architecture (`base/ui/app.py`)

**Tab reset bug**: `st.tabs` has no `key` param and resets to tab 0 on every full-app rerun. Fix: every tab's content is wrapped in `@st.fragment` so interactions stay isolated.

**Fragment pattern** — used in every interactive tab:
```python
with tabN:
    @st.fragment          # add run_every=N for auto-refresh tabs (dashboard, distributions)
    def _tab_name():
        ...               # all tab content here
    _tab_name()
```

**Rules**:
- `st.rerun()` inside a fragment → must be `st.rerun(scope="fragment")`
- Module-level vars needed across fragments (e.g. `SCHEMA_TABLES`, `TABLE_DOCS`) must be assigned **before** `st.tabs(...)`, not inside a tab block — fragment functions create local scope
- New tabs: wrap in `@st.fragment` from the start; don't add bare widget code at tab level

**Current fragment inventory**: `_dashboard` (run_every=15), `_generator_control`, `_scenarios`, `_promotions`, `_distributions` (run_every=15), `_table_explorer`, `_data_dictionary`
