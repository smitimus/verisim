# AGENTS.md — Verisim Generator

Multi-industry mock data generation platform. Primary product: `smiti/verisim-grocery` on Docker Hub.

## Directory Layout

| Directory | Purpose |
|-----------|---------|
| `/opt/verisim/base/` | Shared platform: postgres + FastAPI + Streamlit UI |
| `/opt/verisim/grocery/` | Grocery generator — **active, primary product** |
| `/opt/verisim/gas-station/` | Gas station generator — **paused, source preserved** |
| `*/standalone/` | All-in-one Docker build (postgres + api + ui + generator via supervisord) |
| `*/generator/` | Data generation logic + models |
| `*/api/` | FastAPI endpoints (grocery has its own stripped-down API) |

## switch.sh — Dev Mode Management

From `/opt/verisim/`:

```bash
./switch.sh dev      # Multi-container from source (postgres + api + ui + generator)
./switch.sh test     # Build standalone → verisim-grocery:local
./switch.sh release  # Pull Docker Hub image (what stacks/verisim-grocery/ uses)
./switch.sh status   # Show current mode
```

- Dev stack runs on same ports as prod (5499/8010/8501) — bring down stacks/verisim-grocery/ first
- After code changes in dev: `docker compose -f grocery/compose.yaml build <service> && docker compose -f grocery/compose.yaml up -d <service>`
- Config changes (`grocery/config.yaml`) take effect next tick — no rebuild needed

## Docker Hub Releases

From `/opt/verisim/`:

```bash
bash build-and-push.sh grocery            # builds smiti/verisim-grocery:latest
bash build-and-push.sh grocery 1.1.0      # tags 1.1.0 + latest
bash build-and-push.sh gas-station        # builds smiti/verisim-gas-station:latest
```

- Platform: `linux/amd64`
- Build context is `verisim/` (needs access to both `base/` and industry source)

### Build Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `build-and-push.sh` fails on `docker buildx` | Buildx not configured | Run `docker buildx create --use` first |
| Image builds but generator crashes on startup | Route stripping removed wrong routes | Check `standalone/strip_*.py` logic |
| `docker compose` pulls old image instead of local | `switch.sh release` mode active | Run `switch.sh dev` or `switch.sh test` |
| Standalone image ~2GB | Multi-stage build not using slim base | Check Dockerfile for unnecessary deps in final stage |
| Generator won't connect to postgres | DB not ready yet | Container waits up to 30 retries (5s apart) — check logs |

## Route Stripping at Build Time

Base API (`base/api/main.py`) contains routes for all industries. At build time:
- `grocery/standalone/strip_gas_station.py` removes gas-station routes for the grocery image
- `gas-station/standalone/strip_grocery.py` removes grocery routes for the gas-station image

## Testing Infrastructure

### compose.test.yaml — Local Standalone Image Test

`grocery/compose.test.yaml` runs the locally-built standalone image (verisim-grocery:local) for end-to-end verification before pushing to Docker Hub.

```bash
# 1. Build test image
./switch.sh test

# 2. Start test container (clean data dir)
docker compose -f grocery/compose.test.yaml up -d

# 3. Monitor backfill (30 days auto-backfill on first start)
docker logs -f verisim-grocery-test

# 4. Run crash/recovery cycle tests
#    See grocery/test-cycles-final-report.md for the full test protocol
docker stop verisim-grocery-test && sleep 120 && docker start verisim-grocery-test
```

**What compose.test.yaml validates:**
- Fresh start + 30-day backfill
- Auto-transition to realtime
- Data persistence across container restarts
- Crash recovery (no duplicate IDs, no data loss)
- Daily/hourly distribution accuracy

**Test VM** (clean install validation):
- Host: proxmox (192.168.1.40) — SSH as root
- VM ID 106 at testvm (192.168.1.6)
- Used to validate `install.sh` from a clean Debian

### Test Coverage Gaps
- **No unit tests** — generator code has no pytest setup
- **No API integration tests** — FastAPI endpoints untested
- **No CI/CD pipeline** — no automated test runs

## Generator Config (`config.yaml`)

- `tick_interval_seconds` — wall-clock interval (30s = 15min simulated)
- `locations` — store + warehouse counts and employee ranges per location
- `volumes` — `pos_transactions_per_day` range, 24 hourly weights, day-of-week multipliers
- `products` — category tree (10 departments, ~40 categories), initial SKU count
- `pricing` — price change frequency, tax rate
- `inventory` — initial stock, restock threshold
- `loyalty` — signup rate, usage rate
- `coupons` / `combo_deals` — active counts, duration, usage rates
- `scenarios` — named event presets (rush_hour, weekend, holiday_week, etc.)

Config is read at each tick — no restart needed for changes. Mounted into the container read-only.

## Backfill / Realtime Behavior

- On fresh (empty) DB: auto-backfills last 30 days, then switches to realtime
- If `backfill_end_date = today`: backfills current day hour-by-hour up to current hour
- Re-running backfill over same range is safe — existing days skipped
- Force re-generate: `POST /grocery/generator/start` with `{"mode":"backfill","force":true}`

## REST API Endpoints (FastAPI, port 8010)

The Verisim grocery standalone image serves a FastAPI API at port 8010 with Swagger docs at `/docs`.

### Generator Control
| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/generator/status` | Current generator state (mode, running, tick stats) |
| POST | `/grocery/generator/start` | Start/resume generator, accepts `{"mode":"backfill","force":true}` |
| POST | `/grocery/generator/stop` | Pause generator at next tick boundary |

### Data Access (all with offset pagination)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/employees` | Employee roster |
| GET | `/grocery/locations` | Store/warehouse locations |
| GET | `/grocery/products` | Product catalog |
| GET | `/grocery/transactions` | POS transaction headers |
| GET | `/grocery/transaction-items` | Line items |
| GET | `/grocery/timeclock-events` | Timeclock events |
| GET | `/grocery/inventory/stock-levels` | Current stock counts |
| GET | `/grocery/supply-chain/orders` | Store orders |
| GET | `/grocery/supply-chain/fulfillments` | Warehouse fulfillments |
| GET | `/grocery/supply-chain/shipments` | Truck shipments |

### Analytics / Dashboards

Used by the Streamlit UI tabs (`_dashboard`, `_distributions`):
| Method | Path | Description |
|--------|------|-------------|
| GET | `/grocery/distributions/hourly` | Transaction distribution by hour |
| GET | `/grocery/distributions/daily` | Transaction distribution by day |
| GET | `/grocery/distributions/department` | Department-level sales mix |

## PostgreSQL Access (Dev Stack)

```bash
docker exec verisim-grocery-dev-postgres psql -U verisim -d grocery -c "SELECT ..."
```

Credentials: `verisim` / `verisim` / db: `grocery` / port: `5499`

## Grocery Data Model — Non-Obvious Business Logic

- **Transaction total**: `total = subtotal + tax - coupon_savings - deal_savings`
- **Line item total**: `line_total = (unit_price - discount) * quantity` — discount is per-unit
- **Timeclock events**: 4 types — `clock_in`, `clock_out`, `break_start`, `break_end`
- **`mart_loyalty_cohort.total_spend`**: nullable for members who signed up but never purchased

## Source Tables Consumed by dbt

The data-lab dbt project expects these 27 source tables. If you add/rename a table here, update the matching staging model in `/opt/data-lab/airflow/dbt/grocery/models/staging/`.

| Generator Schema | Tables | dbt Staging Model Prefix |
|-----------------|--------|--------------------------|
| hr | locations, employees, schedules | stg_hr_* |
| pos | departments, products, price_history, coupons, combo_deals, loyalty_members, loyalty_point_transactions, transactions, transaction_items | stg_pos_* |
| timeclock | events | stg_timeclock_* |
| ordering | store_orders, store_order_items | stg_ordering_* |
| fulfillment | orders, order_items | stg_fulfillment_* |
| transport | trucks, loads, load_items | stg_transport_* |
| inv | stock_levels, shrinkage_events, receipts, receipt_items, products | stg_inv_* |
| pricing | weekly_ads, ad_items | stg_pricing_* |

## Code Quality Notes

### Oversized Files (need splitting)
| File | Pure LOC | Problem |
|------|----------|---------|
| `base/ui/app.py` | ~2000 | Monolithic Streamlit UI — one file for all 7 tabs |
| `grocery/generator/main.py` | ~550 | Entry point mixes DB bootstrap, state management, and the generation loop |
| `grocery/generator/models/pos.py` | ~520 | All POS logic (seeding, transactions, coupons, deals, loyalty) in one file |

### Tooling Gaps
- **No pyproject.toml** — no type checker, no linter config
- **No pytest** — no test runner, no conftest, no test files
- **No pre-commit hooks** — no automated quality gates
- **No CI/CD** — no GitHub Actions, no automated builds/tests
- **Unvalidated config** — `config.py` reads YAML without schema validation (pydantic or similar)

## Known Bugs (Fixed — Do Not Revert)

Both confirmed fixed on fresh backfill data:
- **Timeclock pairing**: `generate_events()` queries existing events before inserting; `generate_day_events()` clamps clock_out to 23:30 same day
- **Loyalty points balance**: `_record_loyalty_points()` uses `FOR UPDATE` row lock; dbt test orders by `(transaction_dt, points_balance_after)` for same-tick tiebreaks

## Gas Station Status

Source preserved in `gas-station/`. Requires verisim-base running (base/ contains shared postgres + api + ui). Not active development — grocery standalone is primary product.

## Streamlit UI Architecture (`base/ui/app.py`)

**Tab reset bug**: `st.tabs` has no `key` param and resets to tab 0 on every full-app rerun. Fix: wrap every tab's content in `@st.fragment` for isolation.

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
- Module-level vars needed across fragments (e.g. SCHEMA_TABLES, TABLE_DOCS) assigned **before** `st.tabs(...)`, not inside tab block — fragment functions create local scope
- New tabs: wrap in `@st.fragment` from the start; don't add bare widget code at tab level

**Fragment inventory**: `_dashboard` (run_every=15), `_generator_control`, `_scenarios`, `_promotions`, `_distributions` (run_every=15), `_table_explorer`, `_data_dictionary`
