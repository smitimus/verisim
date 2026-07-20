# AGENTS.md — Grocery Generator

Python data generator at `grocery/generator/`. Generates realistic POS, timeclock, supply chain, and inventory data into a PostgreSQL database.

## Structure

```
generator/
├── main.py          # Entry point: bootstrap, seeding, backfill, tick loop (620 LOC)
├── config.py        # YAML config loader + dataclass hierarchy (228 LOC)
├── schema.sql       # DB schema (tables, indexes, control schema)
├── models/          # Domain-specific DB write modules
│   ├── pos.py       # POS transactions, coupons, deals, loyalty (660 LOC — largest)
│   ├── hr.py        # Locations, employees, hire/terminate
│   ├── timeclock.py # Clock in/out events, daily pairing
│   ├── ordering.py  # Store replenishment orders
│   ├── fulfillment.py # Warehouse order processing
│   ├── transport.py # Truck dispatch, load management, delivery
│   ├── inventory.py # Stock levels, depletion, receipts
│   ├── shrinkage.py # Perishable expiry, shrinkage events
│   ├── promotions.py # Weekly ads lifecycle
│   └── scheduling.py # Labor scheduling, actuals resolution
├── scenarios/
│   └── scenario_engine.py  # Named event presets (rush_hour, weekend, etc.)
└── tests/           # pytest tests (4 test files)
```

## Where to Look

| Task | Location | Notes |
|------|----------|-------|
| Change generation volume/timing | `config.py` dataclasses or `config.yaml` | Config reloaded each tick — no restart needed |
| Add/modify POS logic | `models/pos.py` | Largest model — transactions, coupons, deals, loyalty |
| Add new source table | `schema.sql` + relevant `models/*.py` | Must also update `data-lab/airflow/dbt/grocery/models/sources.yml` |
| Modify backfill behavior | `main.py` auto_backfill_if_fresh() | Gap-aware, idempotent, partial-day handling |
| Add scenario | `scenarios/scenario_engine.py` + `config.yaml` scenarios section | |
| Run tests | `pytest grocery/generator/tests/` | |

## Conventions

### Model Architecture
Each `models/*.py` module owns one domain and exposes:
- `seed_<domain>(conn, cfg, ...)` — idempotent seed on first start
- `generate_<events>(conn, sim_dt, ...)` — called each tick or daily
- `fetch_<entities>(conn)` — refresh in-memory caches (every 20 ticks)

All models use raw `psycopg2` with `execute_values` for bulk inserts. No ORM.

### Tick Lifecycle
```
1. reload_config() — hot-reload config.yaml
2. read_state()    — check mode (realtime/backfill/stopped/paused)
3. run_tick():
   a. Scenario context (volume multiplier, tag)
   b. POS transactions → inventory depletion
   c. Timeclock events
   d. Probabilistic events (price changes, hire/terminate)
   e. Supply chain (if midnight): orders → fulfillment → dispatch → delivery
   f. Daily models (if midnight): shrinkage, promotions, scheduling
4. record_stats() → control.generation_stats
```

### Backfill
- Fresh DB: 30-day backfill (today-30 → today), then auto-transition to realtime
- Gap detection: checks max transaction timestamp per day, fills missing days
- Partial day: today gets hours 0 → current_hour, final tick uses `datetime.now()` for seamless realtime handoff
- Idempotent: existing days skipped, partial days resume from last hour

### Config Hierarchy
`config.yaml` (mounted read-only) → `config.py` dataclasses → env var overrides for DB connection only.

Key config sections: `generator` (tick_interval, sim_minutes), `volumes` (daily range, hourly weights, DOW multipliers), `locations`, `loyalty`, `pricing`, `inventory`, `coupons`, `combo_deals`, `scenarios`.

## Anti-PATTERNS

- **Don't** use SQLAlchemy — this project uses raw psycopg2 by design (ADR)
- **Don't** change `schema.sql` without updating all affected model files
- **Don't** add tables that dbt consumes without updating `data-lab/airflow/dbt/grocery/models/sources.yml`
- **Don't** modify `config.yaml` defaults in `config.py` — YAML overrides at runtime
- **Don't** add imports beyond stdlib + psycopg2 + pyyaml — standalone image must stay slim
