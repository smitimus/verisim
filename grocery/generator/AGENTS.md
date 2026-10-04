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

**`run_backfill` writes the ledger too (t_ac80c514).** It did not, which meant a day
produced by a backfill left no telemetry at all — the 30-day window of a fresh install,
and every day a gap-fill repairs. On dev that left 94,867 transactions across 30 days with
no ledger rows, and 2026-09-05..09-09 (the labour-day window) empty while the holiday
regime was plainly stamped on the fact tables. The backfill now calls the same
`record_stats` once per simulated hour, inside the hour's own loop, passing the counts
the hour actually wrote (`len(depletion)`, never the planned `pos_count`) and
`scenario.scenario_tag`. It passes `bump_state_clock=False`: `last_tick_at` is what
`/status` and data-lab's readiness sensor read as "the generator is alive", and a
backfill is simulating yesterday, so stamping the wall clock there would read as live
progress. `grocery/api/tests/test_generation_stats.py` pins all of this.

Note the sibling generators were never affected: gas-station and support each write an
inline INSERT per simulated hour in their own `run_backfill`. Grocery was the odd one out.

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
