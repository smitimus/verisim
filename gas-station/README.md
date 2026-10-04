# verisim-gas-station

Gas station / convenience store industry generator for the Verisim platform.
Generates realistic POS transactions, fuel sales, inventory movements,
loyalty activity, and price changes.

**Self-contained since 2026-09 (t_a6ecb731 revival)** — no verisim-base
dependency. The dev stack, test stack, and standalone image all bring their
own postgres + API + UI, exactly like the grocery industry.

## Schemas Written

| Schema | Tables | Description |
|--------|--------|-------------|
| `hr` | locations, employees | Stores and staff |
| `pos` | employees, loyalty_members, products, price_history, transactions, transaction_items | Point-of-sale |
| `fuel` | grades, price_history, pumps, transactions | Fuel pump activity |
| `inv` | products, stock_levels, receipts, receipt_items | Inventory |
| `control` | generator_state, generation_stats | Platform bookkeeping |

16 data tables across 4 schemas + control.

## Port Layout (offset from grocery so both run side by side)

| Service | Gas station | Grocery |
|---------|-------------|---------|
| PostgreSQL | 5500 | 5499 |
| API | 8011 | 8010 |
| UI | 8502 | 8501 |

## Modes (from /opt/verisim/)

```bash
./switch.sh dev gas-station        # multi-container from source
./switch.sh test gas-station       # build standalone → verisim-gas-station:local + run
./switch.sh rebuild gas-station    # rebuild image + restart test stack
./switch.sh status                 # show running modes (both industries)
```

Release mode (`switch.sh release gas-station`) still needs a deploy stack under
`/opt/data-lab/verisim-gas-station/compose.yaml`; until one exists the script says so
rather than pretending. The image itself *is* published — CI pushes
`smiti/verisim-gas-station` on every main push and on `v*` tags, on the same terms as
grocery (build + smoke + contract tests first, and a missing Docker credential fails
the run instead of skipping it).

```bash
bash build-and-push.sh gas-station          # local build + smoke test, then push
bash build-and-push.sh gas-station 1.0.0    # versioned tag
```

## Tests

```bash
python -m pytest gas-station/generator/tests/    # unit tests + build/config checkers
python -m pytest gas-station/api/tests/          # contract tests; needs a live container
```

The API suite skips when no API is up, so it is safe to run locally; CI starts a
container first, so there a skip would be a hole rather than a convenience. The
generator suite also runs the `tools/check_*.py` build checkers, which is what keeps
the strip scripts, the schema grants and the config from drifting silently — see the
"Gas Station Status" section of the top-level `AGENTS.md` for what each one catches
and why.

## Generator Behavior

- **Fresh DB**: schema self-bootstraps, seeds reference data, auto-backfills
  the last 30 days, then transitions to realtime.
- **Backfill is gap-aware and idempotent**: completed days are skipped,
  partial days resume from the hour after the last recorded transaction.
- **Config hot-reload**: `config.yaml` is re-read every tick — no restart
  needed for volume/scenario tuning.
- **Scenarios**: normal, rush_hour (commute peaks), weekend, promotion
  (Snacks/Beverages discount), fuel_spike (+12% pump prices).
- **End-of-day**: inventory restock (receipts per location/supplier) and
  fuel price moves fire at the midnight tick.

## Credentials

| Item | Value |
|------|-------|
| DB | `gas_station` on stack postgres, port 5500 (dev/test) |
| User / password | `verisim` / `verisim` |
| API docs | http://localhost:8011/docs |

## Tests

```bash
python -m pytest gas-station/generator/tests/ -v
```

Covers config loading (incl. scenario keys + weight normalization) and the
scenario engine (rush-hour stacking, fuel_spike price modifier, promotion
category injection). CI runs this suite as a blocking job.
