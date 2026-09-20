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

Release mode (Docker Hub image) becomes available once
`smiti/verisim-gas-station` is published:

```bash
bash build-and-push.sh gas-station          # builds + smoke-tests the image
bash build-and-push.sh gas-station 1.0.0    # versioned tag
```

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
