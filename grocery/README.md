# verisim-grocery

Grocery store industry generator for the Verisim platform.
Generates realistic grocery transactions, supply chain movements, timeclock events, loyalty activity, promotions, and shrinkage.

The dev stack is self-contained — no external dependencies. Use `switch.sh dev` from the repo root.

## Schemas Written

| Schema | Tables | Description |
|--------|--------|-------------|
| `hr` | locations, employees | Stores, warehouses, and staff |
| `pos` | transactions, transaction_items, products, departments, loyalty_members, price_history, coupons, combo_deals | Point-of-sale |
| `timeclock` | events | Employee clock-in/out |
| `ordering` | store_orders, store_order_items | Store replenishment orders |
| `fulfillment` | orders, items | Warehouse order fulfillment |
| `transport` | trucks, loads, load_items | Truck dispatch and delivery |
| `inv` | products, stock_levels, receipts, receipt_items, shrinkage_events | Inventory |
| `pricing` | weekly_ads, ad_items | Weekly ad promotions |
| `control` | generator_state, generation_stats | Generator control + metrics |

~40 tables across 9 schemas.

## Directory Structure

```
verisim/grocery/
├── compose.yaml            # Dev stack (self-contained: postgres + api + ui + generator)
├── .env
├── README.md
├── config.yaml             # Generator config (tick rate, departments, volumes, etc.)
├── generator/
│   ├── Dockerfile
│   ├── main.py             # Main generation loop + DB bootstrap
│   ├── schema.sql          # Full grocery schema definition
│   ├── models/             # Data generation modules
│   │   ├── hr.py
│   │   ├── pos.py
│   │   ├── timeclock.py
│   │   ├── ordering.py
│   │   ├── fulfillment.py
│   │   ├── transport.py
│   │   ├── inventory.py
│   │   ├── promotions.py
│   │   ├── scheduling.py
│   │   └── shrinkage.py
│   └── scenarios/          # Scenario engine (holidays, promotions, spikes)
└── standalone/             # All-in-one standalone image (for distribution)
    ├── Dockerfile
    ├── supervisord.conf
    └── entrypoint.sh
```

## Development

```bash
cd /opt/verisim
./switch.sh dev      # start self-contained dev stack (build from source)
./switch.sh test     # build standalone image, run as single container
./switch.sh release  # pull and run Docker Hub image
```

After code changes, rebuild just the affected service:
```bash
docker compose -f grocery/compose.yaml build <service>
docker compose -f grocery/compose.yaml up -d <service>
```

On first start, the generator self-bootstraps: connects to postgres, creates the `grocery` database, and runs `schema.sql` to create all schemas and tables.

To reset: stop the stack, delete `conf/verisim-grocery-dev/`, restart.

## Configuration

Edit `config.yaml` to tune:
- Tick interval and simulated time per tick
- Number of store and warehouse locations
- Transaction volume ranges and hourly patterns
- Product departments and SKU counts
- Promotion frequency (coupons, combo deals, weekly ads)
- Shrinkage rates

Changes to most settings take effect on next container restart. Volume multiplier can be adjusted live via the API or Streamlit UI.

## Credentials

| Item | Value |
|------|-------|
| DB host | `verisim-grocery-dev-postgres` (internal) / `${IP}` (external) |
| DB port | `5499` (external) |
| DB name | `grocery` |
| DB user | `verisim` |
| DB password | `verisim` |

## Standalone Distribution Image

A pre-built all-in-one image is available for users who don't need the full analytics stack:

```bash
docker pull smiti/verisim-grocery
```

This image includes PostgreSQL, the FastAPI, the Streamlit UI, and the generator — everything in one container.

To build and push a new version:

```bash
cd /opt/verisim
bash build-and-push.sh grocery            # latest
bash build-and-push.sh grocery 1.2.0      # versioned (also tags latest)
```

## Downstream

| Tool | Connection |
|------|------------|
| Meltano | `tap-postgres-grocery` — `${IP}:5499`, DB `grocery` |
| Airflow | `grocery_pipeline` DAG — runs Meltano extract + dbt |
| dbt | `/opt/stacks/airflow/dbt/grocery/` — 27 staging + 14 mart models |
| Superset | "Grocery Overview" dashboard — mart tables in EDW |
| CloudBeaver | Pre-configured as "Verisim — Grocery Source" |
| OpenMetadata | "VerisimGrocery" database service |

## Crash Simulation (testing backfill gap recovery)

```bash
cd /opt/verisim

# 1. Switch to test mode (builds verisim-grocery:local, starts as verisim-grocery-test)
./switch.sh test

# 2. Wait for auto-backfill (~60–90s), then hard-kill the container
sleep 60 && docker kill verisim-grocery-test

# 3. Restart — the backfill should detect incomplete days and fill them
cd /opt/verisim && ./switch.sh test

# 4. Wait for recovery, then verify zero incomplete days
sleep 120
docker exec verisim-grocery-test psql -U postgres -d grocery \
  -c "SELECT count(*) FROM (SELECT transaction_dt::date, count(*) as c FROM pos.transactions GROUP BY 1 HAVING count(*) < 600) sub;"
-- Expected: 0

# 5. Verify FK integrity
docker exec verisim-grocery-test psql -U postgres -d grocery \
  -c "SELECT 'txn->employee', count(*) FROM pos.transactions t WHERE t.employee_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM hr.employees e WHERE e.employee_id = t.employee_id)
      UNION ALL SELECT 'total mismatch', count(*) FROM pos.transactions WHERE total != (subtotal + tax - coupon_savings - deal_savings);"
-- Expected: two rows, both with count 0

# Teardown: ./switch.sh test (runs compose down before switching)
```
