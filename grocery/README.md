# verisim-grocery

Grocery store industry generator for the Verisim platform.
Generates realistic grocery transactions, supply chain movements, timeclock events, loyalty activity, promotions, and shrinkage. Writes directly into the `grocery` database on the shared verisim postgres via `verisim_network`.

**Requires `verisim-base` to be running first.**

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
stacks/verisim-grocery/
├── compose.yaml
├── .env
├── README.md
├── config.yaml             # Generator config (tick rate, departments, volumes, etc.)
├── build-and-push.sh       # Build and push standalone Docker Hub image
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

## Deploy (development — with verisim-base)

```bash
# verisim-base must be running first
cd /opt/stacks/verisim-base
docker compose build && docker compose up -d

# Then start the grocery generator
cd /opt/stacks/verisim-grocery
docker compose build && docker compose up -d
```

On first start, the generator self-bootstraps: it connects to postgres, creates the `grocery` database, and runs `schema.sql` to create all schemas and tables.

To reset: stop the stack, drop the `grocery` database, restart.

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

Inherits from `verisim-base`:

| Item | Value |
|------|-------|
| DB host | `verisim-base-postgres` (internal) / `${IP}` (external) |
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
# Must be run from /opt/stacks/ (build context includes both verisim-base and verisim-grocery)
cd /opt/stacks
bash verisim-grocery/build-and-push.sh           # latest
bash verisim-grocery/build-and-push.sh 1.0.0     # versioned
```

## Downstream

| Tool | Connection |
|------|-----------|
| Meltano | `tap-postgres-grocery` — `${IP}:5499`, DB `grocery` |
| Airflow | `grocery_pipeline` DAG — runs Meltano extract + dbt |
| dbt | `/opt/stacks/airflow/dbt/grocery/` — 27 staging + 14 mart models |
| Superset | "Grocery Overview" dashboard — mart tables in EDW |
| CloudBeaver | Pre-configured as "Verisim — Grocery Source" |
| OpenMetadata | "VerisimGrocery" database service |
