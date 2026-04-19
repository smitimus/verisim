# verisim-gas-station

Gas station / convenience store industry generator for the Verisim platform.
Generates realistic POS transactions, fuel sales, inventory movements, loyalty activity, and price changes. Writes directly to the `verisim` postgres via `verisim_network`.

**Requires `verisim-base` to be running first.**

## Schemas Written

| Schema | Tables | Description |
|--------|--------|-------------|
| `hr` | locations, employees | Stores and staff |
| `pos` | employees, loyalty_members, products, price_history, transactions, transaction_items | Point-of-sale |
| `fuel` | grades, price_history, pumps, transactions | Fuel pump activity |
| `inv` | products, stock_levels, receipts, receipt_items | Inventory |

16 tables total across 4 schemas.

## Directory Structure

```
stacks/verisim-gas-station/
├── compose.yaml
├── .env
├── README.md
├── config.yaml         # Generator config (tick rate, location count, etc.)
└── generator/
    ├── Dockerfile
    ├── config.py       # Pydantic config dataclass
    └── ...             # Generator logic
```

## Deploy

```bash
# verisim-base must be running first
cd /opt/stacks/verisim-gas-station
docker compose build
docker compose up -d
```

The generator runs continuously, inserting new records on each tick (default: every few seconds). Check logs: `docker logs verisim-gas-station`.

## Configuration

Edit `config.yaml` to tune:
- Tick interval
- Number of locations
- Transaction volume per tick
- Fuel price drift rate

Changes take effect on next container restart.

## Credentials

Inherits from `verisim-base`:

| Item | Value |
|------|-------|
| DB host | `verisim-base-postgres` (internal) / `<your-server-ip>` (external) |
| DB port | `5499` (external) |
| DB name | `verisim` |
| DB user | `verisim` |
| DB password | `verisim` |

## Downstream

Meltano taps this database every 15 minutes (triggered by Airflow `gasstation_pipeline` DAG), replicating all 16 tables to the EDW via Singer.
