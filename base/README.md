# verisim-base

Shared platform infrastructure for the Verisim multi-industry mock data generator.
Provides: PostgreSQL (port 5499), FastAPI REST API (port 8010), Streamlit control panel (port 8501).

Industry-specific generator stacks (`verisim-gas-station`, `verisim-grocery`, etc.) connect to `verisim_network` and write into their own database on the shared postgres instance.

## Services

| Container | Image | Port | Purpose |
|-----------|-------|------|---------|
| `verisim-base-postgres` | `postgres:16` | 5499 | Shared source database (hosts all industry DBs) |
| `verisim-base-api` | built from `./api` | 8010 | FastAPI — multi-industry REST API |
| `verisim-base-ui` | built from `./ui` | 8501 | Streamlit control panel |

## Databases

Each industry has its own database on the shared postgres instance:

| Database | Industry | Created by |
|----------|----------|-----------|
| `gas_station` | Gas station / convenience store | `db/init.sql` (postgres init script) |
| `grocery` | Grocery store | `verisim-grocery` generator (self-bootstraps on first start) |

## API

The FastAPI uses an `/{industry}/` path prefix for all endpoints:

- `GET /industries` — list available industry slugs
- `GET /{industry}/pos/transactions`
- `GET /{industry}/stats/generation`
- `POST /{industry}/generator/start|stop|pause|resume`
- `PATCH /{industry}/generator/config`
- Gas station specific: `/{industry}/fuel/*`
- Grocery specific: `/{industry}/timeclock/*`, `/{industry}/ordering/*`, `/{industry}/fulfillment/*`, `/{industry}/transport/*`

Full API docs at `http://<host>:8010/docs`

## Streamlit UI Tabs

1. Overview — live metrics + auto-refresh
2. Generator Control — start/stop/pause/backfill
3. Scenarios — activate scenarios (gas station: fuel prices; grocery: coupons/deals)
4. Pricing — price history and changes
5. Table Explorer — browse all tables across schemas with per-table filters and docs
6. SQL Query — run arbitrary SQL
7. Admin — config management

## Credentials

| Item | Value |
|------|-------|
| DB user | `verisim` |
| DB password | `verisim` |
| DB port | `5499` |

## Directory Structure

```
stacks/verisim-base/
├── compose.yaml
├── .env
├── README.md
├── api/            # FastAPI app + Dockerfile
├── ui/             # Streamlit app + Dockerfile
└── db/
    ├── init.sql            # gas_station DB schema + seed data
    └── 03_grocery_schema.sql  # grocery DB (bootstrapped by verisim-grocery generator)
```

Runtime data: `conf/verisim-base/postgres/data/`

## Deploy

```bash
# Start verisim-base first (creates verisim_network)
cd /opt/stacks/verisim-base
docker compose build && docker compose up -d

# Then start industry generators
cd /opt/stacks/verisim-gas-station
docker compose build && docker compose up -d

cd /opt/stacks/verisim-grocery
docker compose build && docker compose up -d
```

On first start, postgres runs `db/init.sql` to create the `gas_station` database and all schemas. The grocery generator self-bootstraps its own database on first start.

To reset an industry: drop the database and restart the generator. To full reset: stop all stacks, delete `conf/verisim-base/postgres/data/`, restart.

## Network

Creates `verisim_network` (bridge). All industry generator stacks reference it as external:

```yaml
networks:
  verisim_network:
    external: true
```

## Adding a New Industry

1. Create `stacks/verisim-<industry>/`
2. Use `verisim_network` as external network
3. Connect to `verisim-base-postgres` at `host: verisim-base-postgres, port: 5432` (internal)
4. Self-bootstrap a new database on first start (see `verisim-grocery/generator/main.py` for pattern)
5. Register the new DB with the API via `INDUSTRY_DBS` env var or individual `<INDUSTRY>_DB` vars

## Downstream Connections

| Tool | Connection |
|------|-----------|
| Meltano | `${IP}:5499` — taps industry databases |
| CloudBeaver | Pre-configured connections for all industry sources |
| OpenMetadata | Registered as per-industry database services |
