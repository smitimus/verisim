# Verisim

Realistic mock data generator for retail analytics. Spins up a self-contained grocery store simulation — transactions, inventory, employee timeclock, supply chain, loyalty programs — with automatic 30-day backfill and real-time tick generation.

Built for analytics engineers who need a realistic, schema-rich data source to practice dbt, Airflow, Meltano, Superset, and similar tools. Pairs with [data-lab](https://github.com/smitimus/data-lab) for a full end-to-end analytics stack.

---

## Quick Start

### Option 1 — Docker (simplest)

```bash
docker run -d --name verisim-grocery \
  -p 5499:5432 -p 8010:8000 -p 8501:8501 \
  smiti/verisim-grocery:latest
```

### Option 2 — One-liner installer (Debian/Ubuntu)

```bash
curl -fsSL https://raw.githubusercontent.com/smitimus/verisim/main/install.sh | sudo bash
```

Installs Docker if needed, pulls the image, starts the container.

### Option 3 — Clone + develop

```bash
git clone https://github.com/smitimus/verisim.git
cd verisim
cp grocery/.env.example grocery/.env   # edit YOUR_* values
./switch.sh dev
```

---

## What You Get

| Service | URL | Purpose |
|---------|-----|---------|
| Streamlit UI | `http://localhost:8501` | Dashboard · Generator Control · Scenarios · Promotions · Distributions · Table Explorer · Data Dictionary |
| FastAPI | `http://localhost:8010/docs` | REST API + interactive docs |
| PostgreSQL | `localhost:5499` | Source database (user: `verisim` / pass: `verisim` / db: `grocery`) |

**Data model:** ~40 tables across 9 schemas — `hr`, `pos`, `timeclock`, `ordering`, `fulfillment`, `transport`, `inv`, `pricing`, `control`

**On first start:** the generator backfills 30 days of history, then switches to real-time simulation (15-minute ticks by default).

---

## Configuration

Edit `grocery/config.yaml` — changes take effect on the next tick, no restart needed.

Key settings:
```yaml
tick_interval_seconds: 30      # wall-clock interval (= 15 min simulated)
locations:
  store_count: 3
  warehouse_count: 1
daily_volumes:
  min_transactions: 800
  max_transactions: 3000
```

**Trigger a scenario via API:**
```bash
curl -X POST http://localhost:8010/grocery/generator/scenarios \
  -H "Content-Type: application/json" \
  -d '{"scenario_name": "rush_hour"}'
```

Available scenarios: `rush_hour`, `weekend`, `holiday_week`, `double_coupons`, `promotion`

---

## Development

```bash
./switch.sh dev      # start multi-container dev stack (build from source)
./switch.sh test     # build standalone → run as single container
./switch.sh release  # pull Docker Hub image
./switch.sh status   # show current mode
```

After code changes, rebuild just the affected service:
```bash
docker compose -f grocery/compose.yaml build generator
docker compose -f grocery/compose.yaml up -d generator
```

**Publish to Docker Hub:**
```bash
bash build-and-push.sh grocery 1.1.0
```

---

## Gas Station

The gas station generator source is preserved in `gas-station/`. It targets the shared `verisim-base` platform (postgres + api + ui) and generates fuel pump transactions, convenience store POS, and pricing data. Development is currently paused — grocery standalone is the active product.

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│              Standalone Container                │
│   supervisord manages 4 processes:               │
│   ┌──────────┐ ┌─────────┐ ┌────────┐ ┌──────┐ │
│   │PostgreSQL│ │Generator│ │FastAPI │ │  UI  │ │
│   │  :5432   │ │(daemon) │ │ :8000  │ │:8501 │ │
│   └──────────┘ └─────────┘ └────────┘ └──────┘ │
└──────────────────────────────────────────────────┘
```

The grocery standalone image is built from source with gas-station routes stripped at build time (see `grocery/standalone/strip_gas_station.py`). This keeps the image lean and branded without code duplication.

---

## License

MIT — see [LICENSE](LICENSE)
