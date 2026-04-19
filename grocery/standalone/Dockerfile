# =============================================================================
# Verisim Grocery — Standalone Image
# =============================================================================
# All-in-one container: PostgreSQL 16 + FastAPI + Streamlit + Generator.
# Managed by supervisord. Build context: /opt/verisim/
#
# Build (from /opt/verisim/):
#   docker build --platform linux/amd64 \
#     -t smiti/verisim-grocery:latest \
#     -f grocery/standalone/Dockerfile \
#     .
#
# Or use the helper script:
#   bash /opt/verisim/build-and-push.sh
#
# Run:
#   docker run -d \
#     -p 8010:8000 -p 8501:8501 -p 5432:5432 \
#     -v verisim_data:/var/lib/postgresql/data \
#     [-e POSTGRES_USER=verisim] \
#     [-e POSTGRES_PASSWORD=verisim] \
#     [-e POSTGRES_DB=grocery] \
#     smiti/verisim-grocery:latest
# =============================================================================

# ── Stage 1: Python dependencies ─────────────────────────────────────────────
FROM python:3.11-slim AS deps

WORKDIR /build
COPY grocery/standalone/requirements.txt .
RUN python -m venv /app/venv \
 && /app/venv/bin/pip install --no-cache-dir --upgrade pip \
 && /app/venv/bin/pip install --no-cache-dir -r requirements.txt


# ── Final image ───────────────────────────────────────────────────────────────
FROM python:3.11-slim

LABEL org.opencontainers.image.title="Verisim Grocery" \
      org.opencontainers.image.description="Grocery retail mock data generator — PostgreSQL + API + UI + Generator" \
      org.opencontainers.image.source="https://github.com/smiti/verisim-grocery"

# Install PostgreSQL 16 from the official PGDG apt repository + supervisor
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    gnupg curl ca-certificates lsb-release supervisor locales \
 && echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
 && locale-gen \
 && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] \
    https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends postgresql-16 \
 && rm -rf /var/lib/apt/lists/*

# Python venv from stage 1
COPY --from=deps /app/venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# ── API: strip gas-station routes at build time ───────────────────────────────
COPY grocery/standalone/strip_gas_station.py /tmp/strip_gas_station.py
COPY base/api/main.py /tmp/api_src.py
RUN mkdir -p /app/api \
 && python /tmp/strip_gas_station.py /tmp/api_src.py /app/api/main.py \
 && rm /tmp/strip_gas_station.py /tmp/api_src.py

# ── UI ────────────────────────────────────────────────────────────────────────
COPY base/ui/app.py /app/ui/app.py

# ── Generator ─────────────────────────────────────────────────────────────────
COPY grocery/generator/ /app/generator/

# ── Default config (overridable by volume mount at /config/config.yaml) ──────
COPY grocery/standalone/config.yaml /app/config.yaml

# ── supervisord + entrypoint ──────────────────────────────────────────────────
COPY grocery/standalone/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY grocery/standalone/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# ── Postgres directories ──────────────────────────────────────────────────────
RUN mkdir -p /var/lib/postgresql/data /config /var/log/supervisor \
 && chown -R postgres:postgres /var/lib/postgresql/data

VOLUME ["/var/lib/postgresql/data", "/config"]

ENV POSTGRES_USER=verisim \
    POSTGRES_PASSWORD=verisim \
    POSTGRES_DB=grocery

# API: 8000   UI: 8501   PostgreSQL: 5432
EXPOSE 8000 8501 5432

ENTRYPOINT ["/entrypoint.sh"]
