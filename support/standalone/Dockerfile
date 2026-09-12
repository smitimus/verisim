# =============================================================================
# Verisim Customer Support — Standalone Image
# =============================================================================
# All-in-one container: PostgreSQL 18 + FastAPI + Streamlit + Generator.
# Managed by supervisord. Build context: /opt/verisim/
#
# Build (from /opt/verisim/):
#   docker build --platform linux/amd64 \
#     -t smiti/verisim-support:latest \
#     -f support/standalone/Dockerfile \
#     .
#
# Or use the helper script:
#   bash /opt/verisim/build-and-push.sh support
#
# Run:
#   docker run -d \
#     -p 8012:8000 -p 8502:8501 -p 5501:5432 \
#     -v verisim_support_data:/var/lib/postgresql/data \
#     smiti/verisim-support:latest
# =============================================================================

# ── Stage 1: Python dependencies ─────────────────────────────────────────────
FROM python:3.11-slim AS deps

WORKDIR /build
COPY support/standalone/requirements.txt .
RUN python -m venv /app/venv \
 && /app/venv/bin/pip install --no-cache-dir --upgrade pip \
 && /app/venv/bin/pip install --no-cache-dir -r requirements.txt


# ── Final image ───────────────────────────────────────────────────────────────
FROM python:3.11-slim

LABEL org.opencontainers.image.title="Verisim Customer Support" \
      org.opencontainers.image.description="Contact-center mock data generator — tickets, voice ACD, live chat, sNPS surveys — PostgreSQL + API + UI + Generator" \
      org.opencontainers.image.source="https://github.com/smiti/verisim"

# Install PostgreSQL 18 from the official PGDG apt repository + supervisor
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
 && apt-get install -y --no-install-recommends postgresql-18 \
 && rm -rf /var/lib/apt/lists/*

# Python venv from stage 1
COPY --from=deps /app/venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# ── UI ────────────────────────────────────────────────────────────────────────
COPY base/ui/app.py /app/ui/app.py

# ── Generator ─────────────────────────────────────────────────────────────────
COPY support/generator/ /app/generator/

# ── Default config (overridable by volume mount at /config/config.yaml) ──────
COPY support/config.yaml /app/config.yaml

# ── supervisord + entrypoint ──────────────────────────────────────────────────
COPY support/standalone/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY support/standalone/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# ── API: strip grocery + gas-station routes at build time ─────────────────────
COPY support/standalone/strip_support.py /tmp/strip_support.py
COPY base/api/main.py /tmp/api_src.py
RUN mkdir -p /app/api \
 && python /tmp/strip_support.py /tmp/api_src.py /app/api/main.py \
 && rm /tmp/strip_support.py /tmp/api_src.py

# ── Postgres directories ──────────────────────────────────────────────────────
RUN mkdir -p /var/lib/postgresql/data /config /var/log/supervisor \
 && chown -R postgres:postgres /var/lib/postgresql/data

VOLUME ["/var/lib/postgresql/data", "/config"]

ENV POSTGRES_USER=verisim \
    POSTGRES_PASSWORD=verisim \
    POSTGRES_DB=support

# API: 8000   UI: 8501   PostgreSQL: 5432
EXPOSE 8000 8501 5432

ENTRYPOINT ["/entrypoint.sh"]
