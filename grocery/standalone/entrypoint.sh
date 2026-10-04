#!/bin/bash
set -e

PG_DATA=/var/lib/postgresql/data
PG_BIN=/usr/lib/postgresql/18/bin
PG_CTL="$PG_BIN/pg_ctl"
PSQL="$PG_BIN/psql"
INITDB="$PG_BIN/initdb"
# Every schema the generator writes to. DERIVED FROM schema.sql, not hand-listed.
#
# It used to be a literal comma list, and adding a schema to schema.sql without adding
# it here meant the entrypoint applied schema.sql as `postgres` and never granted the
# app role on the new schema — so the generator, which connects as that role, died on
# its first write with `permission denied for schema weather` and crash-looped until
# supervisord gave up. CI caught it as a container that never left `stopped` (the
# `weather` covariate, t_2ab1fb0a). Reading the DDL is the whole fix: the list is the
# schemas that exist by construction, so a new one cannot be forgotten.
SCHEMAS=$(grep -oE 'CREATE SCHEMA IF NOT EXISTS [a-z_]+' /app/generator/schema.sql \
          | awk '{print $NF}' | sort -u | paste -sd, -)
echo "[entrypoint] Schemas to grant: $SCHEMAS"

POSTGRES_USER=${POSTGRES_USER:-verisim}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-verisim}
POSTGRES_DB=${POSTGRES_DB:-grocery}

# ── First-run: initialize PostgreSQL data directory ──────────────────────────
if [ ! -f "$PG_DATA/PG_VERSION" ]; then
    echo "[entrypoint] First run — initializing PostgreSQL..."
    chown -R postgres:postgres "$PG_DATA"
    su -s /bin/bash postgres -c \
        "$INITDB -D $PG_DATA --encoding=UTF8 --locale=C.UTF-8 --auth=trust"

    # Allow local connections without password during setup
    echo "host all all 127.0.0.1/32 trust" >> "$PG_DATA/pg_hba.conf"

    # Start postgres temporarily for database setup
    su -s /bin/bash postgres -c \
        "$PG_CTL start -D $PG_DATA -w -l /tmp/pg_setup.log"

    echo "[entrypoint] Creating $POSTGRES_USER role and $POSTGRES_DB database..."
    su -s /bin/bash postgres -c \
        "$PSQL -c \"CREATE ROLE $POSTGRES_USER WITH LOGIN PASSWORD '$POSTGRES_PASSWORD';\""
    su -s /bin/bash postgres -c \
        "$PSQL -c \"CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;\""

    echo "[entrypoint] Applying schema..."
    su -s /bin/bash postgres -c \
        "$PSQL -d $POSTGRES_DB -f /app/generator/schema.sql"

    # Grant all on each schema
    for schema in ${SCHEMAS//,/ }; do
        su -s /bin/bash postgres -c \
            "$PSQL -d $POSTGRES_DB -c \"GRANT ALL ON SCHEMA $schema TO $POSTGRES_USER;\""
        su -s /bin/bash postgres -c \
            "$PSQL -d $POSTGRES_DB -c \"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA $schema TO $POSTGRES_USER;\""
        su -s /bin/bash postgres -c \
            "$PSQL -d $POSTGRES_DB -c \"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA $schema TO $POSTGRES_USER;\""
        su -s /bin/bash postgres -c \
            "$PSQL -d $POSTGRES_DB -c \"ALTER DEFAULT PRIVILEGES IN SCHEMA $schema GRANT ALL ON TABLES TO $POSTGRES_USER;\""
        su -s /bin/bash postgres -c \
            "$PSQL -d $POSTGRES_DB -c \"ALTER DEFAULT PRIVILEGES IN SCHEMA $schema GRANT ALL ON SEQUENCES TO $POSTGRES_USER;\""
    done

    # Stop postgres — supervisord will start it properly
    su -s /bin/bash postgres -c "$PG_CTL stop -D $PG_DATA -m fast"
    echo "[entrypoint] PostgreSQL initialization complete."
fi

# ── PostgreSQL: ensure external connections are allowed ──────────────────────
if ! grep -q "0.0.0.0/0" "$PG_DATA/pg_hba.conf" 2>/dev/null; then
    echo "host all all 0.0.0.0/0 md5" >> "$PG_DATA/pg_hba.conf"
fi

# ── Config: seed default if nothing is mounted ───────────────────────────────
if [ ! -f /config/config.yaml ]; then
    echo "[entrypoint] No config mounted — using defaults."
    cp /app/config.yaml /config/config.yaml
fi

# ── Log directory ────────────────────────────────────────────────────────────
mkdir -p /var/log/supervisor
chown -R postgres:postgres /var/lib/postgresql/data

# ── Hand off to supervisord ──────────────────────────────────────────────────
echo "[entrypoint] Starting supervisord (postgres + generator + api + ui)..."
exec supervisord -c /etc/supervisor/conf.d/supervisord.conf
