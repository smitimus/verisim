#!/bin/bash
set -e

PG_DATA=/var/lib/postgresql/data
PG_BIN=/usr/lib/postgresql/16/bin
PG_CTL="$PG_BIN/pg_ctl"
PSQL="$PG_BIN/psql"
INITDB="$PG_BIN/initdb"
SCHEMAS="hr,pos,timeclock,ordering,fulfillment,transport,inv,control,pricing"

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
