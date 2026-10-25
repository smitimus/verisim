#!/bin/bash
set -e

PG_DATA=/var/lib/postgresql/data
PG_BIN=/usr/lib/postgresql/18/bin
PG_CTL="$PG_BIN/pg_ctl"
PSQL="$PG_BIN/psql"
INITDB="$PG_BIN/initdb"
MIGRATIONS_DIR=/app/migrations
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

start_pg() {   # $1 = log file
    su -s /bin/bash postgres -c "$PG_CTL start -D $PG_DATA -w -l $1"
}
stop_pg() {
    su -s /bin/bash postgres -c "$PG_CTL stop -D $PG_DATA -m fast"
}

chown -R postgres:postgres "$PG_DATA"

# ── First-run: initialize the data directory ─────────────────────────────────
FIRST_RUN=0
if [ ! -f "$PG_DATA/PG_VERSION" ]; then
    FIRST_RUN=1
    echo "[entrypoint] First run — initializing PostgreSQL..."
    su -s /bin/bash postgres -c \
        "$INITDB -D $PG_DATA --encoding=UTF8 --locale=C.UTF-8 --auth=trust"

    # Allow local connections without password during setup
    echo "host all all 127.0.0.1/32 trust" >> "$PG_DATA/pg_hba.conf"
fi

# ── Setup server: schema on a first run, MIGRATIONS on every run ─────────────
#
# PostgreSQL runs here only for as long as setup needs it; supervisord starts the
# long-lived one afterwards.
#
# The migration block is the missing half of the boot. `schema.sql` is applied
# ONLY when `$PG_DATA/PG_VERSION` is absent, so on every existing data dir a
# schema change in the image reaches nothing at all — and `schema_reconcile` (the
# generator's own pass) only ever ADDS columns/indexes/tables and runs as a role
# that does not own them. A `DROP NOT NULL` therefore had no path to an existing
# volume. `/app/migrations/*.sql` is that path: idempotent files, applied as
# `postgres` (the owner of every relation created here), every boot, in name
# order, with ON_ERROR_STOP so a half-applied schema cannot look healthy.
#
# It fails the boot rather than serving on a schema this image cannot write: a
# container whose generator could not seed its own tables reported `healthy` for
# forty minutes once already (t_66647bbd), and a crash-loop is the honest signal.
echo "[entrypoint] Starting PostgreSQL for schema setup / migrations..."
if ! start_pg /tmp/pg_setup.log; then
    echo "[entrypoint] PostgreSQL did not start for setup — see /tmp/pg_setup.log" >&2
    tail -n 40 /tmp/pg_setup.log >&2 || true
    exit 1
fi

if [ "$FIRST_RUN" = "1" ]; then
    echo "[entrypoint] Creating $POSTGRES_USER role and $POSTGRES_DB database..."
    su -s /bin/bash postgres -c \
        "$PSQL -c \"CREATE ROLE $POSTGRES_USER WITH LOGIN PASSWORD '$POSTGRES_PASSWORD';\""
    su -s /bin/bash postgres -c \
        "$PSQL -c \"CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;\""

    echo "[entrypoint] Applying schema..."
    su -s /bin/bash postgres -c \
        "$PSQL -d $POSTGRES_DB -f /app/generator/schema.sql"
fi

shopt -s nullglob
migrations=("$MIGRATIONS_DIR"/*.sql)
if [ "${#migrations[@]}" -eq 0 ]; then
    echo "[entrypoint] No migrations found in $MIGRATIONS_DIR"
fi
for migration in "${migrations[@]}"; do
    echo "[entrypoint] Applying migration $(basename "$migration")"
    su -s /bin/bash postgres -c \
        "$PSQL -v ON_ERROR_STOP=1 -d $POSTGRES_DB -f $migration"
done

# Grant the app role on each schema. EVERY boot, not just the first: it is
# idempotent, and it is what gives a schema an image upgrade just added the same
# privileges a fresh bootstrap would have given it (the derivation above only
# helps if the grants actually run again).
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

# Stop the setup server — supervisord will start it properly
stop_pg
echo "[entrypoint] PostgreSQL setup complete (first_run=$FIRST_RUN, ${#migrations[@]} migration(s))."

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
