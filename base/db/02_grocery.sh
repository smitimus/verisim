#!/bin/bash
# Create the grocery database and initialize its schema.
# Runs automatically on first postgres start (alphabetically after init.sql).
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c "CREATE DATABASE grocery;"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname grocery \
    -f /docker-entrypoint-initdb.d/03_grocery_schema.sql

echo "Grocery database initialized."
