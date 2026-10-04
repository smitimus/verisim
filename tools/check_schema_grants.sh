#!/bin/bash
# Reproduce the t_ac80c514 failure mode on each industry's entrypoint:
# does the granted-schema list stay in step with schema.sql?
#
# A hand-kept literal list means adding a schema to schema.sql without adding it
# to the entrypoint applies the DDL as `postgres` and never grants the app role,
# so the generator dies on its first write with `permission denied for schema X`.
# (That is how grocery's weather covariate crash-looped the image in t_2ab1fb0a.)
set -u
root="${1:?usage: check_schema_grants.sh <verisim-checkout>}"
rc=0

for industry in grocery gas-station support; do
    ent="$root/$industry/standalone/entrypoint.sh"
    schema="$root/$industry/generator/schema.sql"
    [ -f "$ent" ] || { echo "$industry: no entrypoint.sh"; continue; }
    [ -f "$schema" ] || { echo "$industry: no schema.sql"; continue; }

    # What schema.sql actually creates.
    want=$(grep -oE 'CREATE SCHEMA IF NOT EXISTS [a-z_]+' "$schema" \
           | awk '{print $NF}' | sort -u)

    # What the entrypoint grants: either derived from schema.sql or a literal.
    if grep -q 'CREATE SCHEMA IF NOT EXISTS' "$ent"; then
        echo "$industry: DERIVED from schema.sql (safe)"
        echo "  grants: $(grep -A1 'SCHEMAS=' "$ent" | head -2 | tr '\n' ' ')"
        continue
    fi

    have=$(grep -E '^SCHEMAS=' "$ent" | head -1 \
           | sed 's/^SCHEMAS="//; s/"$//' | tr ',' '\n' | tr -d ' ' | sort -u)

    missing=$(comm -23 <(echo "$want") <(echo "$have"))
    extra=$(comm -13 <(echo "$want") <(echo "$have"))

    if [ -n "$missing" ]; then
        echo "$industry: FAIL — schema(s) created but never granted: $(echo "$missing" | paste -sd, -)"
        rc=1
    else
        echo "$industry: OK — all $(echo "$want" | wc -l) schemas granted"
    fi
    [ -n "$extra" ] && echo "  (note: grants list names non-existent schema(s): $(echo "$extra" | paste -sd, -))"
done

exit $rc
