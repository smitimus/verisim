"""
Pytest runner for the cross-schema integrity harness (Verisim #11).

Two kinds of tests:

1. DB-free contract tests — always run, no live database required:
     * every assertion spec is well-formed (id / dimension / title / sql)
     * the committed SQL spec file documents every assertion id
   These guarantee the harness itself stays correct and in sync even in a
   CI environment with no Postgres.

2. Live assertions — parametrized over every AssertionSpec, run against a
   real grocery DB. The whole class is SKIPPED when no reachable database is
   found (probe reads GROCERY_TEST_DB / VERISIM_* env or tries the local
   dev port 5499). Run it after a fresh backfill:

       pytest grocery/generator/tests/test_cross_schema_integrity.py

   or, to point at a specific database:

       GROCERY_TEST_DB=postgresql://verisim:verisim@127.0.0.1:5499/grocery \
           pytest grocery/generator/tests/test_cross_schema_integrity.py
"""
from __future__ import annotations

import os
import pytest

from .cross_schema_integrity import (
    ASSERTIONS,
    AssertionSpec,
    run_assertion,
    by_dimension,
)

SQL_SPEC_PATH = os.path.join(
    os.path.dirname(__file__), "..", "sql", "check_cross_schema_integrity.sql"
)

VALID_DIMENSIONS = {"hard_fk", "semantic_type"}

# Columns that carry a generation/event timestamp. Assertions that reference one
# of these MUST also scope to completed days (< CURRENT_DATE), because the
# supply-chain block only runs at the hour-0 midnight boundary — a partial
# backfill day (today) legitimately lacks downstream rows. Snapshot/seed tables
# (stock_levels, pricing.ad_items) have no such column and are exempt.
TIME_BOUNDED_COLS = {
    "transaction_dt",
    "event_dt",
    "received_dt",
    "recorded_at",
    "created_at",
    "scheduled_date",
}


# ---------------------------------------------------------------------------
# DB-free contract tests
# ---------------------------------------------------------------------------
def test_all_assertions_well_formed():
    seen_ids = set()
    for spec in ASSERTIONS:
        assert isinstance(spec, AssertionSpec)
        assert spec.id and spec.id not in seen_ids, f"duplicate or empty id: {spec.id!r}"
        seen_ids.add(spec.id)
        assert spec.dimension in VALID_DIMENSIONS, (
            f"{spec.id}: dimension {spec.dimension!r} not in {VALID_DIMENSIONS}"
        )
        assert spec.title.strip(), f"{spec.id}: empty title"
        sql = spec.sql.strip()
        assert sql.upper().startswith("SELECT"), f"{spec.id}: sql must be a SELECT"
        # Orphan-style assertions return child rows. They are either a NOT EXISTS
        # anti-join (hard FK checks) or a positive join + type filter (semantic
        # type checks, which are NOT FK-enforced). Both return offending rows.
        is_orphan_query = "NOT EXISTS" in sql.upper() or "WHERE" in sql.upper()
        assert is_orphan_query, (
            f"{spec.id}: sql must isolate orphan/type-mismatch rows "
            f"(NOT EXISTS anti-join or a WHERE-filtered join)"
        )
        # Completed-day tolerance must be present on time-bounded tables.
        # Snapshot/seed tables (stock_levels, pricing.ad_items) have no
        # generation timestamp and are exempt from this rule.
        references_time_col = any(col in sql.lower() for col in TIME_BOUNDED_COLS)
        if references_time_col:
            assert "CURRENT_DATE" in sql.upper(), (
                f"{spec.id}: missing completed-day (< CURRENT_DATE) tolerance filter"
            )


def test_dimension_counts_nonzero():
    assert len(by_dimension("hard_fk")) >= 1
    assert len(by_dimension("semantic_type")) >= 1


def test_sql_spec_file_documents_every_assertion():
    """The committed .sql spec must list every assertion id (stays in sync)."""
    if not os.path.exists(SQL_SPEC_PATH):
        pytest.skip(f"SQL spec not present at {SQL_SPEC_PATH}")
    with open(SQL_SPEC_PATH, "r", encoding="utf-8") as fh:
        contents = fh.read()
    missing = [spec.id for spec in ASSERTIONS if spec.id not in contents]
    assert not missing, f"SQL spec missing assertion ids: {missing}"


# ---------------------------------------------------------------------------
# Live assertions (skipped unless a grocery DB is reachable)
# ---------------------------------------------------------------------------
def _probe_connection():
    """Return a live psycopg2 connection or None (no DB available)."""
    import psycopg2

    dsn = os.environ.get("GROCERY_TEST_DB")
    if dsn:
        try:
            return psycopg2.connect(dsn, connect_timeout=5)
        except Exception:
            return None

    # Fall back to the local dev stack / common docker ports.
    candidates = [
        ("127.0.0.1", 5499, "verisim", "verisim", "grocery"),
        ("127.0.0.1", 5432, "verisim", "verisim", "grocery"),
    ]
    for host, port, user, pw, db in candidates:
        try:
            return psycopg2.connect(
                host=host, port=port, user=user, password=pw,
                dbname=db, connect_timeout=3,
            )
        except Exception:
            continue
    return None


@pytest.fixture(scope="module")
def grocery_conn():
    conn = _probe_connection()
    if conn is None:
        pytest.skip("No reachable grocery database (set GROCERY_TEST_DB to enable)")
    yield conn
    conn.close()


@pytest.mark.parametrize(
    "spec",
    ASSERTIONS,
    ids=[a.id for a in ASSERTIONS],
)
def test_cross_schema_assertion(grocery_conn, spec):
    orphans = run_assertion(grocery_conn, spec)
    assert not orphans, (
        f"{spec.id} [{spec.dimension}] {spec.title}: "
        f"{len(orphans)} orphan row(s) found — first: {orphans[0]}"
    )
