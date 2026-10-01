"""Regenerate grocery/generator/sql/check_cross_schema_integrity.sql from ASSERTIONS.

The committed .sql file is a human-readable/psql-runnable copy of the Python
harness, and test_sql_spec_file_documents_every_assertion fails if the two drift.
Kept as a script rather than a hand edit so the copy can never silently go stale:

    python3 scripts/regen_integrity_sql.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
GEN = os.path.abspath(os.path.join(HERE, ".."))
if GEN not in sys.path:
    sys.path.insert(0, GEN)

from tests.cross_schema_integrity import ASSERTIONS  # noqa: E402

HEADER = """-- =============================================================================
-- Verisim Grocery — Cross-schema referential integrity checks (Verisim #11)
-- =============================================================================
--
-- Each block returns the OFFENDING child rows for one cross-schema link.
-- Zero rows returned == integrity holds.
--
-- Three classes:
--   hard_fk      : child key references a parent row that does not exist
--                  (Postgres FK constraints normally prevent these).
--   semantic_type: key exists but points at the WRONG KIND of parent
--                  (NOT FK-enforced — highest-risk gaps).
--   temporal     : key and parent both exist but the parent's validity window
--                  does not cover the child's timestamp (coupon/deal-tagged
--                  line items outside valid_from..valid_until).
--
-- Partial-day tolerance: time-bounded tables filter to completed days
-- (col::date < CURRENT_DATE) because the supply-chain block only runs at
-- the hour-0 midnight boundary; a partial backfill day legitimately lacks
-- downstream rows.
--
-- This file is generated from grocery/generator/tests/cross_schema_integrity.py
-- (ASSERTIONS). Do not edit by hand — edit the module and regenerate:
--     python3 grocery/generator/scripts/regen_integrity_sql.py
-- =============================================================================
"""


def main() -> int:
    out = [HEADER]
    last_dimension = None
    for spec in ASSERTIONS:
        if spec.dimension != last_dimension:
            out.append("")
            out.append("-- " + "-" * 74)
            out.append(f"-- {spec.dimension.upper()} — {spec.title}")
            out.append("-- " + "-" * 74)
            last_dimension = spec.dimension
        out.append("")
        out.append(f"-- [{spec.id}] {spec.title}")
        out.append(spec.sql.strip().rstrip(";") + ";")
    out.append("")

    target = os.path.join(GEN, "sql", "check_cross_schema_integrity.sql")
    os.makedirs(os.path.dirname(target), exist_ok=True)
    with open(target, "w", encoding="utf-8") as fh:
        fh.write("\n".join(out))
    print(f"Wrote {target} — {len(ASSERTIONS)} assertions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())