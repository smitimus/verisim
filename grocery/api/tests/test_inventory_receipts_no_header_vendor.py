"""
`/grocery/inventory/receipts` must NOT serve a header vendor — kanban t_7d966a37.

WHY THIS IS A TEST OF AN ABSENCE
--------------------------------
t_ed802fa6 fixed `/inventory/products` by projecting `ip.supplier_id`, and the natural
symmetry was to do the same one route over: `inv.receipts.supplier_id` is declared in
`schema.sql`, indexed by `idx_inv_receipts_supplier`, and referenced by data-lab's
`stg_inv_receipts` compile-time probe. But a receipt header has no single vendor to
hold, and nothing writes the column:

* the only grocery writer is `transport.receive_delivered_loads`, and its INSERT is
  `(receipt_id, location_id, received_dt, po_number, load_id, total_cost)` — no
  `supplier_id` *and no* `supplier_name`;
* a DSD drop deliberately writes no receipt at all (`suppliers.generate_dsd_deliveries`
  — its vendor is on `inv.dsd_deliveries`), so there is no DSD receipt path either;
* and a receipt is one of OUR warehouse loads, whose goods are the PRODUCTS' vendors'.
  Measured on a fresh 30-day backfill (image `3436724c`, 2026-10-25): 78 receipts,
  **0** with a non-NULL `supplier_id`, **0** with a non-NULL `supplier_name`; 75 of the
  78 spanned 2–5 vendors, the other 3 spanned exactly one by coincidence of that day's
  pick list. Four independent 8-day seeds agreed (0 of 9 each).

Projecting the column would therefore add a landzone column that is NULL on every row
and, worse, would 500 the other industries: the route SQL in `base/api/main.py` is
shared, and `gas-station/generator/schema.sql`, `base/db/init.sql` and
`base/db/03_grocery_schema.sql` all define `inv.receipts` WITHOUT `supplier_id`.

`supplier_name` stays in the projection: gas-station's `inventory.check_and_restock`
populates it (one receipt per vendor per location), and grocery leaving it NULL is the
honest shape, not a regression.

Vendor attribution lives on the LINE: every `inv.receipt_items` row reaches a vendor
through `product_id -> inv.products.supplier_id` (18,539 of 18,539 lines on that same
backfill). That is the join data-lab should use, and the reason a header column is not
needed.

If a future card gives receipts a genuine single-vendor path, these tests fail on
purpose: the decision has to be re-made deliberately (and the gas-station/base schemas
given the column first), not inherited from the products route.
"""
import pathlib
import re

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
API_SOURCE = REPO_ROOT / "base" / "api" / "main.py"
MODELS_DIR = REPO_ROOT / "grocery" / "generator" / "models"

RECEIPTS_SOURCE_PATH = "/{industry}/inventory/receipts"
RECEIPTS_REQUEST_PATH = "/grocery/inventory/receipts"

#: The route's whole payload contract, asserted as a set. `supplier_id` is absent by
#: design (see the module docstring); `supplier_name` is present for gas-station.
PAYLOAD_KEYS = {
    "receipt_id", "load_id", "location_id", "received_dt", "supplier_name",
    "po_number", "total_cost", "line_items",
}


def _route_source(path: str) -> str:
    """The source of one route handler, from its decorator to the next one."""
    source = API_SOURCE.read_text()
    marker = f'@app.get("{path}",'
    start = source.index(marker)
    return source[start:source.index("\n@app.", start + len(marker))]


def _page_projection(body: str) -> str:
    """The column list of the route's row-returning SELECT."""
    blocks = list(re.finditer(r"SELECT\b(.*?)\bFROM\b", body, re.S))
    assert blocks, "no `SELECT ... FROM` found in the receipts route"
    for block in reversed(blocks):
        projection = block.group(1).strip()
        if not re.match(r"^COUNT\s*\(", projection, re.I):
            return projection
    raise AssertionError("the receipts route has no row-returning SELECT")


def _receipt_writers():
    """Every grocery model module that inserts a row into `inv.receipts`."""
    return sorted(
        path for path in MODELS_DIR.glob("*.py")
        if re.search(r"INSERT INTO inv\.receipts\b", path.read_text(), re.I)
    )


# ---------------------------------------------------------------------------
# 1. Source-level: the route does not project a column of NULLs
# ---------------------------------------------------------------------------
def test_the_receipts_route_does_not_project_a_header_vendor():
    """
    A header `supplier_id` would be NULL on every row AND would break the industries
    whose `inv.receipts` has no such column, because this route SQL is shared.
    """
    projection = _page_projection(_route_source(RECEIPTS_SOURCE_PATH))
    assert "supplier_id" not in projection, (
        "`/grocery/inventory/receipts` projects a `supplier_id`, but no grocery writer "
        "populates `inv.receipts.supplier_id` (0 of 78 rows on a fresh 30-day "
        "backfill) and `gas-station/generator/schema.sql`, `base/db/init.sql` and "
        "`base/db/03_grocery_schema.sql` define the table without that column — the "
        "shared route would 500 those data dirs. Vendor attribution is per LINE "
        "(`inv.receipt_items -> inv.products.supplier_id`); see kanban t_7d966a37.\n"
        f"Projection was:\n{projection}")


def test_the_receipts_route_keeps_the_supplier_name_display_copy():
    """
    `supplier_name` is populated by gas-station's restock path (one receipt per vendor
    per location), so it must survive even though grocery leaves it NULL.
    """
    projection = _page_projection(_route_source(RECEIPTS_SOURCE_PATH))
    for column in ("r.supplier_name", "r.load_id", "r.po_number", "r.total_cost"):
        assert column in projection, (
            f"`/grocery/inventory/receipts` stopped projecting `{column}`; "
            "`supplier_name` is gas-station's vendor key on a receipt and `load_id` is "
            "the receipt -> transport.loads join")


# ---------------------------------------------------------------------------
# 2. Source-level: the writer leaves both vendor columns unset
# ---------------------------------------------------------------------------
def test_the_receipt_writer_inserts_no_vendor_column():
    """
    Pins the shape `transport.receive_delivered_loads` writes, so the premise of
    t_7d966a37 ("written in the same INSERT as supplier_name") cannot come back
    unexamined. A warehouse load's goods belong to the products' vendors, not to one
    vendor for the pallet.
    """
    writers = _receipt_writers()
    assert [path.name for path in writers] == ["transport.py"], (
        "expected exactly one grocery writer of `inv.receipts` "
        f"(transport.py); found {[p.name for p in writers]}. A second writer means "
        "the header may carry a vendor after all — re-read kanban t_7d966a37 and the "
        "cross-schema map §4 before projecting it.")

    source = writers[0].read_text()
    match = re.search(r"INSERT INTO inv\.receipts\s*\((.*?)\)\s*VALUES", source,
                      re.S | re.I)
    assert match, "could not find the `INSERT INTO inv.receipts (...)` column list"
    columns = {col.strip() for col in match.group(1).split(",")}
    assert columns == {"receipt_id", "location_id", "received_dt", "po_number",
                       "load_id", "total_cost"}, (
        "the grocery receipt INSERT changed shape: "
        f"{sorted(columns)}. `supplier_id` / `supplier_name` are deliberately absent — "
        "a receipt is a warehouse load spanning several vendors (t_7d966a37)")


def test_the_dsd_drop_still_writes_no_receipt():
    """
    The other channel. If a DSD drop ever became a receipt, it *would* have one vendor
    and the header column would suddenly be meaningful — so this is the test to fail
    first if that design changes.
    """
    suppliers = (MODELS_DIR / "suppliers.py").read_text()
    match = re.search(r'def generate_dsd_deliveries\(.*?\n    """(.*?)"""',
                      suppliers, re.S)
    assert match, "could not find `generate_dsd_deliveries`'s docstring"
    assert "no `inv.receipts` row" in match.group(1), (
        "`generate_dsd_deliveries` no longer documents that a DSD drop writes no "
        "receipt; if it started writing one, `inv.receipts.supplier_id` has a single "
        "vendor to carry and this decision must be re-made (kanban t_7d966a37)")
    assert not re.search(r"INSERT INTO inv\.receipts\b", suppliers, re.I), (
        "`suppliers.py` now inserts into `inv.receipts` — a DSD receipt is a "
        "single-vendor receipt, which is the case that would justify the header "
        "column. Re-open kanban t_7d966a37 instead of extending this guard.")


# ---------------------------------------------------------------------------
# 3. Live: the payload keys are exactly the contract
# ---------------------------------------------------------------------------
@pytest.mark.usefixtures("ensure_api_reachable")
def test_the_receipts_payload_carries_exactly_the_contracted_keys(
        quiesced_generator, api_base_url):
    """
    `raw_inv.receipts` gains and loses a column exactly as this key set changes, so
    data-lab's staging model is built against this list. The key set is asserted (not
    "contains") so a later card cannot add `supplier_id` without deliberately
    updating this test — and the reason it is absent is in the module docstring.

    The route returns the `{data, total, limit, offset}` envelope its siblings use
    (t_2ab8e903) — it was the last receipt-shaped route answering with a bare JSON
    array — so the rows come out of `data`.
    """
    client = quiesced_generator
    body = client.get(RECEIPTS_REQUEST_PATH, params={"limit": 1}).json()
    assert isinstance(body, dict) and isinstance(body.get("data"), list), (
        "the receipts route no longer returns the {data,total,limit,offset} "
        "envelope; the payload shape is part of what data-lab's raw table is built "
        "from (t_2ab8e903)")
    rows = body["data"]
    assert rows and body["total"] >= len(rows), (
        "the receipts route returned no rows, or a `total` smaller than the page it "
        "just served")
    keys = set(rows[0])
    assert keys == PAYLOAD_KEYS, (
        f"the receipts payload keys changed: missing {sorted(PAYLOAD_KEYS - keys)}, "
        f"unexpected {sorted(keys - PAYLOAD_KEYS)}")


# ---------------------------------------------------------------------------
# 4. Source-level: the route can be paged and counted (t_2ab8e903)
# ---------------------------------------------------------------------------
def _order_by_clause(body: str) -> str:
    """The last ORDER BY clause in the route body, up to its LIMIT."""
    clauses = re.findall(r"ORDER BY\s+(.*?)(?=\s*(?:\"\"\"|\n|LIMIT|\)))", body, re.S)
    assert clauses, "the receipts route has no ORDER BY"
    return clauses[-1].strip()


def test_the_receipts_route_declares_offset_and_returns_a_total():
    """
    Offline (no container), so CI enforces it: the route was the one
    receipt-shaped route a client could not page — `limit` capped at 1000, no
    `offset`, and a bare JSON array with no `total` — so data-lab landed the
    1,000-row page as a whole 32-day window and orphaned ~297k `receipt_items`
    every night (data-lab t_6b215e70).

    Both levers are asserted here because either one alone is not enough: `total`
    without `offset` still cannot reach row 1,001, and `offset` without `total`
    cannot tell a complete window from a truncated one.
    """
    body = _route_source(RECEIPTS_SOURCE_PATH)

    signature = body[body.index("def inventory_receipts("):body.index("):", body.index(
        "def inventory_receipts("))]
    assert re.search(r"\boffset\s*:\s*int\s*=\s*0", signature), (
        "`/inventory/receipts` no longer declares `offset: int = 0`; a client "
        f"cannot page past `limit` without it (t_2ab8e903).\nSignature was:\n{signature}")
    assert "OFFSET %s" in body, (
        "`/inventory/receipts` slices with `LIMIT %s` and no `OFFSET`; the declared "
        "`offset` parameter would be accepted and silently ignored (measured: "
        "`limit=1000&offset=1000` returned the same rows as `offset=0`, t_2ab8e903)")
    assert re.search(r'"total"\s*:\s*total\b', body), (
        "`/inventory/receipts` no longer returns `total`; without it a client "
        "cannot tell a complete window from a truncated page (t_2ab8e903)")
    # The count has to be for the same WHERE — a `COUNT(*)` over the joined rows
    # would report one per line item instead of one per receipt.
    count_sql = re.search(r'SELECT\s+COUNT\(\*\)[^"]*?FROM\s+(\S+)', body, re.S)
    assert count_sql and count_sql.group(1) == "inv.receipts", (
        "the receipts `total` no longer counts `inv.receipts` rows; the row query "
        "groups by `r.receipt_id`, so counting the join would multiply by line "
        "count (t_2ab8e903)")


def test_the_receipts_page_order_ends_on_the_primary_key():
    """
    `received_dt` is not unique — on the test data dir all 7,232 receipts share
    their timestamp with at least one other across 85 distinct values, one day
    holding 711 of them — so paging it was measured losing 191 rows to duplicate
    pages while the count still matched (the t_d7892e10 signature).

    `test_pagination.py` enforces this for every `OFFSET %s` query; this asserts
    it for the route by name, because the route had no `offset` when that scan
    was written and so was the one it could not see.
    """
    clause = _order_by_clause(_route_source(RECEIPTS_SOURCE_PATH))
    last_key = clause.split(",")[-1].strip().split()[0]
    assert last_key.split(".")[-1] == "receipt_id", (
        f"`/inventory/receipts` pages on `{clause}`, which does not end on the "
        "primary key; a tie cluster straddling a page boundary returns one row "
        "twice and another never, while `total` still matches (t_d7892e10)")


