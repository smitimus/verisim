"""
`/grocery/inventory/products` must serve `inv.products.supplier_id` — kanban
t_ed802fa6.

WHY THIS IS AN API TEST AND NOT A SQL ONE
-----------------------------------------
The API payload *is* data-lab's raw landzone schema: `grocery_ingest_api._ensure_table`
builds `raw_inv.products` from the JSON keys of the first page (and
`_detect_schema_drift` ALTERs a column in when the API later grows one). A column
that `schema.sql` declares, that the generator writes, and that the route omits
therefore **does not exist downstream** — no error, no warning at the route, just a
staging model that probes for it, emits `null::text` and marks itself degraded.

That is exactly what happened here: `inv.products.supplier_id` has been a real FK
since t_57b1a1ab, the generator writes it in the same statement as the denormalised
`supplier_name`, and this route did not project it. Consequence measured on CT107
(2026-10-25, image `be940296`): `raw_inv.products` had no `supplier_id` column at
all, `stg_inv_products.supplier_id_available` was `false` on 495 of 495 rows, and
`mart_vendor_performance.product_count` was **0 on 7 of 7 vendors** — a published
column that was structurally empty because the product→vendor link fell back to a
name string that cannot carry an FK.

So the assertions below are the two properties data-lab depends on:

1. **The key is in the payload.** Read from the route's own source, so the check
   runs in CI with no server (the `integration` job is the only place a live API
   exists), plus the live version that proves the SQL executes.
2. **The value is the vendor key, not a copy of the name.** Every row that carries
   a `supplier_id` must resolve to a vendor in `/{industry}/inventory/suppliers`,
   and that vendor's `supplier_name` must be the row's `supplier_name` — the
   pairing harness SEMA-27 asserts at the source, carried through the payload. A
   route that projected `NULL::text AS supplier_id`, or a stale copied id, passes
   a key-presence check and fails this one.

A NULL `supplier_id` is a legitimate state (a product with no known vendor), so the
live test does not demand one on every row; it asserts the pairing wherever the
column is populated.
"""
import pathlib
import re

import httpx
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
API_SOURCE = REPO_ROOT / "base" / "api" / "main.py"

PRODUCTS_SOURCE_PATH = "/{industry}/inventory/products"
PRODUCTS_REQUEST_PATH = "/grocery/inventory/products"
SUPPLIERS_REQUEST_PATH = "/grocery/inventory/suppliers"

#: The route's whole payload contract. Asserted as a set, not as "contains", so a
#: later card cannot quietly drop one of these while adding another — the key count
#: is what data-lab's raw table gains or loses.
PAYLOAD_KEYS = {
    "inv_product_id", "product_id", "product_name", "category", "sku",
    "reorder_point", "reorder_qty", "unit_of_measure",
    "supplier_id", "supplier_name", "lead_time_days",
}


def _route_source(path: str) -> str:
    """The source of one route handler, from its decorator to the next one."""
    source = API_SOURCE.read_text()
    marker = f'@app.get("{path}",'
    start = source.index(marker)
    return source[start:source.index("\n@app.", start + len(marker))]


def _page_projection(body: str) -> str:
    """
    The column list of the route's PAGE query.

    The route runs a COUNT first and then the page SELECT, so the page query is
    selected structurally: the last `SELECT ... FROM` whose projection is not a
    bare `COUNT(`. Mirrors `test_vendor_routes._page_query`, deliberately kept
    local so this file fails for its own reason.
    """
    blocks = list(re.finditer(r"SELECT\b(.*?)\bFROM\b", body, re.S))
    assert blocks, "no `SELECT ... FROM` found in the products route"
    for block in reversed(blocks):
        projection = block.group(1).strip()
        if not re.match(r"^COUNT\s*\(", projection, re.I):
            return projection
    raise AssertionError("the products route has no row-returning SELECT")


# ---------------------------------------------------------------------------
# 1. Source-level: the key is in the route's own SELECT (runs with no API)
# ---------------------------------------------------------------------------
def test_the_products_route_projects_the_vendor_key():
    """
    `inv.products.supplier_id` is the only column the marts can join to a vendor on.
    `supplier_name` is a denormalised display copy — kept, and still projected, but
    it cannot carry an FK and stops being 1:1 the day a vendor is renamed.

    Guarded, not bare: this route SQL is shared by every industry and
    `inv.products.supplier_id` is grocery's alone (t_04e1e8be), so the column is
    named only where `_has_column` says the data dir has it.
    """
    body = _route_source(PRODUCTS_SOURCE_PATH)
    projection = _page_projection(body)
    assert "_has_column(industry, \"inv\", \"products\", \"supplier_id\")" in body, (
        "`/grocery/inventory/products` projects `ip.supplier_id` without probing for "
        "it. `gas-station/generator/schema.sql`, `base/db/init.sql` and "
        "`base/db/03_grocery_schema.sql` define `inv.products` WITHOUT that column, "
        "and the route SQL is shared, so an unguarded projection is a parse-time "
        "`42703` — HTTP 500 on every gas-station data dir (t_04e1e8be). "
        f"Route source was:\n{body}")
    assert "ip.supplier_id" in body, (
        "`/grocery/inventory/products` does not project `ip.supplier_id` at all. The "
        "payload is data-lab's raw landzone schema, so the column will not exist in "
        "`raw_inv.products` and `mart_vendor_performance.product_count` stays 0. "
        f"Projection was:\n{projection}")


def test_the_products_route_still_projects_the_name_fallback():
    """
    The column is additive. `supplier_name` is the fallback data-lab's staging model
    and `assert_store_orders_exclude_dsd_vendor_skus` use while a slot's source
    predates the FK, so it has to stay in the payload alongside the id.
    """
    projection = _page_projection(_route_source(PRODUCTS_SOURCE_PATH))
    for column in ("ip.supplier_name", "ip.lead_time_days"):
        assert column in projection, (
            f"`/grocery/inventory/products` stopped projecting `{column}`; the "
            "vendor fallback and the lead-time mart both read it")


# ---------------------------------------------------------------------------
# 2. Live: the value is the vendor key and agrees with the name
# ---------------------------------------------------------------------------
@pytest.mark.usefixtures("ensure_api_reachable")
def test_the_products_payload_carries_exactly_the_contracted_keys(
        quiesced_generator, api_base_url):
    """
    Key count, not just key presence: `raw_inv.products` gains and loses columns as
    this list changes, and data-lab's staging model is built against the list.

    On a data dir with no vendor layer (`inv.products.supplier_id` does not exist —
    a slot on an image older than t_57b1a1ab, or a non-grocery industry) the route
    omits the key rather than 500ing on it (t_04e1e8be); the suppliers route
    reports that absence as `available: false`, so that is the skip condition.
    """
    client = quiesced_generator
    body = client.get(PRODUCTS_REQUEST_PATH, params={"limit": 1}).json()
    assert body["total"] > 0, "the products route returned no rows"
    keys = set(body["data"][0])
    if "supplier_id" not in keys:
        suppliers = client.get(SUPPLIERS_REQUEST_PATH, params={"limit": 1}).json()
        if not suppliers.get("available"):
            pytest.skip("no vendor layer on this data dir — `inv.products` has no "
                        "`supplier_id`, so the route omits the key by design")
    assert keys == PAYLOAD_KEYS, (
        f"the products payload keys changed: missing {sorted(PAYLOAD_KEYS - keys)}, "
        f"unexpected {sorted(keys - PAYLOAD_KEYS)}")


@pytest.mark.usefixtures("ensure_api_reachable")
def test_every_product_supplier_id_is_a_real_vendor(quiesced_generator,
                                                    api_base_url):
    """
    The projected id must be the FK the generator wrote, not a placeholder: every
    non-null value resolves to a row of `/{industry}/inventory/suppliers`.

    On a data dir that predates the vendor layer the suppliers route degrades to
    `available: false`; the pairing then cannot be checked and the test skips —
    the source-level checks above still guard the projection.
    """
    client = quiesced_generator
    suppliers = client.get(SUPPLIERS_REQUEST_PATH,
                           params={"limit": 2000}).json()
    if not suppliers.get("available") or not suppliers["data"]:
        pytest.skip("no vendor layer on this data dir (pre-t_57b1a1ab)")

    vendors = {row["supplier_id"]: row["supplier_name"]
               for row in suppliers["data"]}
    page = client.get(PRODUCTS_REQUEST_PATH, params={"limit": 2000}).json()
    assert page["data"], "the products route returned no rows"

    populated = [row for row in page["data"] if row.get("supplier_id") is not None]
    assert populated, (
        "not one product carries a supplier_id — the generator writes it for every "
        "assigned SKU, so an all-NULL payload means the route projects a constant")

    orphans = sorted({row["supplier_id"] for row in populated} - set(vendors))
    assert not orphans, (
        f"{len(orphans)} product supplier_id(s) do not resolve to a vendor: "
        f"{orphans[:5]}")

    disagreements = [
        (row["inv_product_id"], row["supplier_id"], row["supplier_name"],
         vendors[row["supplier_id"]])
        for row in populated
        if row.get("supplier_name") != vendors[row["supplier_id"]]
    ]
    assert not disagreements, (
        "supplier_id and supplier_name disagree on "
        f"{len(disagreements)} of {len(populated)} rows (source invariant SEMA-27): "
        f"{disagreements[:5]}")
