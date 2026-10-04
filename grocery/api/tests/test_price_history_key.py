"""`/grocery/pos/price-history` must serve the row's own key, not a name (t_ea399398).

`pos.price_history.product_id` is `NOT NULL REFERENCES pos.products(product_id)`
and always was — the route joined on it and then dropped it, handing the consumer
`p.name` instead. `pos.products.name` has no uniqueness constraint (only `sku` and
`upc` do), so a consumer forced to re-join that name to recover an id had two
silent failure modes:

* a catalog row that goes away takes every `price_history` row behind it out of the
  response *and* out of reach of every page, while `total` — counted on
  `pos.price_history` alone, without the join — never moves;
* a rename re-points a product's whole price history at whichever product now
  holds that name, and a name collision merges two products' histories outright.

Two guards, in the same split as `test_pagination.py`: a source-level check that
the projection keeps the surrogate key and does not narrow the relation it reads,
and a live check that the key is actually in the payload and that `total` still
equals what the pages can reach.
"""
import pathlib
import re

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
API_SOURCE = REPO_ROOT / "base" / "api" / "main.py"

ROUTE_PATH = "/{industry}/pos/price-history"


def _route_sql() -> str:
    """The SQL of the one route, read out of base/api/main.py."""
    source = API_SOURCE.read_text()
    start = source.index(f'@app.get("{ROUTE_PATH}"')
    end = source.index("\n@app.", start)
    return source[start:end]


def test_route_serves_the_surrogate_key_not_the_name():
    """`product_id` is in the projection, under the name every other route uses."""
    sql = _route_sql()
    assert "ph.product_id" in sql, (
        f"{ROUTE_PATH} no longer selects ph.product_id — consumers are back to "
        f"recovering the key from p.name"
    )
    # Bare `ph.product_id` in the SELECT list, not only in the WHERE filter: the
    # route has always accepted ?product_id=, which would keep the query valid
    # while the payload silently lost the column. Slice from the LAST SELECT —
    # the count query comes first and would otherwise be the one inspected.
    page_sql = sql[sql.rindex("SELECT"):]
    select_list = page_sql[:page_sql.index("FROM")]
    assert "ph.product_id" in select_list, (
        f"{ROUTE_PATH} filters on ph.product_id but does not return it: {select_list.strip()}"
    )


def test_route_does_not_narrow_the_relation_behind_total():
    """`total` counts pos.price_history without the join, so the page query must not
    narrow it either — that is the only way the two can disagree, and they disagree
    silently."""
    sql = _route_sql()
    # Anchor on whitespace: "JOIN pos.products" is a substring of
    # "LEFT JOIN pos.products", so a bare `not in` would reject the fix itself.
    assert not re.search(r"(?<!LEFT )\bJOIN\s+pos\.products\s+p\s+ON", sql), (
        f"{ROUTE_PATH} inner-joins pos.products while its COUNT(*) does not, so a "
        f"product missing from the catalog drops rows from every page while total "
        f"still counts them"
    )
    assert re.search(r"LEFT\s+JOIN\s+pos\.products\s+p\s+ON", sql), (
        f"{ROUTE_PATH} should LEFT JOIN pos.products — the label is optional, the row is not"
    )


def test_route_still_ends_its_order_by_on_the_primary_key():
    """Guards the change against breaking the pagination contract it relies on."""
    match = re.search(r"ORDER BY ([^\n]+)", _route_sql())
    assert match, f"{ROUTE_PATH} has no ORDER BY at all"
    order_by = match.group(1)
    last_key = order_by.split(",")[-1].strip().split()[0]
    assert last_key == "ph.price_history_id", (
        f"{ROUTE_PATH} must break ties on the primary key, got: {order_by.strip()}"
    )


@pytest.mark.usefixtures("ensure_api_reachable")
def test_payload_carries_product_id(quiesced_generator):
    """The live route returns the key, and `total` still matches the pages."""
    client = quiesced_generator
    body = client.get("/grocery/pos/price-history", params={"limit": 5}).json()
    assert body["data"], "price-history returned no rows; nothing to assert"
    for row in body["data"]:
        assert row.get("product_id"), (
            f"a price-history record has no product_id: {row}"
        )
        assert "product_name" in row, (
            f"product_name was dropped rather than kept alongside product_id: {row}"
        )

    # The key must actually filter: asking by a returned product_id has to find that
    # product's own rows, or the column is decorative.
    product_id = body["data"][0]["product_id"]
    scoped = client.get("/grocery/pos/price-history",
                        params={"product_id": product_id, "limit": 1000}).json()
    assert scoped["total"] >= 1, f"product_id={product_id} is not filterable"
    assert {r["product_id"] for r in scoped["data"]} == {product_id}, (
        f"filtering by product_id={product_id} returned other products' rows"
    )

    # ... and the advertised total is what the pages can actually reach.
    total = client.get("/grocery/pos/price-history", params={"limit": 1}).json()["total"]
    seen, offset = [], 0
    while offset < total:
        page = client.get("/grocery/pos/price-history",
                          params={"limit": 1000, "offset": offset}).json()["data"]
        if not page:
            break
        seen.extend(row["price_history_id"] for row in page)
        offset += 1000
    assert len(seen) == total, (
        f"total={total} but paging reached {len(seen)} rows — rows are being "
        f"dropped between the count query and the page query"
    )
    assert len(set(seen)) == len(seen), "a price_history_id came back on two pages"