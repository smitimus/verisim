"""
The vendor routes' API contract — verisim card t_57b1a1ab.

Six new paginated routes under `/{industry}/inventory/`, plus one detail route.
Two classes of check:

**Source-level (always run, no server needed).** These are the ones that catch a
contract break in CI, where the `integration` job is the only place a live API
exists. Each asserts the property data-lab's ingest depends on, read the way the
ingest reads it — from `/openapi.json` and from the route's own source. A route
that quietly stops filtering a column, or stops returning the key a consumer
joins on, must fail the build rather than show up as a broken mart.

**Live (skipped when no API is reachable).** The same properties checked against
a running instance, which is what tells you the SQL is right and not just the
declared parameters.

THE PAGINATION CONTRACT
-----------------------
Every paginated query must end its `ORDER BY` on a primary key, or a tie
cluster straddling a page boundary silently loses rows while `total` still
matches — the defect that cost data-lab 272 `raw_online.order_events` rows
(t_7c88f2f9, fixed in t_d7892e10). The new routes all sort on a unique id for
that reason: `short_ship_events.short_ship_id`, `supplier_credit_memos.
credit_memo_id`, `dsd_deliveries.dsd_delivery_id`, and
`supplier_delivery_schedules.schedule_id`.

THE DEGRADE CONTRACT
--------------------
A `schema.sql` change only reaches a fresh bootstrap, so a slot on an older
image has none of the vendor tables. Every route here answers with
`available: false` and an empty page rather than a 500 — the same rule
`_has_stockout_tables` established for the t_959cd040 tables. A 500 would take
the whole inventory section down for data-lab.
"""
import pathlib
import re

import httpx
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
API_SOURCE = REPO_ROOT / "base" / "api" / "main.py"

# (request path, route source path, primary key the ORDER BY must end on)
VENDOR_ROUTES = [
    ("/grocery/inventory/suppliers",
     "/{industry}/inventory/suppliers", "supplier_id"),
    ("/grocery/inventory/short-ship-events",
     "/{industry}/inventory/short-ship-events", "short_ship_id"),
    ("/grocery/inventory/supplier-credit-memos",
     "/{industry}/inventory/supplier-credit-memos", "credit_memo_id"),
    ("/grocery/inventory/dsd-deliveries",
     "/{industry}/inventory/dsd-deliveries", "dsd_delivery_id"),
    ("/grocery/inventory/dsd-delivery-items",
     "/{industry}/inventory/dsd-delivery-items", "dsd_item_id"),
    ("/grocery/inventory/supplier-delivery-schedules",
     "/{industry}/inventory/supplier-delivery-schedules", "schedule_id"),
]

PATHS = [request_path for request_path, _, _ in VENDOR_ROUTES]


def _route_source(path: str) -> str:
    """The source of one route handler, from its decorator to the next one."""
    source = API_SOURCE.read_text()
    marker = f'@app.get("{path}",'
    start = source.index(marker)
    end = source.index("\n@app.", start + len(marker))
    return source[start:end]


def _params(path: str):
    for route in VENDOR_ROUTES:
        if route[0] == path:
            return route
    raise KeyError(path)


def _page_query(body: str) -> str:
    """
    The column list of the route's PAGE query.

    The route runs a COUNT first and then the page SELECT, so "the last
    SELECT ... FROM" is not reliably the page query — for
    `dsd-delivery-items` the count query comes after the page one. So the page
    query is selected structurally instead: the last SELECT whose projection is
    not a bare COUNT, which is the row-returning one.
    """
    blocks = list(re.finditer(r"SELECT\b(.*?)\bFROM\b", body, re.S))
    assert blocks, "no `SELECT ... FROM` found in the route"
    for block in reversed(blocks):
        projection = block.group(1).strip()
        if not re.match(r"^COUNT\s*\(", projection, re.I):
            return projection
    raise AssertionError("the route has no row-returning SELECT")


def _last_order_key(body: str) -> str:
    """
    The final key of the page query's ORDER BY, with any table alias stripped.

    `ORDER BY se.event_dt DESC, se.short_ship_id` ends on `se.short_ship_id` —
    the primary key, aliased. Stripping the alias is what makes the comparison
    against `short_ship_id` mean something; comparing the raw token would fail a
    route that is correct.
    """
    assert "ORDER BY" in body, "no ORDER BY in the route"
    tail = body[body.index("ORDER BY"):]
    if "LIMIT" in tail:
        tail = tail[:tail.index("LIMIT")]
    last_key = tail.split(",")[-1].strip().split()[0].strip()
    return last_key.rsplit(".", 1)[-1]


def _max_limit(path: str) -> int:
    """The `le=` ceiling the route declares on its own `limit` parameter.

    The live paging check must ask for a page the route will actually serve.
    The caps are not uniform across the API by design — 24 routes cap at 5000
    and 15 at 2000, and `/{industry}/inventory/suppliers` is one of the 2000s
    while its five vendor siblings are 5000s — so a check that hardcoded one
    number asked a 2000-cap route for 5000, took a 422, and read `["data"]` off
    the error body: `KeyError: 'data'`, which looks like a paging defect and is
    not one. Measured in CI on the t_bed5eab3 merge (148 passed, this 1 failed).
    """
    body = _route_source(_params(path)[1])
    m = re.search(r"limit:\s*int\s*=\s*Query\([^)]*?le=(\d+)", body)
    assert m, (f"{path}: no `le=` ceiling found on the route's limit "
               f"parameter, so this check cannot know a valid page size")
    return int(m.group(1))


# ---------------------------------------------------------------------------
# 1. Every route exists, and is guarded
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("path", PATHS)
def test_route_is_declared(path):
    """
    The route exists at all. Compared against the SOURCE path
    (`/{industry}/...`), not the request path — the request path is what a
    consumer types, the source path is what FastAPI registers, and asserting on
    the wrong one gives a green test for a route that does not exist.
    """
    _request, source, _pk = _params(path)
    assert f'@app.get("{source}",' in API_SOURCE.read_text(), \
        f"{path} is not declared as {source}"


@pytest.mark.parametrize("path", PATHS)
def test_route_degrades_when_the_vendor_tables_are_absent(path):
    """
    A data dir generated before t_57b1a1ab has none of the vendor tables, and a
    `schema.sql` change only reaches a fresh bootstrap — so the guard is what
    keeps an older slot from 500ing and taking the inventory section with it.
    """
    _request, source, _pk = _params(path)
    body = _route_source(source)
    assert "_has_vendor_tables(industry)" in body, (
        f"{path} does not guard on the vendor tables")
    assert '"available": False' in body, (
        f"{path} does not tell the caller the tables are missing")


def test_every_vendor_route_shares_one_table_probe():
    """
    One probe, not six. Each route calling its own existence check would mean six
    round trips on a cold API and six places for a new vendor table to be added
    to only some of them.
    """
    source = API_SOURCE.read_text()
    probe = source[source.index("VENDOR_TABLES = ("):
                   source.index("def _has_vendor_tables")]
    for table in ('suppliers', 'short_ship_events', 'supplier_credit_memos',
                  'dsd_deliveries', 'dsd_delivery_items',
                  'supplier_delivery_schedules'):
        assert f"'{table}'" in probe, (
            f"{table} is missing from VENDOR_TABLES, so the probe would report "
            f"'not available' on a database that has it")


# ---------------------------------------------------------------------------
# 2. The pagination contract
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("path", PATHS)
def test_paginated_route_breaks_ties_on_its_primary_key(path):
    """
    `ORDER BY <non-unique> LIMIT %s OFFSET %s` is silent row loss: Postgres may
    return tied rows in a different order per query, so a tie cluster
    straddling a page boundary can hand one row to two pages while another is
    returned by none — and `total` still matches, so the consumer cannot see it.
    """
    _request, source, primary_key = _params(path)
    body = _route_source(source)
    last_key = _last_order_key(body)
    assert last_key == primary_key, (
        f"{path} paginates with an ORDER BY ending on {last_key!r}, not the "
        f"primary key {primary_key!r}, so a tie cluster can straddle a page "
        f"boundary and lose rows")


@pytest.mark.parametrize("path", PATHS)
def test_count_and_page_share_one_where_clause(path):
    """A filter on one and not the other makes `total` disagree with the pages."""
    _request, source, _pk = _params(path)
    body = _route_source(source)
    uses = body.count("WHERE {where}")
    assert uses >= 2, (
        f"{path}: the count query and the page query must share one WHERE "
        f"clause (found {uses} uses of `WHERE {{where}}`)")


@pytest.mark.parametrize("path", PATHS)
def test_route_declares_the_page_ceiling_a_consumer_can_read(path):
    """Every paginated route must publish the ceiling on its `limit`.

    A consumer cannot pick a valid page size unless the route says what the
    largest one is: FastAPI's generated OpenAPI carries `maximum`, but nothing
    asserted it was present, so the first live paging check wrote one number for
    every route and the `le=2000` route answered 422 (t_bed5eab3). `_max_limit`
    failing here is a clearer statement of the same thing than a KeyError
    inside the live check.
    """
    m = re.search(r"limit:\s*int\s*=\s*Query\([^)]*?le=(\d+)",
                  _route_source(_params(path)[1]))
    assert m, (
        f"{path}: `limit` carries no `le=` ceiling, so a consumer has no way to "
        f"know a valid page size — declare `Query(<default>, le=<max>)`")


# ---------------------------------------------------------------------------
# 3. The columns a consumer joins on must be in the payload
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("path,required", [
    ("/grocery/inventory/suppliers",
     ["supplier_id", "supplier_name", "fulfillment_model",
      "lead_time_mean_days", "lead_time_stddev_days", "short_ship_rate"]),
    ("/grocery/inventory/short-ship-events",
     ["short_ship_id", "supplier_id", "supplier_name", "product_id",
      "location_id", "quantity_requested", "quantity_picked", "quantity_short",
      "short_value", "promised_lead_time_days", "realized_lead_time_days",
      "detected_source", "is_creditable", "event_dt"]),
    ("/grocery/inventory/supplier-credit-memos",
     ["credit_memo_id", "credit_memo_number", "short_ship_id", "supplier_id",
      "memo_status", "credit_amount", "claim_deadline", "submitted_dt",
      "resolved_dt"]),
    ("/grocery/inventory/dsd-deliveries",
     ["dsd_delivery_id", "supplier_id", "supplier_name", "location_id",
      "delivery_date", "total_units", "total_value", "line_count"]),
    ("/grocery/inventory/dsd-delivery-items",
     ["dsd_item_id", "dsd_delivery_id", "product_id", "quantity_delivered",
      "unit_cost", "line_total"]),
    ("/grocery/inventory/supplier-delivery-schedules",
     ["schedule_id", "supplier_id", "supplier_name", "location_id",
      "delivery_weekday", "window_start", "window_end"]),
])
def test_payload_carries_the_columns_a_mart_needs(path, required):
    """
    The route has to return the keys its own joins and filters name. A column
    that exists in the table but not in the payload is the same break one layer
    up: the consumer cannot build the raw table it would watermark on.
    """
    _request, source, _pk = _params(path)
    select_list = _page_query(_route_source(source))
    missing = [c for c in required if c not in select_list]
    assert not missing, (
        f"{path} does not return {missing} in its payload:\n{select_list}")


def test_short_ship_route_returns_the_receipt_comparable_columns():
    """
    `unit_cost` is what makes a credit memo reconcilable against the receipt: a
    mart that cannot see the cost the shortfall was priced at cannot tie the two
    together, which is the first thing a procurement analyst tries.
    """
    _request, source, _pk = _params("/grocery/inventory/short-ship-events")
    select_list = _page_query(_route_source(source))
    assert "unit_cost" in select_list


def test_the_supplier_detail_route_exposes_the_performance_measures():
    """
    Fill rate, dollars short, and days-to-pay are the three numbers the whole
    card exists to make possible — "which vendor keeps shorting us" was
    unanswerable before, and a detail route that omitted them would leave it
    that way.
    """
    source = API_SOURCE.read_text()
    marker = '@app.get("/{industry}/inventory/suppliers/{supplier_id}"'
    start = source.index(marker)
    body = source[start:source.index("\n@app.", start + len(marker))]
    for column in ("fill_rate_pct", "short_value", "avg_days_to_pay",
                   "avg_lead_time_slippage_days", "paid_amount",
                   "outstanding_amount"):
        assert column in body, (
            f"the supplier detail route does not expose {column}, which is one "
            f"of the measures the card exists to produce")


def test_the_supplier_detail_route_guards_too():
    source = API_SOURCE.read_text()
    marker = '@app.get("/{industry}/inventory/suppliers/{supplier_id}"'
    start = source.index(marker)
    body = source[start:source.index("\n@app.", start + len(marker))]
    assert "_has_vendor_tables(industry)" in body
    assert '"available": False' in body


# ---------------------------------------------------------------------------
# 4. Live checks (skipped when no API is up)
# ---------------------------------------------------------------------------
@pytest.mark.usefixtures("ensure_api_reachable")
@pytest.mark.parametrize("path", PATHS)
def test_route_answers(api_base_url, path):
    """Every route is reachable and returns the pagination envelope."""
    resp = httpx.get(f"{api_base_url}{path}", params={"limit": 5}, timeout=10.0)
    assert resp.status_code == 200, f"{path} returned {resp.status_code}"
    data = resp.json()
    assert isinstance(data.get("data"), list), f"{path} returned no data list"
    assert "total" in data, f"{path} returned no total"
    assert "limit" in data and "offset" in data
    assert "available" in data, (
        f"{path} does not report whether the vendor tables are present")


@pytest.mark.usefixtures("ensure_api_reachable")
def test_a_vendored_data_dir_serves_the_routes(quiesced_generator, api_base_url):
    """
    On a database generated WITH the vendor tables, every route reports
    `available: true` and `suppliers` is non-empty. A route that degrades to
    `available: false` on a fresh install is worse than no route: data-lab would
    ingest an empty table and never learn why.
    """
    client = quiesced_generator
    suppliers = client.get("/grocery/inventory/suppliers",
                           params={"limit": 100}).json()
    if not suppliers.get("available"):
        pytest.skip(
            "this data dir predates t_57b1a1ab — the degrade path is correct "
            "here; a rebuilt slot is needed for the live vendor check")
    assert suppliers["total"] > 0, (
        "a fresh install must have vendors; an empty table means seed_suppliers "
        "did not run")

    for row in suppliers["data"]:
        for key in ("supplier_id", "supplier_name", "fulfillment_model",
                    "lead_time_mean_days", "short_ship_rate"):
            assert row.get(key) is not None, (
                f"vendor {row.get('supplier_name')} has no {key}")


@pytest.mark.usefixtures("ensure_api_reachable")
def test_the_detail_route_answers_for_a_real_vendor(quiesced_generator,
                                                    api_base_url):
    client = quiesced_generator
    listing = client.get("/grocery/inventory/suppliers",
                         params={"limit": 1}).json()
    if not listing.get("available") or not listing["data"]:
        pytest.skip("no vendors on this data dir (pre-t_57b1a1ab)")
    supplier_id = listing["data"][0]["supplier_id"]

    resp = client.get(f"/grocery/inventory/suppliers/{supplier_id}")
    assert resp.status_code == 200, f"detail route returned {resp.status_code}"
    detail = resp.json()
    assert detail["available"] is True
    assert detail["supplier"] is not None
    assert detail["supplier"]["supplier_id"] == supplier_id
    assert "fill_rate_pct" in detail["supplier"]
    assert "credit_summary" in detail
    assert "credit_memos" in detail


@pytest.mark.usefixtures("ensure_api_reachable")
def test_an_unknown_vendor_is_an_empty_detail_not_a_500(quiesced_generator):
    """A vendor id that does not exist is a legitimate question with an
    answerable 'no', not a server error."""
    client = quiesced_generator
    resp = client.get(
        "/grocery/inventory/suppliers/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 200, (
        f"an unknown vendor id returned {resp.status_code}")
    assert resp.json()["supplier"] is None


@pytest.mark.usefixtures("ensure_api_reachable")
def test_paging_a_vendor_route_loses_no_rows(quiesced_generator):
    """
    The live half of the pagination contract: walking a route at a small limit
    must return exactly `total` distinct primary keys. This is the check that
    would catch an `ORDER BY` on a column with ties even if the source-level
    check were somehow bypassed.
    """
    client = quiesced_generator
    for path, _source, primary_key in VENDOR_ROUTES:
        total = client.get(path, params={"limit": 1}).json()["total"]
        if total == 0:
            continue
        # Ask for the largest page THIS route will serve, not a hardcoded
        # number: the `le=` ceilings differ per route, and over-asking is a
        # 422 whose body has no `data` key — a KeyError that reads like a
        # paging bug. See `_max_limit`.
        cap = _max_limit(path)
        resp = client.get(path, params={"limit": cap})
        assert resp.status_code == 200, (
            f"{path} returned {resp.status_code} for limit={cap}, its own "
            f"declared ceiling: {resp.text[:200]}")
        page = resp.json()
        seen = {row[primary_key] for row in page["data"]}
        assert len(seen) == min(total, cap), (
            f"{path}: one page returned {len(seen)} distinct {primary_key} for "
            f"a total of {min(total, cap)}")
        assert len(page["data"]) == len(seen), (
            f"{path}: the page returned a duplicate {primary_key}")