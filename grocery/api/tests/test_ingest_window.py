"""The ingest-time window on the backdated grocery tables (t_5d2e2ab0, t_6d2ebc52).

``pos.transactions.transaction_dt``, ``pos.returns.return_dt`` and
``online.orders.placed_dt`` are all *backdating* columns — the generator writes
rows whose business time is in the past:

* ``return_dt = transaction_dt + random(2..21) days`` (clamped to the simulated
  now), so a nightly batch inserted at one instant carries business timestamps
  spread over the previous month. Measured on the dev seed (9185 rows): 4055 sat
  more than a day below the batch's own clamp value, 2072 more than a week.
* ``transaction_dt`` and ``placed_dt`` are backdated by a *backfill* (a whole day
  written at one instant) and by a gap-fill of days older than the stored ones.
  On the 2026-09-21 dev seed, 89320 of 92741 transactions and 10699 of 11062
  orders sat more than a day below their table's own last insert.

A consumer's watermark is ``MAX(<business column>)``, which sits above every row
a *later* batch backdates below it, so no ``start_dt``/``end_dt`` combination can
drive an incremental load of those tables — data-lab's nightly ingest silently
lost 6 of 95015 transactions that way and a dbt relationships test went WARN.

The fix is a second window on the insert clock: ``created_after``/``created_before``
bound the header's ``created_at`` (``DEFAULT NOW()``, written by the same statement
as the row), which is monotone and can only move forward. The line tables
(``pos.transaction_items``, ``pos.return_items``, ``online.order_items``) have no
timestamp of their own, so ``created_at`` is taken from their header — and it is
returned in the payload, because a consumer needs that column in its own raw table
to hold the watermark.

These tests guard the contract data-lab's ingest depends on:
``grocery_ingest_api.py`` names the window parameters in ``TABLE_CONFIGS`` and
verifies them against ``/openapi.json`` before it trusts them, so the parameters
must be declared on the route (not merely accepted) and must really filter the
ingest clock in *both* the count query and the page query.
"""
import pathlib
import re
from datetime import datetime, timedelta, timezone

import httpx
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
API_SOURCE = REPO_ROOT / "base" / "api" / "main.py"

FAR_PAST = "2000-01-01T00:00:00+00:00"
FAR_FUTURE = "2999-01-01T00:00:00+00:00"
ONE_SECOND = "1 second"

# ``/grocery/pos/transactions`` used to *require* start_dt/end_dt (they bound
# transaction_dt) and now takes both windows optionally, so the ingest can ask for the
# insert clock alone; the source contract test below pins that.
BUSINESS_WINDOW = {"start_dt": FAR_PAST, "end_dt": FAR_FUTURE}

# (request path, route source path, header alias)
INGEST_WINDOW_ROUTES = [
    ("/grocery/pos/transactions", "/{industry}/pos/transactions", "t"),
    ("/grocery/pos/transaction-items", "/{industry}/pos/transaction-items", "t"),
    ("/grocery/online/orders", "/grocery/online/orders", "o"),
    ("/grocery/online/order-items", "/grocery/online/order-items", "o"),
    ("/grocery/pos/returns", "/grocery/pos/returns", "r"),
    ("/grocery/pos/return-items", "/grocery/pos/return-items", "r"),
]

PATHS = [request_path for request_path, _, _ in INGEST_WINDOW_ROUTES]


def _route_source(path: str) -> str:
    """The source of one route handler, from its decorator to the next one."""
    source = API_SOURCE.read_text()
    marker = f'@app.get("{path}",'
    start = source.index(marker)
    end = source.index("\n@app.", start + len(marker))
    return source[start:end]


def _params(path: str):
    for route in INGEST_WINDOW_ROUTES:
        if route[0] == path:
            return route
    raise KeyError(path)


def _page_select_list(body: str) -> str:
    """The column list of the route's *page* query (the last SELECT ... FROM).

    The shared POS routes build their column list in a ``select`` variable, so any
    ``select = "..."`` assignment in the body is folded in as well.
    """
    blocks = list(re.finditer(r"SELECT\b(.*?)\bFROM\b", body, re.S))
    assert blocks, "no `SELECT ... FROM` found in the route"
    select_list = blocks[-1].group(1)
    select_list += "".join(re.findall(r'select = "([^"]*)"', body))
    return select_list


@pytest.mark.parametrize("path", PATHS)
def test_route_declares_the_ingest_window_on_created_at(path):
    """The window must be declared (so OpenAPI advertises it) and applied to the
    header's ``created_at`` in the count query and the page query alike — a filter
    on one and not the other makes ``total`` disagree with the pages."""
    request_path, source_path, alias = _params(path)
    body = _route_source(source_path)

    for param in ("created_after", "created_before"):
        assert re.search(rf"^\s+{param}: Optional\[datetime\] = None,$", body, re.M), \
            f"{request_path}: does not declare {param}"

    assert f'filters.append("{alias}.created_at >= %s"); params.append(created_after)' in body, \
        f"{request_path}: created_after is not applied to {alias}.created_at"
    assert f'filters.append("{alias}.created_at <= %s"); params.append(created_before)' in body, \
        f"{request_path}: created_before is not applied to {alias}.created_at"

    # Same WHERE clause for the count and the rows, so `total` cannot drift.
    uses = body.count("WHERE {where}")
    assert uses >= 2, (
        f"{request_path}: the count query and the page query must share one WHERE clause "
        f"(found {uses} uses of `WHERE {{where}}`)"
    )


@pytest.mark.parametrize("path", PATHS)
def test_payload_carries_created_at(path):
    """data-lab watermarks on MAX(created_at) *of the raw table it built from this
    payload*, so the column has to be in the response.

    The line tables have no timestamp of their own and would otherwise have no
    monotone column at all; the header routes need it for the same reason on the
    join path (``pos.transaction_items`` / ``online.order_items`` are loaded from
    the source's own rows, but the watermark column has to exist in the raw table).
    """
    request_path, source_path, alias = _params(path)
    select_list = _page_select_list(_route_source(source_path))
    assert f"{alias}.created_at" in select_list, (
        f"{request_path} does not return {alias}.created_at in its payload:\n{select_list}"
    )


def test_pos_transactions_answers_the_insert_clock_alone():
    """``/grocery/pos/transactions`` used to *require* ``start_dt``/``end_dt``.

    A consumer whose only watermark is the insert clock must be able to ask for it
    without inventing a business window — otherwise the delta request is a 422, which
    is what t_886f7d67 would have hit. Both windows are optional and independent, and
    ``start_dt``/``end_dt`` still filter ``transaction_dt``.
    """
    request_path, source_path, _ = _params("/grocery/pos/transactions")
    body = _route_source(source_path)

    for param in ("start_dt", "end_dt"):
        assert re.search(rf"^\s+{param}: Optional\[datetime\] = None,$", body, re.M), \
            f"{request_path}: {param} is not optional"

    assert 'filters.append("t.transaction_dt >= %s"); params.append(start_dt)' in body, \
        f"{request_path}: start_dt no longer filters transaction_dt"
    assert 'filters.append("t.transaction_dt <= %s"); params.append(end_dt)' in body, \
        f"{request_path}: end_dt no longer filters transaction_dt"


@pytest.mark.usefixtures("ensure_api_reachable")
def test_transactions_answers_with_the_insert_clock_alone_live(quiesced_generator):
    """The delta request itself, with no business window: it must be answered, not 422."""
    client = quiesced_generator
    resp = client.get("/grocery/pos/transactions",
                      params=dict(BUSINESS_WINDOW, limit=1))
    assert resp.status_code == 200, f"a business window was rejected: {resp.status_code}"

    only_clock = client.get("/grocery/pos/transactions",
                            params={"limit": 1, "created_after": FAR_PAST,
                                    "created_before": FAR_FUTURE})
    assert only_clock.status_code == 200, (
        f"/grocery/pos/transactions rejected an insert-clock-only window: "
        f"{only_clock.status_code} {only_clock.text[:200]}"
    )
    assert only_clock.json()["total"] == resp.json()["total"] >= 1, (
        "the wide business window and no business window disagree on the row count"
    )


def _route_query_params(client: httpx.Client, path: str) -> set:
    """Query parameters the route declares, read the way the ingest reads them."""
    spec = client.get("/openapi.json", timeout=10.0).json()
    names: set = set()
    for spec_path, methods in spec.get("paths", {}).items():
        if spec_path.strip("/").split("/")[-2:] != path.strip("/").split("/")[-2:]:
            continue
        for method in methods.values():
            if isinstance(method, dict):
                names |= {p.get("name") for p in method.get("parameters", [])
                          if p.get("in") == "query"}
    return names


@pytest.mark.usefixtures("ensure_api_reachable")
@pytest.mark.parametrize("path", PATHS)
def test_openapi_advertises_the_ingest_window(api_base_url, path):
    """The ingest refuses to trust a configured window the route does not declare
    (FastAPI ignores unknown query parameters, so a bogus window looks like a
    windowed fetch that returns the whole table for every window)."""
    with httpx.Client(base_url=api_base_url, timeout=30.0) as client:
        assert {"created_after", "created_before"} <= _route_query_params(client, path), \
            f"{path}: OpenAPI does not advertise created_after/created_before"


def _as_datetime(value) -> datetime:
    """Parse an API timestamp (``Z`` or ``+00:00``, naive means UTC) as aware UTC."""
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc)


@pytest.mark.usefixtures("ensure_api_reachable")
@pytest.mark.parametrize("path", PATHS)
def test_created_window_filters_the_insert_clock(quiesced_generator, path):
    """A one-second-wide created window must hold only rows stamped with that second.

    This is what distinguishes the ingest window from the business one: a filter
    that ran on the business column would return the rows *whose business time
    falls in that second*, whose ``created_at`` values are spread over the whole
    history.
    """
    request_path, _, _ = _params(path)
    client = quiesced_generator
    page = client.get(path, params={"limit": 5000, "created_after": FAR_PAST,
                                    "created_before": FAR_FUTURE}).json()
    rows = page["data"]
    if not rows:
        pytest.skip(f"{path}: no rows to derive a batch timestamp from")

    newest = max(_as_datetime(row["created_at"]) for row in rows)
    lo = newest.isoformat()
    hi = (newest + timedelta(seconds=1)).isoformat()

    window = client.get(path, params={"limit": 5000, "created_before": hi,
                                      "created_after": lo}).json()
    assert window["total"] >= 1, f"{path}: the batch stamped {lo} returned no rows"
    for row in window["data"]:
        stamp = _as_datetime(row["created_at"])
        assert newest <= stamp <= newest + timedelta(seconds=1), (
            f"{path}: created window [{lo}, {hi}] returned created_at="
            f"{row['created_at']} — the window is not filtering the insert clock"
        )

    # Composable with the business window: both filters apply together.
    both = client.get(path, params={"limit": 5000, "created_after": lo,
                                    "created_before": hi,
                                    "start_dt": FAR_PAST, "end_dt": FAR_FUTURE}).json()
    assert both["total"] == window["total"], (
        f"{path}: adding an all-encompassing business window changed the ingest "
        f"window's count ({both['total']} vs {window['total']})"
    )
