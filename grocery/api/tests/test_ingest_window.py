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

``/grocery/timeclock/events`` is the same shape on its own table (t_de287826): it was
the last route in this family whose only window was a backdating business column
(``event_dt``), which is why data-lab's ingest had to re-read its whole history every
run (a 365-day reach-back and a stranded-row guard, t_64da90fb). ``created_at`` has
been on ``timeclock.events`` all along; the route simply neither filtered nor returned
it. Like ``/grocery/pos/transactions`` before it, ``start_dt``/``end_dt`` are now
optional there too, so a reader whose only watermark is the insert clock can ask for
it alone.

These tests guard the contract data-lab's ingest depends on:
``grocery_ingest_api.py`` names the window parameters in ``TABLE_CONFIGS`` and
verifies them against ``/openapi.json`` before it trusts them, so the parameters
must be declared on the route (not merely accepted) and must really filter the
ingest clock in *both* the count query and the page query.

``online.orders`` carries a second clock, and it answers a different question
(t_51bbc12e). ``created_at`` says *when the row was written*; ``updated_at`` says
*when it last changed* — the generator mutates ``status`` in place at every
lifecycle tick and bumps it in the same statement. So a created-window reader sees
the current state of every row in its window, including rows that changed after the
window ended: an order that completes while the read is in flight is loaded
``completed`` while the event that explains it is only readable on the next run,
which is the residual data-lab's ``assert_online_orders_reconcile`` kept catching on
a wide-window run. ``updated_after``/``updated_before`` bound that state, and — because
the generator writes a status change and its event in one transaction, so
``orders.updated_at == MAX(order_events.created_at)`` for the order — ending the
events read and the orders read at the same instant makes the pair a consistent
snapshot.
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
    # The lifecycle event stream: event_dt is business time and backdated, so the
    # whole-table read was the only thing the ingest could do with this route
    # (t_51bbc12e).
    ("/grocery/online/order-events", "/grocery/online/order-events", "e"),
    ("/grocery/pos/returns", "/grocery/pos/returns", "r"),
    ("/grocery/pos/return-items", "/grocery/pos/return-items", "r"),
    # The last route whose only window was a backdating business column: its
    # event_dt is stamped with the simulated instant a punch belongs to, so a
    # watermark on it could not see a re-generated day or a half written late
    # (t_de287826). Rows here have their own created_at, so the window binds e.
    ("/grocery/timeclock/events", "/grocery/timeclock/events", "e"),
]

# (request path, route source path, header alias) for the routes that also expose the
# *state* clock — which version of the row the reader sees (t_51bbc12e).
STATE_CLOCK_ROUTES = [
    ("/grocery/online/orders", "/grocery/online/orders", "o"),
]

PATHS = [request_path for request_path, _, _ in INGEST_WINDOW_ROUTES]
STATE_PATHS = [request_path for request_path, _, _ in STATE_CLOCK_ROUTES]


def _route_source(path: str) -> str:
    """The source of one route handler, from its decorator to the next one."""
    source = API_SOURCE.read_text()
    marker = f'@app.get("{path}",'
    start = source.index(marker)
    end = source.index("\n@app.", start + len(marker))
    return source[start:end]


def _params(path: str):
    for route in INGEST_WINDOW_ROUTES + STATE_CLOCK_ROUTES:
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


def test_timeclock_events_keeps_event_dt_as_the_business_window():
    """``/grocery/timeclock/events`` keeps its ``start_dt``/``end_dt`` window on
    ``event_dt``, and answers the insert clock alone (t_de287826).

    The route *required* both bounds while the date column was its only window. A
    consumer whose only watermark is the insert clock must be able to ask for it
    without inventing a business window — otherwise the delta request is a 422 and
    the ingest is back to re-reading the whole history every run (data-lab
    t_64da90fb). Adding the insert clock must not move the business window off
    ``event_dt``: that is the date-column window every existing reader uses.
    """
    request_path, source_path, _ = _params("/grocery/timeclock/events")
    body = _route_source(source_path)

    for param in ("start_dt", "end_dt"):
        assert re.search(rf"^\s+{param}: Optional\[datetime\] = None,$", body, re.M), \
            f"{request_path}: {param} is not optional"

    assert 'filters.append("e.event_dt >= %s"); params.append(start_dt)' in body, \
        f"{request_path}: start_dt no longer filters event_dt"
    assert 'filters.append("e.event_dt <= %s"); params.append(end_dt)' in body, \
        f"{request_path}: end_dt no longer filters event_dt"


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


# ---------------------------------------------------------------------------
# The state clock — ``updated_at`` on online.orders (t_51bbc12e)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", STATE_PATHS)
def test_route_declares_the_state_clock_on_updated_at(path):
    """``updated_after``/``updated_before`` must be declared and applied to
    ``updated_at`` in the count *and* the page query, exactly as the ingest window is:
    a filter on one and not the other makes ``total`` disagree with the pages."""
    request_path, source_path, alias = _params(path)
    body = _route_source(source_path)

    for param in ("updated_after", "updated_before"):
        assert re.search(rf"^\s+{param}: Optional\[datetime\] = None,$", body, re.M), \
            f"{request_path}: does not declare {param}"

    assert f'filters.append("{alias}.updated_at >= %s"); params.append(updated_after)' in body, \
        f"{request_path}: updated_after is not applied to {alias}.updated_at"
    assert f'filters.append("{alias}.updated_at <= %s"); params.append(updated_before)' in body, \
        f"{request_path}: updated_before is not applied to {alias}.updated_at"

    uses = body.count("WHERE {where}")
    assert uses >= 2, (
        f"{request_path}: the count query and the page query must share one WHERE clause "
        f"(found {uses} uses of `WHERE {{where}}`)"
    )


@pytest.mark.parametrize("path", STATE_PATHS)
def test_state_clock_leaves_the_insert_clock_alone(path):
    """The state window is *additional*: ``created_after``/``created_before`` keep
    filtering ``created_at``, so an incremental load can combine both."""
    request_path, source_path, alias = _params(path)
    body = _route_source(source_path)
    assert f'filters.append("{alias}.created_at >= %s"); params.append(created_after)' in body
    assert f'filters.append("{alias}.created_at <= %s"); params.append(created_before)' in body


@pytest.mark.parametrize("path", STATE_PATHS)
def test_payload_carries_updated_at(path):
    """A consumer watermarks on ``MAX(updated_at)`` of its own raw table, so the
    column has to be in the response (as ``created_at`` had to be)."""
    request_path, source_path, alias = _params(path)
    select_list = _page_select_list(_route_source(source_path))
    assert f"{alias}.updated_at" in select_list, (
        f"{request_path} does not return {alias}.updated_at in its payload:\n{select_list}"
    )


@pytest.mark.usefixtures("ensure_api_reachable")
@pytest.mark.parametrize("path", STATE_PATHS)
def test_openapi_advertises_the_state_clock(api_base_url, path):
    """Same trap as the ingest window: FastAPI ignores unknown query parameters, so a
    configured state window the route does not declare looks like a bounded read that
    returns the whole table in its *current* state."""
    with httpx.Client(base_url=api_base_url, timeout=30.0) as client:
        assert {"updated_after", "updated_before"} <= _route_query_params(client, path), \
            f"{path}: OpenAPI does not advertise updated_after/updated_before"


@pytest.mark.usefixtures("ensure_api_reachable")
@pytest.mark.parametrize("path", STATE_PATHS)
def test_state_window_filters_the_state_clock(quiesced_generator, path):
    """A one-second-wide state window must hold only rows whose state moved in that
    second — the analogue of the created-window test above, and what tells a real
    filter apart from one that ran on ``created_at``."""
    request_path, _, _ = _params(path)
    client = quiesced_generator
    page = client.get(path, params={"limit": 5000, "updated_after": FAR_PAST,
                                    "updated_before": FAR_FUTURE}).json()
    rows = page["data"]
    if not rows:
        pytest.skip(f"{path}: no rows to derive a state timestamp from")

    newest = max(_as_datetime(row["updated_at"]) for row in rows)
    lo = newest.isoformat()
    hi = (newest + timedelta(seconds=1)).isoformat()

    window = client.get(path, params={"limit": 5000, "updated_after": lo,
                                      "updated_before": hi}).json()
    assert window["total"] >= 1, f"{path}: the state batch stamped {lo} returned no rows"
    for row in window["data"]:
        stamp = _as_datetime(row["updated_at"])
        assert newest <= stamp <= newest + timedelta(seconds=1), (
            f"{path}: state window [{lo}, {hi}] returned updated_at={row['updated_at']} "
            f"— the window is not filtering the state clock"
        )

    # Composable with the insert window and the business window.
    both = client.get(path, params={"limit": 5000, "updated_after": lo,
                                    "updated_before": hi, "created_after": FAR_PAST,
                                    "created_before": FAR_FUTURE,
                                    "start_dt": FAR_PAST, "end_dt": FAR_FUTURE}).json()
    assert both["total"] == window["total"], (
        f"{path}: adding all-encompassing business and insert windows changed the "
        f"state window's count ({both['total']} vs {window['total']})"
    )


@pytest.mark.usefixtures("ensure_api_reachable")
def test_state_clock_makes_the_completion_invariant_deterministic(quiesced_generator):
    """The residual behind data-lab's ``assert_online_orders_reconcile`` (t_51bbc12e).

    A reader that bounds its window on ``created_at`` alone still gets the *current*
    state of every row in that window: an order that completes while the read is in
    flight is loaded ``completed`` while the event that explains the completion is only
    readable on the next run. Bounding the state as well closes the gap — at an instant
    just before the completion the order is not returned at all, and neither is the
    terminal event, which is what makes the pair consistent by construction.
    """
    client = quiesced_generator
    completed = client.get("/grocery/online/orders",
                           params={"status": "completed", "limit": 200}).json()["data"]
    if not completed:
        pytest.skip("no completed orders to derive a state stamp from")

    order = max(completed, key=lambda row: _as_datetime(row["updated_at"]))
    completed_dt = _as_datetime(order["updated_at"])
    just_before = (completed_dt - timedelta(microseconds=1)).isoformat()

    # 1. The hazard is real: the completion is in the row, its terminal event is not
    #    readable yet (both carry the same transaction stamp).
    early = client.get("/grocery/online/order-events",
                       params={"order_id": order["order_id"],
                               "created_before": just_before, "limit": 5000}).json()
    assert not any(_as_datetime(e["created_at"]) == completed_dt for e in early["data"]), (
        "the terminal event was already readable before the completion it explains"
    )

    # 2. A created-window-only reader loads the order as completed anyway ...
    window_only = client.get("/grocery/online/orders",
                             params={"status": "completed", "created_after": FAR_PAST,
                                     "created_before": just_before, "limit": 1}).json()
    assert window_only["total"] >= 1, (
        "the created-window read did not reach the order, so this test proves nothing"
    )

    # 3. ... and the state bound is what keeps it out of the load.
    body = client.get("/grocery/online/orders",
                      params={"status": "completed", "updated_before": just_before,
                              "limit": 5000}).json()
    assert all(_as_datetime(row["updated_at"]) <= _as_datetime(just_before)
               for row in body["data"]), "updated_before returned a row mutated after it"
    assert order["order_id"] not in {row["order_id"] for row in body["data"]}, (
        "updated_before did not bound the state: an order whose completion is stamped "
        "after the read end is still returned as completed"
    )


def test_the_generator_bumps_the_state_clock_with_every_transition():
    """``updated_at`` is only a state clock while every status change moves it.

    ``advance_online_lifecycle`` is the only code that mutates ``online.orders``, and it
    must stamp the change and write the event it explains in **one** transaction — the
    route's as-of filter and the ``orders.updated_at == MAX(order_events.created_at)``
    identity the ingest's snapshot recipe rests on both come from that. A later edit that
    drops the ``updated_at`` bump, or commits between the two statements, would leave the
    API's contract silently wrong (nothing else would fail: ``updated_before`` would just
    start returning rows in a state the events read cannot match).
    """
    source = (REPO_ROOT / "grocery" / "generator" / "models" / "online.py").read_text()
    lifecycle = source[source.index("def advance_online_lifecycle"):]

    update_start = lifecycle.index("UPDATE online.orders")
    update_end = lifecycle.index('"""', update_start)
    update_statement = lifecycle[update_start:update_end]
    assert re.search(r"SET status = v\.st", update_statement), \
        "advance_online_lifecycle no longer sets the status"
    assert re.search(r"updated_at = NOW\(\)", update_statement), (
        "advance_online_lifecycle no longer bumps updated_at: a status change would stop "
        "moving the state clock and the as-of read would return rows in a state the events "
        "read cannot match (t_51bbc12e)"
    )

    # The event that explains the change is inserted before that function's only commit,
    # i.e. in the same transaction — which is why the two stamps are one value.
    events_at = lifecycle.index("INSERT INTO online.order_events", update_start)
    commits = [m.start() for m in re.finditer(r"conn\.commit\(\)", lifecycle)]
    assert commits, "advance_online_lifecycle does not commit any more"
    assert events_at < min(commits), (
        "the lifecycle event is written after a commit: the order's updated_at and its "
        "event's created_at would no longer be the same stamp, and an as-of read ending "
        "at one instant B would no longer be consistent (t_51bbc12e)"
    )
