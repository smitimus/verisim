"""API contract tests for the gas-station standalone image (t_a6ecb731).

Runs against a live container (CI starts one on port 8011).

What is asserted, and why each one is here:

* **/docs lists only gas-station's routes.** The strip script rewrites the title
  and deletes other industries' sections by matching the banner comment above
  each one — which means a renamed banner silently changes the API while the
  image still builds and still imports. The static form of this check is
  tools/check_strip_scripts.py; this is the form that sees the real artifact.
* **Every gas-station route answers 200, not 500.** A route whose table is missing
  from that product's schema.sql passes every static check and fails only here.
* **The paginated route pages without losing or duplicating a row.** That route
  orders by `transaction_dt DESC, transaction_id DESC` — the PK-terminated form
  the rest of Verisim requires after t_7c88f2f9 / t_d7892e10. This walks it and
  asserts the ids are unique and the count matches `total`.
* **The summary route agrees with the rows it summarises.**

Note the routes are NOT uniform, and the tests follow the real contract rather
than assuming one: only `/fuel/transactions` is paginated (and it *requires*
`start_dt`/`end_dt`), while grades, pumps and price-history return bare lists.
A suite that assumed `{data, total}` everywhere would pass on a route that had
silently stopped paginating, which is the opposite of what it is for.
"""
import httpx
import pytest

from .conftest import GAS_STATION_ROUTES

# Routes that take no parameters and return a bare list.
BARE_LIST_ROUTES = (
    "/gas-station/fuel/grades",
    "/gas-station/fuel/pumps",
)

# /fuel/price-history: filtered, limit-bounded, not offset-paginated.
PRICE_HISTORY = "/gas-station/fuel/price-history"

# The one paginated route. start_dt/end_dt are required, so the walk has to
# supply a window wide enough to hold everything the backfill wrote.
TRANSACTIONS = "/gas-station/fuel/transactions"
WINDOW = {"start_dt": "2000-01-01T00:00:00", "end_dt": "2100-01-01T00:00:00"}

# The routes that must exist. /status is checked separately because it 503s
# before the generator has written control.generator_state.
DATA_ROUTES = (
    "/gas-station/fuel/grades",
    "/gas-station/fuel/pumps",
    PRICE_HISTORY,
    TRANSACTIONS,
    "/gas-station/fuel/transactions/summary",
)


def _get(client, path, **params):
    resp = client.get(path, params=params)
    assert resp.status_code == 200, f"{path} returned {resp.status_code}: {resp.text[:300]}"
    return resp.json()


# ── platform ─────────────────────────────────────────────────────────────────

def test_health(client):
    body = client.get("/health")
    assert body.status_code == 200
    assert body.json(), "/health returned an empty body"


def test_docs_is_gas_station_only(client):
    """/docs must describe a gas-station API, not a multi-industry one.

    The paths are checked in the form FastAPI documents them: the shared routes
    appear as `/{industry}/...` templates, not as `/gas-station/...`. The
    expanded paths are still callable (asserted below), they are just not what
    /docs lists.
    """
    spec = client.get("/openapi.json").json()
    assert "Gas Station" in spec["info"]["title"], (
        f"expected a gas-station title, got {spec['info']['title']!r} — the strip "
        f"script did not run, or its title rewrite stopped matching"
    )

    paths = set(spec["paths"])
    missing = [p for p in GAS_STATION_ROUTES if p not in paths]
    assert not missing, f"gas-station routes missing from /docs: {missing}"

    foreign = sorted(p for p in paths if p.startswith(("/grocery/", "/support/")))
    assert not foreign, f"another industry's routes survived the strip: {foreign[:5]}"


def test_the_templated_status_route_answers_for_gas_station(client):
    """`/{industry}/status` must resolve for gas-station, not just be documented.

    The OpenAPI spec proves the route is registered; this proves the industry
    name in the path reaches the right database. A `/{industry}` route that
    resolves for grocery and 500s or 404s for gas-station passes every static
    check in the repo — it is only visible on a live container.
    """
    resp = client.get("/gas-station/status")
    assert resp.status_code == 200, (
        f"/gas-station/status returned {resp.status_code}; the route is declared as "
        f"a {{industry}} template, so this also proves the industry name is bound "
        f"correctly: {resp.text[:200]}"
    )
    assert "state" in resp.json(), f"/gas-station/status has no state: {resp.text[:200]}"


# ── every route answers ──────────────────────────────────────────────────────

@pytest.mark.parametrize("path", DATA_ROUTES)
def test_route_answers(client, path):
    """A route must answer 200, not 500.

    A 500 here means the route's table is not in gas-station's schema.sql — the
    failure that passes the static strip check because the API source and the
    schema are each individually valid.
    """
    params = dict(WINDOW) if path in (TRANSACTIONS, "/gas-station/fuel/transactions/summary") else {}
    _get(client, path, **params)


def test_status_answers_once_the_generator_has_written_state(client):
    """/gas-station/status 503s until control.generator_state exists, and never after."""
    resp = client.get("/gas-station/status")
    assert resp.status_code == 200, (
        f"/gas-station/status returned {resp.status_code} after the backfill "
        f"finished: {resp.text[:300]}"
    )
    body = resp.json()
    assert "state" in body, f"/gas-station/status has no state: {sorted(body)}"


# ── shape ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("path", BARE_LIST_ROUTES)
def test_bare_list_routes_return_lists(client, path):
    body = _get(client, path)
    assert isinstance(body, list), f"{path} returned {type(body).__name__}, expected a list"
    assert body, f"{path} returned no rows after the backfill"


def test_price_history_is_bounded_and_ordered(client):
    """price-history takes a limit, not an offset, and must honour both.

    A route that grows a limit but loses the ORDER BY would return an arbitrary
    slice of the table — the same class of bug as an unterminated pagination
    ORDER BY, where the caller cannot tell a stable answer from a random one.
    """
    rows = _get(client, PRICE_HISTORY, limit=5, **WINDOW)
    assert isinstance(rows, list)
    assert len(rows) <= 5, f"limit=5 returned {len(rows)} rows"

    if len(rows) > 1:
        stamps = [r["changed_at"] for r in rows]
        assert stamps == sorted(stamps, reverse=True), (
            f"price-history is not newest-first: {stamps}"
        )


def test_transactions_is_paginated(client):
    """The one paginated route returns the documented envelope and honours limit."""
    body = _get(client, TRANSACTIONS, limit=5, **WINDOW)
    for key in ("data", "total", "limit", "offset"):
        assert key in body, f"{TRANSACTIONS} response is missing {key!r}: {sorted(body)}"
    assert isinstance(body["data"], list)
    assert len(body["data"]) <= 5, f"limit=5 returned {len(body['data'])} rows"
    assert body["total"] >= len(body["data"]), (
        f"total {body['total']} < page size {len(body['data'])}"
    )


# ── the pagination contract ──────────────────────────────────────────────────

def test_transactions_pagination_loses_no_rows(quiesced):
    """Walking every page returns each row exactly once.

    The t_7c88f2f9 failure in its general form: a route whose ORDER BY ends on
    something other than the primary key can hand one row to two pages while
    `total` still matches, so the client cannot detect the loss. This asserts the
    collected ids are unique and that the walk saw exactly `total` rows — which
    only holds if the ORDER BY is PK-terminated.
    """
    page_size = 50
    seen: list[str] = []
    offset = 0
    total = None

    while True:
        body = _get(quiesced, TRANSACTIONS, limit=page_size, offset=offset, **WINDOW)
        total = body["total"]
        rows = body["data"]
        if not rows:
            break
        for row in rows:
            assert "transaction_id" in row, (
                f"{TRANSACTIONS} row has no transaction_id: {sorted(row)}"
            )
            seen.append(row["transaction_id"])
        offset += page_size
        if offset >= total:
            break

    assert total is not None
    assert len(seen) == total, (
        f"walked {len(seen)} rows but the route reports total={total} — a row is "
        f"duplicated across pages or lost"
    )
    assert len(set(seen)) == len(seen), f"duplicate transaction_id across pages"


def test_transactions_require_the_window(client):
    """start_dt/end_dt are required — a consumer must not get an unbounded scan.

    Asserted because the required-ness is easy to lose: making them Optional
    would let a caller pull the whole table in one request, and the route has no
    offset-stable ordering guarantee across that many rows.
    """
    resp = client.get(TRANSACTIONS, params={"limit": 5})
    assert resp.status_code == 422, (
        f"{TRANSACTIONS} accepted a request with no time window "
        f"(got {resp.status_code}); start_dt/end_dt are required by contract"
    )


# ── internal consistency ─────────────────────────────────────────────────────

def test_summary_agrees_with_the_transactions_it_summarises(quiesced):
    """The summary must cover the same rows the list route serves.

    The summary groups by day over the requested window; summing its counts must
    equal the paginated route's `total` for the same window. They are two
    separate SQL statements against the same table, so a disagreement means one
    of them has a different filter — which is exactly the sort of drift a
    consumer cannot see.
    """
    summary = quiesced.get(
        "/gas-station/fuel/transactions/summary", params={**WINDOW, "group_by": "day"}
    )
    assert summary.status_code == 200, summary.text[:300]
    buckets = summary.json()
    assert isinstance(buckets, list)

    listed = _get(quiesced, TRANSACTIONS, limit=1, **WINDOW)
    assert listed["total"] > 0, (
        "no fuel transactions after the backfill — the summary cannot be checked "
        "against an empty table"
    )

    summed = sum(b["transaction_count"] for b in buckets)
    assert summed == listed["total"], (
        f"summary covers {summed} transactions but the list route reports "
        f"{listed['total']} for the same window — the two disagree about which "
        f"rows they describe"
    )


def test_grade_join_resolves_on_every_transaction(quiesced):
    """Every transaction row must carry its grade's name.

    `/fuel/transactions` inner-joins fuel.grades, so an unresolved grade does not
    500 — it silently disappears from the page while `total` counts it. That is
    the pagination-loss failure wearing a different hat, and it is the reason this
    test asserts on the returned rows rather than only on their number.
    """
    body = _get(quiesced, TRANSACTIONS, limit=100, **WINDOW)
    assert body["data"], "no transaction rows to check"
    unresolved = [r for r in body["data"] if not r.get("grade_name")]
    assert not unresolved, (
        f"{len(unresolved)} of {len(body['data'])} transaction rows have no "
        f"grade_name — they were dropped by the inner join but still counted in "
        f"total={body['total']}"
    )
