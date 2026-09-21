"""The ingest-time window on ``pos.returns`` / ``pos.return_items`` (t_5d2e2ab0).

``pos.returns.return_dt`` is a *backdating* column — the generator stamps
``return_dt = transaction_dt + random(2..21) days`` (clamped to the simulated now),
so a nightly batch inserted at one instant carries business timestamps spread over
the previous month. Measured on the dev seed: 4055 of 9185 rows sat more than a day
below the batch's own clamp value, 2072 more than a week below. A consumer's
watermark of ``MAX(return_dt)`` therefore sits at the clamp and can never reach the
rows a *later* batch backdates below it, so no ``start_dt``/``end_dt`` combination
can drive an incremental load of the table.

The fix is a second window on the insert clock: ``created_after``/``created_before``
bound ``pos.returns.created_at`` (``DEFAULT NOW()``, written by the same statement as
the row), which is monotone and can only move forward. ``pos.return_items`` has no
timestamp of its own, so ``created_at`` is taken from its header — and is returned in
the payload, because a consumer needs that column in its own raw table to hold the
watermark.

These tests guard the contract data-lab's ingest depends on:
``grocery_ingest_api.py`` names the window parameters in ``TABLE_CONFIGS`` and
verifies them against ``/openapi.json`` before it trusts them, so the parameters must
be declared on the route (not merely accepted) and must really filter the ingest clock
in *both* the count query and the page query.
"""
import pathlib
import re
from datetime import datetime, timedelta, timezone

import httpx
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
API_SOURCE = REPO_ROOT / "base" / "api" / "main.py"

# Both routes expose the same pair; the column it must filter is the header's
# created_at in every case (return_items has no timestamp of its own).
ROUTES = ("/grocery/pos/returns", "/grocery/pos/return-items")

FAR_PAST = "2000-01-01T00:00:00+00:00"
FAR_FUTURE = "2999-01-01T00:00:00+00:00"
ONE_SECOND = "1 second"


def _route_source(path: str) -> str:
    """The source of one route handler, from its decorator to the next one."""
    source = API_SOURCE.read_text()
    marker = f'@app.get("{path}",'
    start = source.index(marker)
    end = source.index("\n@app.", start + len(marker))
    return source[start:end]


@pytest.mark.parametrize("path", ROUTES)
def test_route_declares_the_ingest_window_on_created_at(path):
    """The window must be declared (so OpenAPI advertises it) and applied to
    ``r.created_at`` in the count query and the page query alike — a filter on one
    and not the other makes ``total`` disagree with the pages."""
    body = _route_source(path)

    for param in ("created_after", "created_before"):
        assert re.search(rf"^\s+{param}: Optional\[datetime\] = None,$", body, re.M), \
            f"{path}: does not declare {param}"

    assert 'filters.append("r.created_at >= %s"); params.append(created_after)' in body, \
        f"{path}: created_after is not applied to r.created_at"
    assert 'filters.append("r.created_at <= %s"); params.append(created_before)' in body, \
        f"{path}: created_before is not applied to r.created_at"

    # Same WHERE clause for the count and the rows, so `total` cannot drift.
    uses = body.count("WHERE {where}")
    assert uses >= 2, (
        f"{path}: the count query and the page query must share one WHERE clause "
        f"(found {uses} uses of `WHERE {{where}}`)"
    )


def test_return_items_payload_carries_created_at():
    """data-lab watermarks on MAX(created_at) *of the raw table it built from this
    payload*, so the column has to be in the response — return_items has no
    timestamp of its own and would otherwise have no monotone column at all."""
    body = _route_source("/grocery/pos/return-items")
    select = body.split("FROM pos.return_items", 1)[0]
    assert "r.created_at" in select, \
        "/grocery/pos/return-items does not return r.created_at in its payload"


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
@pytest.mark.parametrize("path", ROUTES)
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
@pytest.mark.parametrize("path", ROUTES)
def test_created_window_filters_the_insert_clock(quiesced_generator, path):
    """A one-second-wide created window must hold only rows stamped with that second.

    This is what distinguishes the ingest window from the business one: a filter that
    ran on ``return_dt`` would return the rows *whose business time falls in that
    second*, whose ``created_at`` values are spread over the whole history.
    """
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
