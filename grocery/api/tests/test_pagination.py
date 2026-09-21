"""Pagination must not lose rows when the sort key has ties.

Regression suite for the silent row loss reported in verisim `t_d7892e10` and hit
by data-lab in `t_7c88f2f9`:

    ORDER BY <non-unique column> LIMIT %s OFFSET %s

Postgres may return tied rows in a different order from one query to the next, so a
tie cluster straddling a page boundary can hand one row to two pages while another
row is never returned by any page. Both requests still return a full page and the
count still matches ``total``, which is what made the loss invisible to consumers
(data-lab's ``raw_online.order_events`` came out 272 rows short while the source DB
held them all; 19 orders ended up ``completed`` with no terminal event).

Two guards:

* a source-level check that every query sliced with ``LIMIT %s OFFSET %s`` ends its
  ``ORDER BY`` on a primary key, so a new paginated route cannot be added without
  one. ``support/customers`` passes it because it already sorts on its primary key;
* a live check that paging a running instance at ``limit=1000`` returns ``total``
  distinct primary keys, and that a second pass returns exactly the same set.
"""
import pathlib
import re
import time

import httpx
import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
API_SOURCE = REPO_ROOT / "base" / "api" / "main.py"
SCHEMA_FILES = [
    REPO_ROOT / "grocery" / "generator" / "schema.sql",
    REPO_ROOT / "gas-station" / "generator" / "schema.sql",
    REPO_ROOT / "support" / "generator" / "schema.sql",
]

# Every key column that the paginated ORDER BY clauses are allowed to break ties on.
def _primary_key_columns() -> "set[str]":
    columns = set()
    for schema in SCHEMA_FILES:
        if not schema.exists():
            continue
        for line in schema.read_text().splitlines():
            match = re.match(r"\s*([a-z_]+)\s+\S+.*\bPRIMARY KEY\b", line)
            if match:
                columns.add(match.group(1))
    return columns


def _paginated_order_by_clauses(source: pathlib.Path):
    """Yield (line number, ORDER BY clause) for every OFFSET-paginated query."""
    lines = source.read_text().splitlines()
    for index, line in enumerate(lines):
        if "OFFSET %s" not in line:
            continue
        order_by = None
        for back in range(index, max(-1, index - 60), -1):
            if "ORDER BY" in lines[back]:
                order_by = lines[back].split("ORDER BY", 1)[1].split("LIMIT", 1)[0]
                break
        yield index + 1, (order_by or "").strip()


def test_paginated_queries_break_ties_on_a_primary_key():
    """A LIMIT/OFFSET page is only complete if the ORDER BY is a total order."""
    primary_keys = _primary_key_columns()
    assert "transaction_id" in primary_keys, "schema parsing failed: no primary keys found"

    clauses = list(_paginated_order_by_clauses(API_SOURCE))
    # The API has 35+ paginated routes; a sudden drop means the scan broke, not that
    # somebody un-paginated the API.
    assert len(clauses) >= 30, f"only found {len(clauses)} paginated queries in {API_SOURCE}"

    offenders = []
    for line_no, clause in clauses:
        if not clause:
            offenders.append((line_no, "no ORDER BY at all"))
            continue
        last_key = clause.split(",")[-1].strip()
        last_key = re.split(r"\s+", last_key)[0]      # drop ASC / DESC
        column = last_key.split(".")[-1]              # drop the table alias
        if column not in primary_keys:
            offenders.append((line_no, clause))

    assert not offenders, (
        "paginated queries must end their ORDER BY on a unique key (the primary key); "
        "otherwise rows in a tie cluster that straddles a page boundary can be returned "
        "twice or never, while `total` still matches:\n"
        + "\n".join(f"  base/api/main.py:{ln}: {clause}" for ln, clause in offenders)
    )


# (route, primary key column returned by the route, page size)
PAGINATED_ROUTES = [
    ("/grocery/pos/loyalty-members", "member_id", 1000),
    ("/grocery/pos/price-history", "price_history_id", 1000),
    ("/grocery/pos/return-items", "return_item_id", 1000),
    ("/grocery/online/order-events", "event_id", 1000),
    ("/grocery/hr/schedules", "schedule_id", 1000),
    ("/grocery/online/order-items", "item_id", 1000),
]


@pytest.fixture
def quiesced_generator(api_base_url):
    """Pause the generator for the duration of a walk.

    Paging and row counts are only comparable while nothing is inserting: a tick that
    lands mid-walk shifts every later page by a few rows, which looks like pagination
    loss but is not. The generator is resumed even if the test fails.
    """
    with httpx.Client(base_url=api_base_url, timeout=30.0) as client:
        state = {}
        try:
            state = client.get("/grocery/status").json()["state"]
        except Exception as exc:                       # noqa: BLE001
            pytest.skip(f"generator status unavailable: {exc}")
        was_running = bool(state.get("is_running")) and not state.get("is_paused")
        if was_running:
            client.post("/grocery/generator/pause")
        try:
            yield client
        finally:
            if was_running:
                client.post("/grocery/generator/resume")


def _walk(client, path, pk, limit):
    """Page a route once; return (advertised total, pk string per row returned)."""
    total = client.get(path, params={"limit": 1, "offset": 0}).json()["total"]
    seen: "list[str]" = []
    offset = 0
    while offset < total:
        body = client.get(path, params={"limit": limit, "offset": offset}).json()
        page = body["data"]
        if not page:
            break
        seen.extend(str(row[pk]) for row in page)
        offset += limit
    return total, seen


@pytest.mark.usefixtures("ensure_api_reachable")
@pytest.mark.parametrize("path,pk,limit", PAGINATED_ROUTES)
def test_first_pass_paging_returns_every_row(quiesced_generator, path, pk, limit):
    """limit=1000 with consecutive offsets must reach `total` distinct primary keys."""
    client = quiesced_generator
    total, seen = _walk(client, path, pk, limit)
    for _ in range(2):
        # If the generator (or a backfill) still managed to write mid-walk the pages are
        # shifted rather than broken; re-measure until the row count is stable.
        after = client.get(path, params={"limit": 1, "offset": 0}).json()["total"]
        if after == total:
            break
        time.sleep(2)
        total, seen = _walk(client, path, pk, limit)
    else:
        pytest.fail(f"{path}: row count kept changing during the walk - nothing to assert")

    distinct = set(seen)
    assert total == len(distinct), (
        f"{path}: paging limit={limit} reached {len(distinct)} distinct {pk}s "
        f"but the route advertises total={total} ({total - len(distinct)} rows unreachable)"
    )
    assert len(seen) == len(distinct), (
        f"{path}: {len(seen) - len(distinct)} rows were returned on more than one page"
    )

    _, again = _walk(client, path, pk, limit)
    assert set(again) == distinct, f"{path}: a second pass returned a different set of rows"
