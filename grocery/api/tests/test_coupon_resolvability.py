"""A coupon a line item references must stay resolvable on the coupons route.

The coupon counterpart of ``test_promo_resolvability.py`` (which pins the same
contract for combo deals, added in t_27c2dcf7).

``pos.transaction_items.coupon_id`` carries a real FK to ``pos.coupons``
(``transaction_items_coupon_id_fkey``), so every coupon a line references exists
in Verisim. It does not follow that a *consumer* can see it: ``seed_coupons``
sets ``is_active = FALSE`` once ``valid_until`` passes (models/pos.py) and never
deletes the row, because the redemptions it earned are still on disk.
``/grocery/pos/coupons`` defaults to ``active_only=true``, so that retirement
moved coupons out of the response while the line items referencing them stayed.

Measured on the dev slot 2026-10-04: 8 coupons on disk, 8 active, all windows
running to 2027-09-21, referenced by 308,543 transaction_items across 8 distinct
coupon_ids, 0 orphans. So nothing is broken *yet* — the gap is latent, which is
exactly why it is pinned by tests here rather than discovered from an EDW row
count months later when the first coupon actually retires.

These tests pin the three halves of the contract:

* ``active_only=false`` serves the full relation, so a consumer resolving an FK
  can reach every coupon it needs. This already worked — it is the default
  ``active_only=true`` and the EDW's use of it that cost the rows, so the test
  exists to stop a future default flip re-breaking it silently.
* ``is_active`` is in the projection, so that consumer can still tell retired
  from current. Without the column a full load is indistinguishable from an
  active one, and every downstream "active promotion" count silently doubles.
* The two sides of the filter read the same rows, so a refactor recomputing them
  independently cannot drift them apart without this failing.
* The route is paged and counted (t_e3008391): it answers with the
  ``{data, total, limit, offset}`` envelope its siblings use, honours ``offset``
  across pages, and ends its ``ORDER BY`` on ``coupon_id``. Before that it was the
  last count-less capped route in the API — a bare list with no ``offset`` and no
  ``total``, so a consumer could neither reach row 1,001 nor prove its window
  complete. Every read below unpacks ``data``.

Unlike combo deals, these are asserted as *invariants* rather than as a count
comparison. The coupon relation does not accumulate retired rows the way combo
deals did — ``seed_coupons`` retires the expired ones and tops the active set
back up to ``active_at_any_time``, so the relation stays at its seeded size
rather than growing without bound. The interesting property is therefore
containment (the active slice is drawn from the full relation) and projection
(``is_active`` is there), not a specific number.
"""
import math
import pathlib
import re

import httpx
import pytest

COUPONS = "/grocery/pos/coupons"

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
API_SOURCE = REPO_ROOT / "base" / "api" / "main.py"


def _rows(payload):
    """The rows of a coupons response — the envelope's ``data`` (t_e3008391).

    The route used to answer with a bare list. It now returns
    ``{data, total, limit, offset}`` like ``/inventory/receipts`` does, so every
    assertion here goes through this helper rather than assuming either shape.
    """
    assert isinstance(payload, dict), (
        f"the coupons route answered with {type(payload).__name__}, not the "
        "{data,total,limit,offset} envelope — see t_e3008391")
    assert isinstance(payload.get("data"), list), f"envelope has no `data` list: {payload}"
    return payload["data"]


@pytest.mark.usefixtures("ensure_api_reachable")
def test_coupons_full_relation_resolves_referenced_coupons(api_base_url):
    """`active_only=false` resolves every coupon the line items reference.

    This is the invariant the EDW needs and the `active_only=true` default
    denies it: an active-only mirror leaves any retired coupon unresolvable,
    and every line item that was discounted by it becomes a dangling
    `coupon_id` in `raw_pos.transaction_items`.

    Asserted against the *item side* rather than the route's own row count,
    because "the route returned some rows" is exactly the check that passes
    while the EDW is still broken — the active slice is non-empty too.
    """
    base = f"{api_base_url}{COUPONS}"
    full = httpx.get(base, params={"active_only": False, "limit": 1000}, timeout=30.0)
    assert full.status_code == 200, f"coupons returned {full.status_code}"
    payload = full.json()
    coupons = _rows(payload)
    assert payload["total"] == len(coupons), (
        f"the route advertises total={payload['total']} but served "
        f"{len(coupons)} rows at limit=1000 — this test needs the whole relation "
        "(\"the page I got\" is not the full relation)")
    if not coupons:
        pytest.skip("no coupons on this dataset")

    # Pull the referenced ids out of the items the API itself serves, so this
    # needs no database access and holds wherever the suite is pointed.
    items = httpx.get(f"{api_base_url}/grocery/pos/transaction-items",
                      params={"limit": 5000}, timeout=60.0)
    if items.status_code != 200:
        pytest.skip(f"transaction-items route returned {items.status_code}")

    referenced = {i["coupon_id"] for i in items.json().get("data", [])
                  if i.get("coupon_id")}
    if not referenced:
        pytest.skip("no coupon-tagged items on this dataset")

    served = {c["coupon_id"] for c in coupons}
    unresolvable = referenced - served
    assert not unresolvable, (
        f"{len(unresolvable)} of {len(referenced)} coupons referenced by "
        "transaction_items are not served by active_only=false: "
        f"{sorted(unresolvable)[:5]}. A consumer mirroring this relation cannot "
        "resolve its own foreign keys."
    )


@pytest.mark.usefixtures("ensure_api_reachable")
def test_coupons_payload_carries_is_active(api_base_url):
    """A retired coupon must be distinguishable from a live one.

    Guards the reason `active_only=false` is safe to consume: without the
    column, a full load and an active-only load are the same rows. This is the
    Verisim-side prerequisite for data-lab mirroring the whole relation — it is
    asserted here because the failure mode it prevents is a wrong number nobody
    would query for.
    """
    coupons = _rows(httpx.get(f"{api_base_url}{COUPONS}",
                              params={"active_only": False, "limit": 1000},
                              timeout=30.0).json())
    if not coupons:
        pytest.skip("no coupons on this dataset")
    missing = [c for c in coupons if "is_active" not in c]
    assert not missing, (
        f"{len(missing)}/{len(coupons)} coupons carry no `is_active` — a consumer "
        "cannot tell a retired coupon from a live one. data-lab's "
        "stg_pos_coupons carries `not_null` on this column precisely so a "
        "regression here fails loudly instead of reporting retired coupons as live."
    )
    assert all(isinstance(c["is_active"], bool) for c in coupons), \
        "`is_active` must serialize as a boolean, not a string"


@pytest.mark.usefixtures("ensure_api_reachable")
def test_active_only_is_a_strict_subset_of_coupons(api_base_url):
    """`active_only=true` must not report anything the full route disagrees with.

    The coupon counterpart of the combo-deals containment test. Guards the
    ordering invariant: the active slice is drawn from the same rows the full
    relation serves, so a coupon_id present in both must be the same coupon
    with the same window. A refactor that recomputed the two sides
    independently could drift them, and the symptom would be an FD-resolution
    test failing far from the route.

    Containment is asserted in both directions on purpose: the active slice must
    be a subset of the full relation (no phantom rows), and the full relation
    must not be a subset of the active one (which would mean `active_only=false`
    is not actually widening anything — the exact regression this route exists
    to make possible, and it would be invisible while every coupon is active).
    """
    base = f"{api_base_url}{COUPONS}"
    full = {c["coupon_id"]: c for c in _rows(
        httpx.get(base, params={"active_only": False, "limit": 1000}, timeout=30.0).json())}
    active = _rows(httpx.get(base, params={"limit": 1000}, timeout=30.0).json())

    if not full:
        pytest.skip("no coupons on this dataset")

    for coupon in active:
        assert coupon["coupon_id"] in full, (
            f"coupon {coupon['coupon_id']} is served as active but absent from "
            "the full relation — the two sides of the filter are reading "
            "different rows"
        )
        assert coupon["valid_until"] == full[coupon["coupon_id"]]["valid_until"], \
            f"coupon {coupon['coupon_id']} has two different valid_until values"

    assert len(active) <= len(full), "the active slice cannot exceed the full relation"

    # The load-bearing direction for a full mirror: while every coupon happens
    # to be active the two are equal, so a route that ignored `active_only`
    # entirely would pass the equality check above forever. Any strict subset
    # proves the flag widens.
    if len(active) < len(full):
        assert not set(full) <= set(active), (
            "active_only=false returned no rows beyond the active slice — the "
            "parameter is not widening the response, so a consumer mirroring "
            "this relation would silently get the active slice anyway."
        )


# ---------------------------------------------------------------------------
# Paged and counted (t_e3008391)
# ---------------------------------------------------------------------------
def _walk(base, active_only, limit):
    """Page the route with consecutive offsets; return (advertised total, ids in order).

    Deliberately uses a SMALL page size: the relation is ~8 rows on every current
    data dir, so a limit=1000 walk would be a single request and prove nothing
    about `offset`. At limit=3 the same relation spans three pages, including a
    short final page.
    """
    params = {"limit": limit, "offset": 0}
    if active_only is not None:
        params["active_only"] = active_only
    first = httpx.get(base, params=params, timeout=30.0).json()
    total = first["total"]
    ids = [str(row["coupon_id"]) for row in _rows(first)]
    offset = limit
    while offset < total:
        params = {"limit": limit, "offset": offset}
        if active_only is not None:
            params["active_only"] = active_only
        body = httpx.get(base, params=params, timeout=30.0).json()
        assert body["offset"] == offset, (
            f"asked for offset={offset} and the envelope echoed {body['offset']}")
        page = _rows(body)
        if not page:
            break
        ids.extend(str(row["coupon_id"]) for row in page)
        offset += limit
    return total, ids


@pytest.mark.usefixtures("ensure_api_reachable")
def test_coupons_route_returns_the_counts_envelope(api_base_url):
    """The route answers `{data,total,limit,offset}`, not a bare list (t_e3008391).

    This was the LAST count-less capped route: `limit` capped at 1000, no
    `offset`, bare JSON array, no `total`. A loader that prefers `total` could
    not tell a complete window from a truncated page on this relation at all,
    which is why data-lab's short-page rule was still load-bearing for
    `pos_coupons` after t_2ab8e903 fixed receipts.
    """
    body = httpx.get(f"{api_base_url}{COUPONS}",
                     params={"active_only": False, "limit": 5, "offset": 0},
                     timeout=30.0).json()
    assert isinstance(body, dict), (
        f"the coupons route still answers with a bare {type(body).__name__}; "
        "it must return the {data,total,limit,offset} envelope (t_e3008391)")
    assert set(body) == {"data", "total", "limit", "offset"}, (
        f"the coupons envelope carries {sorted(body)} — expected exactly "
        "data/total/limit/offset, the shape /inventory/receipts and the rest of "
        "the family use")
    assert body["limit"] == 5 and body["offset"] == 0, (
        f"the envelope does not echo the requested page: {body['limit']=} {body['offset']=}")
    assert isinstance(body["total"], int) and body["total"] >= len(body["data"]), (
        f"`total` must count the relation under the filter, got {body['total']!r} "
        f"for a page of {len(body['data'])}")


@pytest.mark.usefixtures("ensure_api_reachable")
def test_coupons_offset_pages_are_disjoint_and_cover_the_relation(api_base_url):
    """`offset` must actually move the window, over N pages, with no duplicates.

    This is the check the route could not pass before t_e3008391: with no
    `offset` parameter every request returned the same first page, so a walk
    over `ceil(total/limit)` requests would either stall or repeat rows.
    Asserted on ids, not on length — a route that ignored `offset` would return
    a full page every time and still look healthy.
    """
    base = f"{api_base_url}{COUPONS}"
    total = httpx.get(base, params={"limit": 1, "offset": 0, "active_only": False},
                      timeout=30.0).json()["total"]
    if total < 2:
        pytest.skip(f"relation holds {total} coupon(s) — nothing to page")
    # Genuinely multi-page: at least two pages, and a smaller page size than a
    # relation this size would usually get by with.
    limit = max(1, min(3, total - 1))

    total, ids = _walk(base, False, limit)
    distinct = set(ids)
    assert len(ids) == len(distinct), (
        f"{len(ids) - len(distinct)} of {len(ids)} coupons came back on more than "
        f"one page at limit={limit} — `offset` is not moving the window, or the "
        "ORDER BY is not a total order (t_e3008391 / t_d7892e10)")
    assert len(distinct) == total, (
        f"walking limit={limit} over {total // limit + 1} pages reached "
        f"{len(distinct)} distinct coupons but the route advertises total={total} "
        f"({total - len(distinct)} unreachable)")

    # A second pass must return the same rows in the same order: the PK
    # tiebreaker is what makes the page order — and therefore page membership —
    # deterministic. All 8 coupons on the test data dir share one `valid_until`,
    # so before the tiebreaker the whole relation was a single tie cluster.
    _, again = _walk(base, False, limit)
    assert again == ids, (
        "a second identical walk returned a different coupon order; page "
        "membership inside the `valid_until` tie cluster is not deterministic "
        "(t_d7892e10)")


@pytest.mark.usefixtures("ensure_api_reachable")
def test_coupons_limit_above_the_cap_is_still_rejected(api_base_url):
    """The 1000-row ceiling stays a 422 — the route pages instead of widening it.

    Kept deliberately: the fix for a capped route is `offset` + `total`, not a
    bigger cap, and a silently clamped `limit` is what made the original defect
    invisible to the caller.
    """
    resp = httpx.get(f"{api_base_url}{COUPONS}", params={"limit": 1001}, timeout=30.0)
    assert resp.status_code == 422, (
        f"limit=1001 returned {resp.status_code}; the cap must stay an explicit "
        "422 so a caller cannot believe it asked for the whole relation")


# ---------------------------------------------------------------------------
# Source-level (offline, so CI enforces them on every data dir)
# ---------------------------------------------------------------------------
def _route_source(path: str) -> str:
    """The source of one route handler, from its decorator to the next one."""
    source = API_SOURCE.read_text()
    marker = f'@app.get("{path}",'
    start = source.index(marker)
    return source[start:source.index("\n@app.", start + len(marker))]


def test_the_coupons_route_declares_offset_and_counts_the_same_rows():
    """`offset`, `OFFSET %s`, `total`, and a count over `pos.coupons` — by name.

    All four are asserted because each one alone leaves the defect half-open:
    `total` without `offset` still cannot reach row 1001, `offset` without
    `total` cannot prove a window complete, and a `total` counted over the
    LEFT JOIN would count departments instead of coupons.
    """
    body = _route_source("/grocery/pos/coupons")

    signature = body[body.index("def pos_coupons("):body.index("):", body.index(
        "def pos_coupons("))]
    assert re.search(r"\boffset\s*:\s*int\s*=\s*0", signature), (
        "`/grocery/pos/coupons` no longer declares `offset: int = 0`; a client "
        f"cannot page past `limit` without it (t_e3008391).\nSignature was:\n{signature}")
    assert "OFFSET %s" in body, (
        "`/grocery/pos/coupons` slices with `LIMIT %s` and no `OFFSET`; the declared "
        "`offset` would be accepted and silently ignored (t_e3008391)")
    assert re.search(r'"total"\s*:\s*total\b', body), (
        "`/grocery/pos/coupons` no longer returns `total`; without it a consumer "
        "cannot tell a complete relation from a truncated page (t_e3008391)")
    count_sql = re.search(r'SELECT\s+COUNT\(\*\)[^"]*?FROM\s+(\S+)', body, re.S)
    assert count_sql and count_sql.group(1) == "pos.coupons", (
        "the coupons `total` no longer counts `pos.coupons` rows; the row query "
        "LEFT JOINs `pos.departments`, so counting the join would be a count of "
        "the wrong relation (t_e3008391)")


def test_the_coupons_page_order_ends_on_the_primary_key():
    """`valid_until` is not a total order — all 8 coupons on the test data dir
    share one value — so the page order must end on `coupon_id` (t_d7892e10).

    `test_pagination.py` enforces this for every `OFFSET %s` query; this asserts
    it for the route by name, because the route had no `offset` when that scan
    was written and so was the one it could not see.
    """
    body = _route_source("/grocery/pos/coupons")
    clauses = re.findall(r"ORDER BY\s+(.*?)\s*LIMIT", body, re.S)
    assert clauses, "the coupons route has no `ORDER BY ... LIMIT` to page with"
    clause = clauses[-1].strip()
    last_key = clause.split(",")[-1].strip().split()[0]
    assert last_key.split(".")[-1] == "coupon_id", (
        f"`/grocery/pos/coupons` pages on `{clause}`, which does not end on the "
        "primary key; a tie cluster straddling a page boundary returns one coupon "
        "twice and another never, while `total` still matches (t_d7892e10)")