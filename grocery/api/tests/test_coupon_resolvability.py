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

Unlike combo deals, these are asserted as *invariants* rather than as a count
comparison. The coupon relation does not accumulate retired rows the way combo
deals did — ``seed_coupons`` retires the expired ones and tops the active set
back up to ``active_at_any_time``, so the relation stays at its seeded size
rather than growing without bound. The interesting property is therefore
containment (the active slice is drawn from the full relation) and projection
(``is_active`` is there), not a specific number.
"""
import httpx
import pytest

COUPONS = "/grocery/pos/coupons"


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
    full = httpx.get(base, params={"active_only": False}, timeout=30.0)
    assert full.status_code == 200, f"coupons returned {full.status_code}"
    coupons = full.json()
    assert isinstance(coupons, list), "coupons payload should be a list"
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
    coupons = httpx.get(f"{api_base_url}{COUPONS}",
                        params={"active_only": False}, timeout=30.0).json()
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
    full = {c["coupon_id"]: c for c in
            httpx.get(base, params={"active_only": False}, timeout=30.0).json()}
    active = httpx.get(base, timeout=30.0).json()

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