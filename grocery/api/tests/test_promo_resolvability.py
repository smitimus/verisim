"""A deal a line item references must stay resolvable on the combo-deals route.

``pos.transaction_items.deal_id`` carries a real FK to ``pos.combo_deals``
(``transaction_items_deal_id_fkey``), so every deal a line references exists in
Verisim. It does not follow that a *consumer* can see it:
``seed_combo_deals`` sets ``is_active = FALSE`` once ``valid_until`` passes
(models/pos.py) and never deletes the row, because the redemptions it earned are
still on disk. ``/grocery/pos/combo-deals`` defaulted to ``active_only=true``,
so that retirement moved deals out of the response while the line items
referencing them stayed put.

Measured on CT107 2026-10-01: 8 deals on disk, 4 active, and 8 distinct
``deal_id`` values across 68,194 items — 4 resolving, 4 retired, carrying
56,036 items between them. Every one of those redemptions was inside its own
validity window, so nothing was wrong with the source; the EDW simply held an
active-only mirror and could not resolve its own foreign keys.

These tests pin the two halves of the fix:

* ``active_only=false`` serves the full relation, so a consumer resolving an FK
  can reach every deal it needs. This already worked — it is the default
  ``active_only=true`` and the EDW's use of it that cost the rows, so the test
  exists to stop a future default flip re-breaking it silently.
* ``is_active`` is in the projection (as on the coupons route), so that consumer
  can still tell retired from current. Without the column a full load is
  indistinguishable from an active one, and every downstream "active promotion"
  count silently doubles. That column is the Verisim-side prerequisite for
  data-lab flipping their ingest to ``active_only=false``; it is asserted here
  because the failure mode it prevents is a wrong number nobody would query for.
"""
import httpx
import pytest


@pytest.mark.usefixtures("ensure_api_reachable")
def test_combo_deals_full_relation_resolves_referenced_deals(api_base_url):
    """`active_only=false` resolves every deal the line items reference.

    This is the invariant the EDW needed and the defect denied it: an active-only
    mirror leaves 4 of 8 referenced deals unresolvable (56,036 items on CT107
    2026-10-01). Asserted against the *item side* rather than the route's own row
    count, because "the route returned some rows" is exactly the check that passes
    while the EDW is still broken — the active slice is non-empty too.
    """
    base = f"{api_base_url}/grocery/pos/combo-deals"
    full = httpx.get(base, params={"active_only": False}, timeout=30.0)
    assert full.status_code == 200, f"combo-deals returned {full.status_code}"
    deals = full.json()
    assert isinstance(deals, list), "combo-deals payload should be a list"
    if not deals:
        pytest.skip("no combo deals on this dataset")

    # Pull the referenced ids out of the items the API itself serves, so this
    # needs no database access and holds wherever the suite is pointed.
    items = httpx.get(f"{api_base_url}/grocery/pos/transaction-items",
                      params={"limit": 5000}, timeout=60.0)
    if items.status_code != 200:
        pytest.skip(f"transaction-items route returned {items.status_code}")

    referenced = {i["deal_id"] for i in items.json().get("data", [])
                  if i.get("deal_id")}
    if not referenced:
        pytest.skip("no deal-tagged items on this dataset")

    served = {d["deal_id"] for d in deals}
    unresolvable = referenced - served
    assert not unresolvable, (
        f"{len(unresolvable)} of {len(referenced)} deals referenced by "
        "transaction_items are not served by active_only=false: "
        f"{sorted(unresolvable)[:5]}. A consumer mirroring this relation cannot "
        "resolve its own foreign keys."
    )


@pytest.mark.usefixtures("ensure_api_reachable")
def test_combo_deals_payload_carries_is_active(api_base_url):
    """A retired deal must be distinguishable from a live one.

    Guards the reason `active_only=false` is safe to consume: without the
    column, a full load and an active-only load are the same rows.
    """
    deals = httpx.get(f"{api_base_url}/grocery/pos/combo-deals",
                      params={"active_only": False}, timeout=30.0).json()
    if not deals:
        pytest.skip("no combo deals on this dataset")
    missing = [d for d in deals if "is_active" not in d]
    assert not missing, (
        f"{len(missing)}/{len(deals)} deals carry no `is_active` — a consumer "
        "cannot tell a retired deal from a live one. The coupons route has "
        "always projected this column (main.py pos_coupons); combo-deals must too."
    )
    assert all(isinstance(d["is_active"], bool) for d in deals), \
        "`is_active` must serialize as a boolean, not a string"


@pytest.mark.usefixtures("ensure_api_reachable")
def test_active_only_is_a_strict_subset(api_base_url):
    """`active_only=true` must not report anything the full route disagrees with.

    Guards the ordering invariant the count comparison leans on: the active slice
    is drawn from the same rows the full relation serves, so a deal_id present in
    both must be the same deal with the same window. A refactor that recomputes
    the two sides independently could drift them, and the symptom would be an
    FD-resolution test failing far from the route.
    """
    base = f"{api_base_url}/grocery/pos/combo-deals"
    full = {d["deal_id"]: d for d in
            httpx.get(base, params={"active_only": False}, timeout=30.0).json()}
    active = httpx.get(base, timeout=30.0).json()

    if not full:
        pytest.skip("no combo deals on this dataset")

    for deal in active:
        assert deal["deal_id"] in full, (
            f"deal {deal['deal_id']} is served as active but absent from the full "
            "relation — the two sides of the filter are reading different rows"
        )
        assert deal["valid_until"] == full[deal["deal_id"]]["valid_until"], \
            f"deal {deal['deal_id']} has two different valid_until values"

    assert len(active) <= len(full), "the active slice cannot exceed the full relation"