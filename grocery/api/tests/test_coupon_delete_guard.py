"""Deleting a coupon must not orphan the line items it discounted.

THE INCIDENT (t_f963eeb1)
========================
On CT106 the grocery generator stopped writing at 08:11:05 on 2026-10-04 and
never recovered. The first and only distinct error in the log was:

    insert or update on table "transaction_items"
    violates foreign key constraint "transaction_items_coupon_id_fkey"

The API log shows why, from 172.25.0.4 (airflow-worker — data-lab's EDW):

    DELETE /grocery/pos/coupons/297506d7-… 200 OK
    DELETE /grocery/pos/coupons/9e7fb627-… 200 OK
    DELETE /grocery/pos/coupons/37b2ca2e-… 200 OK   (+ 5 more)

`DELETE /grocery/pos/coupons/{id}` (`base/api/main.py`) issued an unguarded
`DELETE FROM pos.coupons`, while `pos.transaction_items.coupon_id` carries a
real FK to `pos.coupons` (`generator/schema.sql:230`). So the DELETE succeeded
for any coupon whose redemptions had *not yet* been written — the FK only bites
once a line references it — and the generator, which holds its own coupon set in
memory and refreshes it every 20 ticks, then tried to tag a line with an id that
no longer existed.

So the route let a perfectly ordinary consumer (an EDW fixture reset, here)
break the generator, with a 200 OK as the receipt. This test pins the contract
that closes it: the delete is refused while history depends on the row, and the
error says what to do instead.

Retire, do not delete
---------------------
`seed_coupons` already retires rather than deletes (`models/pos.py`), and its
own docstring says why: "expired-but-active rows used to block re-seeding
forever … the redemptions it earned are still on disk". `PATCH is_active=false`
is the supported retirement. So the fix is not to invent a purge path — it is to
make the destructive one fail loudly instead of quietly corrupting the writer.
"""
import httpx
import pytest

COUPONS = "/grocery/pos/coupons"


def _coupon_with_redemptions(client, timeout=60.0):
    """A coupon that at least one transaction item references.

    Sourced from the items the API itself serves, so the test needs no database
    access and holds wherever the suite is pointed. Returns None when this
    dataset has no coupon-tagged items — the invariant is then untestable here
    and skipping is honest, where inventing a fixture would not be.
    """
    items = client.get("/grocery/pos/transaction-items", params={"limit": 5000},
                       timeout=timeout)
    if items.status_code != 200:
        pytest.skip(f"transaction-items route returned {items.status_code}")
    referenced = {i["coupon_id"] for i in items.json().get("data", [])
                  if i.get("coupon_id")}
    return referenced or None


@pytest.mark.usefixtures("ensure_api_reachable")
def test_delete_refuses_a_coupon_history_still_references(api_base_url):
    """A referenced coupon cannot be deleted — the route must refuse.

    Asserted on the STATUS, because the pre-fix behaviour was a 200: the
    generator's FK is the only thing that notices, minutes later, in a
    different container, with the original cause long gone from the log.
    """
    with httpx.Client(base_url=api_base_url, timeout=60.0) as client:
        referenced = _coupon_with_redemptions(client)
        if not referenced:
            pytest.skip("no coupon-tagged items on this dataset")

        target = sorted(referenced)[0]
        resp = client.delete(f"{COUPONS}/{target}")

        # Measured on the pre-fix image, 2026-10-04: this returns 500 —
        # psycopg2's ForeignKeyViolation escapes as an unhandled
        # "Internal Server Error", so the caller gets no idea that the delete
        # was *refused* rather than broken, and nothing tells them the
        # supported alternative. (The 200 that took the generator down is the
        # other half: it lands when the coupon has no redemptions *yet*, and
        # the FK only bites once the generator writes the first one.)
        assert resp.status_code != 200, (
            f"DELETE {COUPONS}/{target} returned 200 for a coupon that "
            f"transaction_items already references. If that ever succeeds the "
            f"row is gone and every line item that used it dangles — which is "
            f"the state that stopped the generator on CT106. Retire it with "
            f"PATCH is_active=false instead."
        )
        # 409 Conflict is the precise code: the request is well-formed, the
        # resource exists, and the conflict is with the state of the world.
        assert resp.status_code == 409, (
            f"expected 409 Conflict, got {resp.status_code}: {resp.text[:300]}"
        )

        detail = resp.json().get("detail", "")
        assert "is_active" in detail or "retire" in detail.lower(), (
            f"the 409 must tell the caller what to do instead; got: {detail!r}"
        )


@pytest.mark.usefixtures("ensure_api_reachable")
def test_delete_still_works_for_an_unreferenced_coupon(api_base_url):
    """The guard must not turn DELETE into a route that always refuses.

    A create-then-delete of a coupon nothing references has to keep working,
    or this fix costs a real capability to prevent a rare one. The coupon is
    created through the API itself so the test leaves no residue beyond the row
    it removes.
    """
    with httpx.Client(base_url=api_base_url, timeout=60.0) as client:
        today = "2026-01-01"
        created = client.post(
            COUPONS,
            json={"code": "ZZDELETEPROBE01", "description": "delete-guard probe",
                  "coupon_type": "percent_off", "discount_value": 0.10,
                  "valid_from": today, "valid_until": "2099-01-01",
                  "is_active": True},
        )
        if created.status_code != 200:
            pytest.skip(f"could not create a probe coupon ({created.status_code})")
        coupon_id = created.json()["coupon_id"]

        try:
            resp = client.delete(f"{COUPONS}/{coupon_id}")
            assert resp.status_code == 200, (
                f"an unreferenced coupon must still be deletable, got "
                f"{resp.status_code}: {resp.text[:300]}"
            )
        finally:
            # If the delete was refused, clean up by retiring instead.
            if client.get(f"{COUPONS}/{coupon_id}").status_code == 200:
                client.patch(f"{COUPONS}/{coupon_id}", json={"is_active": False})


@pytest.mark.usefixtures("ensure_api_reachable")
def test_missing_coupon_is_still_a_404(api_base_url):
    """The guard must not swallow the not-found case.

    404 and 409 are different operator situations — nothing to delete vs. a
    delete that is refused — and conflating them sends a caller looking for a
    typo instead of at their own history.
    """
    with httpx.Client(base_url=api_base_url, timeout=30.0) as client:
        resp = client.delete(f"{COUPONS}/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404, (
            f"expected 404 for an unknown coupon, got {resp.status_code}"
        )