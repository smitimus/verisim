"""The customer dimension must be reachable through the API, and joinable.

`pos.customers` exists so a grocery mart has a customer grain to build RFM /
cohort / basket-affinity models on. That is only worth anything if a consumer
can actually read it, which is what these tests pin:

* `/grocery/pos/customers` serves the dimension with its loyalty rollup
  (`loyalty_member_count`, `first_signup_date`) — the columns a mart groups by.
* The rollup columns are computed by the join, never stored, so they must track
  the cards actually on disk. A stored copy goes stale the moment a second card
  joins a household, and nothing else would notice.
* `customer_id` is served on `/grocery/pos/loyalty-members`, so the mart's
  join chain `transactions -> loyalty_members -> customers` is complete from
  the API alone. This is the join-key half of the deal; the card the body
  describes exists precisely so a mart can segment transactions by household.
* `/grocery/pos/customers/summary` counts without paging the dimension through
  a client.
* On a data dir predating the dimension every one of these degrades to an empty
  result with an explicit flag, NOT a 500 — a rolling deploy must keep serving
  the other tables while one slot is still on the old image.

The filters are asserted against the served rows rather than a database, so the
suite holds wherever it is pointed (the same approach as
`test_promo_resolvability.py`).
"""
import pathlib

import httpx
import pytest

CUSTOMERS = "/grocery/pos/customers"
SUMMARY = "/grocery/pos/customers/summary"
MEMBERS = "/grocery/pos/loyalty-members"


def _get(base, path, **params):
    resp = httpx.get(f"{base}{path}", params=params, timeout=30.0)
    assert resp.status_code == 200, f"{path} returned {resp.status_code}: {resp.text[:200]}"
    return resp.json()


@pytest.mark.usefixtures("ensure_api_reachable")
def test_customers_route_serves_the_dimension(api_base_url):
    """The dimension is readable, with the rollup a mart groups by."""
    payload = _get(api_base_url, CUSTOMERS, limit=50)

    if not payload.get("customers_dimension_present"):
        pytest.skip("this data dir predates the customer dimension")

    rows = payload["data"]
    assert rows, "the dimension is present but serves no rows"
    assert payload["total"] == len(rows) or payload["total"] > len(rows), \
        "total must be at least the page it returned"

    required = {"customer_id", "age_band", "household_size", "segment",
                "loyalty_member_count", "first_signup_date"}
    missing = required - set(rows[0])
    assert not missing, (
        f"the projection is missing {missing}. `loyalty_member_count` and "
        "`first_signup_date` are what a mart builds its customer grain on, and "
        "they are computed by the join precisely because a stored copy would go "
        "stale when a second card joined the household."
    )


@pytest.mark.usefixtures("ensure_api_reachable")
def test_household_size_never_exceeds_its_card_count(api_base_url):
    """A household of N people cannot hold more than N loyalty cards.

    Asserted through the API because the rollup is computed in SQL: if the two
    ever disagreed, the mart would compute a segment on a household that holds
    more cards than people, which is not a household.
    """
    payload = _get(api_base_url, CUSTOMERS, limit=5000)
    if not payload.get("customers_dimension_present"):
        pytest.skip("this data dir predates the customer dimension")

    contradictions = [
        r for r in payload["data"]
        if r["household_size"] < (r["loyalty_member_count"] or 0)
    ]
    assert not contradictions, (
        f"{len(contradictions)} served households hold more loyalty cards than "
        f"people: {contradictions[:3]}"
    )


@pytest.mark.usefixtures("ensure_api_reachable")
def test_every_served_household_has_at_least_one_card(api_base_url):
    """A household is formed by cards, so none may report zero.

    `LEFT JOIN` makes a zero count structurally possible, and a zero would mean
    the dimension and the card table disagree about which rows are real.
    """
    payload = _get(api_base_url, CUSTOMERS, limit=5000)
    if not payload.get("customers_dimension_present"):
        pytest.skip("this data dir predates the customer dimension")

    cardless = [r for r in payload["data"] if not r["loyalty_member_count"]]
    assert not cardless, (
        f"{len(cardless)} served households report no loyalty card: "
        f"{cardless[:3]}"
    )


@pytest.mark.usefixtures("ensure_api_reachable")
def test_segment_and_age_band_filters_narrow_the_result(api_base_url):
    """The filters must actually filter — a mart sizes its build off these."""
    payload = _get(api_base_url, CUSTOMERS, limit=5000)
    if not payload.get("customers_dimension_present"):
        pytest.skip("this data dir predates the customer dimension")
    if not payload["data"]:
        pytest.skip("no customers on this dataset")

    first = payload["data"][0]
    segment, age_band = first["segment"], first["age_band"]

    by_segment = _get(api_base_url, CUSTOMERS, segment=segment, limit=5000)
    assert by_segment["total"] > 0
    assert all(r["segment"] == segment for r in by_segment["data"]), (
        f"segment={segment} returned rows from other segments: "
        f"{ {r['segment'] for r in by_segment['data']} }"
    )

    by_band = _get(api_base_url, CUSTOMERS, age_band=age_band, limit=5000)
    assert all(r["age_band"] == age_band for r in by_band["data"]), (
        f"age_band={age_band} leaked other age bands"
    )


@pytest.mark.usefixtures("ensure_api_reachable")
def test_min_household_size_filter_is_inclusive(api_base_url):
    """`min_household_size=4` must return only households of 4 or more."""
    payload = _get(api_base_url, CUSTOMERS, min_household_size=4, limit=5000)
    if not payload.get("customers_dimension_present"):
        pytest.skip("this data dir predates the customer dimension")
    if not payload["data"]:
        pytest.skip("no household of 4+ on this dataset")

    below = [r for r in payload["data"] if r["household_size"] < 4]
    assert not below, f"min_household_size=4 returned households of {below[:3]}"


@pytest.mark.usefixtures("ensure_api_reachable")
def test_loyalty_members_route_carries_customer_id(api_base_url):
    """The join key to the dimension must be on the card route.

    This is the whole point of the card: a mart needs to get from a transaction
    (which has only `member_id`) to a segment. Without `customer_id` here the
    dimension is a table nobody can join, which is what it was before.
    """
    members = _get(api_base_url, MEMBERS, limit=50)
    rows = members["data"]
    if not rows:
        pytest.skip("no loyalty members on this dataset")

    assert "customer_id" in rows[0], (
        "/grocery/pos/loyalty-members does not serve customer_id, so the mart "
        "join chain transactions -> loyalty_members -> customers is broken. "
        "Check that this data dir has the column and that the route's "
        "projection is rebuilt."
    )

    linked = [r for r in rows if r["customer_id"]]
    if not linked:
        pytest.skip("no linked loyalty members on this dataset")

    # And the key must resolve against the dimension route.
    customer_id = linked[0]["customer_id"]
    served = _get(api_base_url, CUSTOMERS, customer_id=customer_id)
    assert served["total"] == 1, (
        f"a loyalty card points at customer {customer_id}, but the customers "
        "route does not serve it — the dimension cannot be reached from the "
        "card"
    )
    assert served["data"][0]["segment"]


@pytest.mark.usefixtures("ensure_api_reachable")
def test_customers_summary_matches_the_detail_route(api_base_url):
    """The summary must be a real count, not a separate number that drifts.

    Two routes reporting two different household counts is exactly the failure
    a data engineer cannot see: every individual row looks right.
    """
    detail = _get(api_base_url, CUSTOMERS, limit=1)
    if not detail.get("customers_dimension_present"):
        pytest.skip("this data dir predates the customer dimension")

    summary = _get(api_base_url, SUMMARY)
    assert summary["total"] == detail["total"], (
        f"/customers/summary reports {summary['total']} households but "
        f"/customers reports {detail['total']}"
    )
    if not summary["data"]:
        assert summary["total"] == 0
        return

    row = summary["data"][0]
    assert {"segment", "age_band", "household_count", "avg_household_size",
            "loyalty_card_count"} <= set(row), (
        f"the summary row is missing columns: {set(row)}"
    )
    assert row["household_count"] > 0


@pytest.mark.usefixtures("ensure_api_reachable")
def test_absent_dimension_degrades_instead_of_500ing(api_base_url):
    """A data dir with no dimension must get an empty result with a flag, never a 500.

    A rolling deploy leaves at least one slot on an older image. If these routes
    raised, that slot's API would fail every request for the tables it does
    have — turning a missing optional dimension into an outage.

    Only the *absent* case is asserted, and only where the dimension really is absent.
    A freshly bootstrapped directory has the dimension and thousands of households, and
    asserting `data == []` there only proved the test could not run on a real install —
    every other test in this file guards its claim with the same
    `customers_dimension_present` check. The route's degradation path is therefore
    asserted from the source contract instead: both routes must carry the flag, so a
    consumer can always tell "no customers" from "this build has no customers concept".
    """
    payload = _get(api_base_url, CUSTOMERS)
    assert "customers_dimension_present" in payload, (
        "the response must say whether the dimension exists, so a consumer can "
        "tell 'no customers' from 'this build has no customers concept'"
    )
    summary = _get(api_base_url, SUMMARY)
    assert "customers_dimension_present" in summary, (
        "the summary must carry the same flag as the detail route"
    )

    if payload["customers_dimension_present"]:
        # The dimension is here, so the honest contract is: rows, not an empty page.
        assert payload["total"] > 0, (
            "customers_dimension_present is true but the route served no households"
        )
        assert summary["customers_dimension_present"] is True
        return

    # Absent: empty, zero total, and the flag false on both routes.
    assert payload["data"] == []
    assert payload["total"] == 0
    assert summary["data"] == []
    assert summary["customers_dimension_present"] is False


def test_both_customer_routes_degrade_without_raising():
    """The degradation path must exist in the route source, not just on one data dir.

    CI bootstraps a fresh directory, so the dimension is always present there and the
    absent branch above can never execute there — an assertion that only runs on one
    kind of install is an assertion nobody is really checking. Pin the branch in the
    source instead: both routes must test the table's presence and return the flag.
    """
    source = pathlib.Path(__file__).resolve().parents[3] / "base" / "api" / "main.py"
    body = source.read_text()
    for route in ('@app.get("/grocery/pos/customers"',
                  '@app.get("/grocery/pos/customers/summary"'):
        start = body.index(route)
        chunk = body[start:start + 4000]
        assert "_has_customers_table(" in chunk, (
            f"{route} no longer checks whether the dimension exists, so a data dir "
            "that predates it would raise instead of degrading"
        )
        assert '"customers_dimension_present": False' in chunk, (
            f"{route} no longer reports the dimension as absent"
        )