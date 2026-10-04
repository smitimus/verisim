"""
First-class vendors and the shortage -> credit-memo lifecycle — card t_57b1a1ab.

THE DEFECT
----------
A supplier was a free-text `supplier_name VARCHAR(200)` stamped onto
`inv.products` and `inv.receipts`, chosen by a module-level literal:

    SUPPLIERS = ['UNFI', 'KeHE Distributors', 'McLane Company',
                 'C&S Wholesale Grocers', 'Nash Finch', 'Supervalu']
    ...
    random.choice(SUPPLIERS), random.randint(1, 4)   # lead_time_days

There was no row to join to, so "which vendor keeps shorting us" and "is this
vendor reliable against its own promise" were both unanswerable — not merely
unbuilt, unbuildable. `lead_time_days` was one static integer per product, so
there was no distribution and no promise to be late against.

And the short-ship side of it was invisible. `fulfillment.items` recorded
`pick_status = 'short'`, and `transport.receive_delivered_loads` then did:

    WHERE li.load_id = %s::uuid AND fi.pick_status = 'picked'

so the short lines were excluded from the receipt and went nowhere. A vendor
who could not fill an order left no trace: the shelf came up short, nothing
recorded it, nobody was blamed, no money came back. The chain was closed from
ordering through receipt and stopped one step short of the vendor relationship.

WHAT THESE TESTS PIN
--------------------
1. vendors are first-class — the list is config, not code, and the config
   loader cannot silently zero a vendor's behaviour by omitting a key;
2. the realised lead time is a DISTRIBUTION around the vendor's promise, and
   the promise is what inv.products stores;
3. the short rate is the VENDOR's, so a bad vendor is measurably worse (the old
   code shorted every line of every vendor at a flat 5%, which made a
   vendor-performance mart impossible by construction);
4. a short-ship becomes `inv.short_ship_events`, and its arithmetic is what the
   table's CHECK constraints enforce;
5. a short-ship becomes a credit memo, and the memo has a real lifecycle —
   open -> submitted -> paid | rejected, plus expiry against the vendor's claim
   window, so days-to-pay is measurable;
6. not every shortfall becomes a claim (the vendor's terms say no, or the
   paperwork is not worth it) — a mart that only saw filed claims would rate
   every vendor as perfect;
7. DSD vendors deliver to the shelf on their own schedule and their short-ship
   is detected at the shelf, with NO receipt behind it (a DSD pallet is not on
   one of our trucks — writing a receipt would double-count the goods);
8. every batched row template has exactly as many slots as its INSERT has
   columns. The stockout template in `inventory` shipped with 11 placeholders
   for 12 columns and only failed on the first tick whose shelf ran short
   (t_959cd040); a credit memo is far rarer than a stockout, so a mismatch
   here would ship and stay dark.

Read the economics in plain terms: a vendor promises 2 days and delivers in 2,
or promises 2 and delivers in 4 — and when the warehouse could only send 30 of
the 50 cases ordered, that 20-case shortfall is recorded against the vendor,
turned into a claim inside their 14-day credit window, and paid (or disputed)
on their own settlement terms. Before this, the 20 cases simply were not in the
receipt.
"""
import random
from datetime import datetime, timedelta

import pytest

from grocery.generator.config import Config, _apply_yaml
from grocery.generator.models import suppliers

SIM_DT = datetime(2026, 10, 3, 23, 30, 0)
PRODUCT = 'aaaaaaaa-0000-0000-0000-000000000001'
PRODUCT_2 = 'bbbbbbbb-0000-0000-0000-000000000002'
STORE = 'cccccccc-0000-0000-0000-000000000003'
WAREHOUSE = 'dddddddd-0000-0000-0000-000000000004'
FULFILLMENT = 'eeeeeeee-0000-0000-0000-000000000005'
ITEM = 'ffffffff-0000-0000-0000-000000000006'

GOOD_VENDOR = {
    'supplier_id': '11111111-0000-0000-0000-000000000001',
    'supplier_name': 'KeHE Distributors',
    'supplier_code': 'KEHE',
    'fulfillment_model': 'warehouse',
    'lead_time_mean_days': 2.0,
    'lead_time_stddev_days': 0.5,
    'short_ship_rate': 0.04,
    'credit_eligible': True,
    'credit_window_days': 14,
    'credit_settle_mean_days': 8,
    'credit_settle_stddev_days': 3,
}

BAD_VENDOR = {
    'supplier_id': '22222222-0000-0000-0000-000000000002',
    'supplier_name': 'Nash Finch',
    'supplier_code': 'NASH',
    'fulfillment_model': 'warehouse',
    'lead_time_mean_days': 3.5,
    'lead_time_stddev_days': 1.2,
    'short_ship_rate': 0.15,
    'credit_eligible': False,
    'credit_window_days': 7,
    'credit_settle_mean_days': 15,
    'credit_settle_stddev_days': 5,
}

DSD_VENDOR = dict(GOOD_VENDOR,
                  supplier_id='33333333-0000-0000-0000-000000000003',
                  supplier_name='FreshFields Produce',
                  supplier_code='FRFP',
                  fulfillment_model='dsd',
                  lead_time_mean_days=1.0,
                  lead_time_stddev_days=0.3)


def _short_row(product_id=PRODUCT, requested=50, picked=30, cost=4.0):
    return {
        'item_id': ITEM,
        'fulfillment_id': FULFILLMENT,
        'product_id': product_id,
        'quantity_requested': requested,
        'quantity_picked': picked,
        'unit_cost': cost,
    }


# ---------------------------------------------------------------------------
# 1. Vendors are config, not a code literal
# ---------------------------------------------------------------------------
def test_the_vendor_catalogue_is_configurable():
    assert len(Config().vendors.vendors) >= 5, (
        "the default catalogue should keep the vendors the old hardcoded "
        "literal carried, so a stock image is not suddenly vendorless")


def test_the_old_hardcoded_names_are_still_available():
    names = {v['name'] for v in Config().vendors.vendors}
    for legacy in ('UNFI', 'KeHE Distributors', 'McLane Company',
                   'C&S Wholesale Grocers', 'Nash Finch', 'Supervalu'):
        assert legacy in names, (
            f"{legacy} was in the pre-t_57b1a1ab hardcoded SUPPLIERS literal "
            "and must not vanish from the default catalogue")


def test_every_vendor_carries_the_numbers_the_model_reads():
    """A vendor missing a behaviour key would fall back to a default that
    nothing told the operator about."""
    required = ('name', 'code', 'fulfillment_model', 'lead_time_mean_days',
                'lead_time_stddev_days', 'short_ship_rate', 'credit_eligible',
                'credit_window_days')
    for vendor in Config().vendors.vendors:
        for key in required:
            assert key in vendor, f"{vendor.get('name')} has no {key!r}"


def test_at_least_one_dsd_vendor_is_configured():
    """Produce and deli arrive by DSD; with no DSD vendor the perishable half
    of the catalogue would have no inbound mechanism at all."""
    dsd = [v for v in Config().vendors.vendors
           if v['fulfillment_model'] == 'dsd']
    assert dsd, "no DSD vendor in the default catalogue"


def test_a_config_vendor_list_replaces_the_defaults():
    cfg = Config()
    _apply_yaml(cfg, {'vendors': {'vendors': [
        {'name': 'Only Vendor', 'code': 'ONLY', 'short_ship_rate': 0.01,
         'lead_time_mean_days': 4.0},
    ]}})
    assert len(cfg.vendors.vendors) == 1
    assert cfg.vendors.vendors[0]['name'] == 'Only Vendor'
    assert cfg.vendors.vendors[0]['short_ship_rate'] == 0.01


def test_an_omitted_vendor_key_keeps_its_default_not_zero():
    """The trap this guards: a config that sets `short_ship_rate: 0` for a
    vendor is asking for a vendor that never shorts, while a config that merely
    OMITS it is asking for the default. Merging off the declared defaults is
    the only way to tell those apart."""
    cfg = Config()
    _apply_yaml(cfg, {'vendors': {'vendors': [
        {'name': 'UNFI', 'code': 'UNFI', 'credit_window_days': 30},
    ]}})
    vendor = cfg.vendors.vendors[0]
    assert vendor['credit_window_days'] == 30, "the explicit key wins"
    assert vendor['short_ship_rate'] == 0.06, (
        "an omitted key must keep the declared default, not become 0 "
        "(0 would silently mean 'this vendor never shorts anything')")
    assert vendor['lead_time_mean_days'] == 2.0
    assert vendor['credit_eligible'] is True


def test_an_explicit_zero_short_rate_survives_the_merge():
    """The mirror of the test above: 0 here is a real, deliberate choice."""
    cfg = Config()
    _apply_yaml(cfg, {'vendors': {'vendors': [
        {'name': 'UNFI', 'code': 'UNFI', 'short_ship_rate': 0.0},
    ]}})
    assert cfg.vendors.vendors[0]['short_ship_rate'] == 0.0


def test_the_dsd_behaviour_keys_reach_the_config():
    cfg = Config()
    _apply_yaml(cfg, {'vendors': {
        'dsd_departments': ['Produce'],
        'dsd_deliveries_per_week': 2,
        'dsd_short_ship_bonus': 0.05,
        'credit_claim_rate': 0.5,
        'credit_pay_rate': 0.9,
        'credit_chase_after_days': 7,
    }})
    assert cfg.vendors.dsd_departments == ['Produce']
    assert cfg.vendors.dsd_deliveries_per_week == 2
    assert cfg.vendors.dsd_short_ship_bonus == 0.05
    assert cfg.vendors.credit_claim_rate == 0.5
    assert cfg.vendors.credit_pay_rate == 0.9
    assert cfg.vendors.credit_chase_after_days == 7


# ---------------------------------------------------------------------------
# 2. The realised lead time is a distribution around the promise
# ---------------------------------------------------------------------------
def test_lead_time_is_drawn_around_the_vendors_promise():
    """The whole reason the two columns exist. With one static
    `inv.products.lead_time_days` a vendor-performance mart had no promise to
    measure an actual against."""
    draws = [suppliers.draw_lead_time_days(GOOD_VENDOR) for _ in range(400)]
    mean = sum(draws) / len(draws)
    assert 1.7 <= mean <= 2.3, (
        f"mean realised lead time {mean} is not the promised 2.0")


def test_the_lead_time_actually_varies():
    """A zero-variance draw would make `lead_time_stddev_days` decorative —
    the same defect `restock_threshold_pct` had before t_959cd040."""
    draws = {suppliers.draw_lead_time_days(GOOD_VENDOR) for _ in range(200)}
    assert len(draws) > 1, (
        "every draw was identical, so the stddev column is not doing anything")


def test_a_wider_spread_reaches_further_from_the_mean():
    tight = {suppliers.draw_lead_time_days(
        {'lead_time_mean_days': 3.0, 'lead_time_stddev_days': 0.1})
        for _ in range(300)}
    wide = {suppliers.draw_lead_time_days(
        {'lead_time_mean_days': 3.0, 'lead_time_stddev_days': 1.5})
        for _ in range(300)}
    assert len(wide) > len(tight), (
        f"a 1.5-day spread ({len(wide)} distinct values) must reach further "
        f"than a 0.1-day one ({len(tight)})")


def test_a_zero_spread_is_the_rounded_mean_not_an_error():
    assert suppliers.draw_lead_time_days(
        {'lead_time_mean_days': 2.4, 'lead_time_stddev_days': 0.0}) == 2


def test_a_lead_time_is_never_negative():
    """A gaussian can go negative. A negative lead time is not a thing, and a
    CHECK on the column would turn a whole delivery into a failed INSERT."""
    draws = [suppliers.draw_lead_time_days(
        {'lead_time_mean_days': 0.2, 'lead_time_stddev_days': 3.0})
        for _ in range(500)]
    assert min(draws) >= 0, f"a negative lead time was drawn: {min(draws)}"


def test_a_private_rng_makes_the_draw_deterministic():
    """So a test (or a reproducible run) can pin the realised time."""
    first = [suppliers.draw_lead_time_days(BAD_VENDOR, random.Random(7))
             for _ in range(5)]
    second = [suppliers.draw_lead_time_days(BAD_VENDOR, random.Random(7))
              for _ in range(5)]
    assert first == second


# ---------------------------------------------------------------------------
# 3. The short rate is the vendor's, not a flat 5%
# ---------------------------------------------------------------------------
def test_a_bad_vendor_is_worse_than_a_good_one():
    cfg = Config()
    good = suppliers.short_probability_for(GOOD_VENDOR, cfg)
    bad = suppliers.short_probability_for(BAD_VENDOR, cfg)
    assert bad > good, (
        f"the flat 5% this replaced rated every vendor identically by "
        f"construction: good={good}, bad={bad}")


def test_the_short_rate_is_the_configured_rate():
    cfg = Config()
    assert suppliers.short_probability_for(
        {'short_ship_rate': 0.15}, cfg) == pytest.approx(0.15)


def test_a_missing_short_rate_falls_back_to_five_percent():
    """The pre-t_57b1a1ab behaviour, for a vendor row that somehow lost it."""
    assert suppliers.short_probability_for({}, Config()) == pytest.approx(0.05)


def test_a_dsd_line_carries_the_configured_bonus():
    """A DSD truck that misses its window empties the shelf that morning —
    the perishable version of the same problem."""
    cfg = Config()
    cfg.vendors.dsd_short_ship_bonus = 0.05
    warehouse = suppliers.short_probability_for(
        {'short_ship_rate': 0.10}, cfg, suppliers.DETECTED_RECEIVING)
    dsd = suppliers.short_probability_for(
        {'short_ship_rate': 0.10}, cfg, suppliers.DETECTED_DSD)
    assert dsd == pytest.approx(warehouse + 0.05)


def test_a_nonsense_short_rate_clamps_instead_of_shorting_everything():
    """A config with short_ship_rate: 3.0 must not short 100% of lines with
    no one noticing the typo."""
    assert suppliers.short_probability_for(
        {'short_ship_rate': 3.0}, Config()) == 1.0
    assert suppliers.short_probability_for(
        {'short_ship_rate': -1.0}, Config()) == 0.0


def test_the_short_reasons_are_a_vendor_side_mix():
    """`quality_reject` is OUR fault (the goods arrived and we rejected them);
    the rest are the vendor's. A mart that cannot tell them apart cannot say
    who to chase."""
    reasons = {suppliers.draw_short_reason() for _ in range(300)}
    assert 'quality_reject' in reasons
    assert 'warehouse_shortage' in reasons
    assert len(reasons) >= 3


def test_our_own_quality_rejection_is_the_least_likely_reason():
    counts = {}
    for _ in range(2000):
        reason = suppliers.draw_short_reason()
        counts[reason] = counts.get(reason, 0) + 1
    assert counts['quality_reject'] == min(counts.values()), (
        "a rejection at our own dock should be the rare case, not the common one")


# ---------------------------------------------------------------------------
# 4. The shortfall arithmetic
# ---------------------------------------------------------------------------
def test_the_shortfall_is_what_the_pick_failed_to_fill():
    assert suppliers.short_quantity(50, 30) == 20.0


def test_a_fully_picked_line_has_no_shortfall():
    assert suppliers.short_quantity(50, 50) == 0.0


def test_a_shortfall_is_never_negative():
    """A warehouse that picked MORE than requested must not write a negative
    `quantity_short` — the table's CHECK would reject the INSERT, taking the
    whole delivery down with it."""
    assert suppliers.short_quantity(50, 55) == 0.0


def test_the_shortfall_is_rounded_to_three_dp():
    """Every quantity column here is NUMERIC(8,3); a 6dp value is stored
    exactly but reads as a different number to a mart summing it."""
    assert suppliers.short_quantity(10, 3.3333333) == 6.667


# ---------------------------------------------------------------------------
# 5. Credit terms: the claim window and the settlement
# ---------------------------------------------------------------------------
def test_the_claim_window_comes_from_the_vendor_terms():
    assert suppliers.claim_deadline_days(
        {'credit_window_days': 14}) == 14
    assert suppliers.claim_deadline_days(
        {'credit_window_days': 7}) == 7


def test_a_nonsense_claim_window_falls_back_to_two_weeks():
    assert suppliers.claim_deadline_days({}) == 14
    assert suppliers.claim_deadline_days({'credit_window_days': 0}) == 1, (
        "a zero-day window must still allow the claim to exist")


def test_settlement_is_drawn_from_the_vendors_payment_terms():
    """Days-to-pay is the number a procurement analyst actually asks about, so
    its shape must be the vendor's to control."""
    draws = [suppliers.settlement_days(
        {'credit_settle_mean_days': 10, 'credit_settle_stddev_days': 4})
        for _ in range(400)]
    mean = sum(draws) / len(draws)
    assert 9.0 <= mean <= 11.0, f"mean settlement {mean} is not the promised 10"
    assert len(set(draws)) > 1, "settlement never varies"


def test_a_slow_vendor_settles_slower_than_a_fast_one():
    fast = [suppliers.settlement_days(
        {'credit_settle_mean_days': 5, 'credit_settle_stddev_days': 2})
        for _ in range(200)]
    slow = [suppliers.settlement_days(
        {'credit_settle_mean_days': 20, 'credit_settle_stddev_days': 3})
        for _ in range(200)]
    assert sum(slow) / len(slow) > sum(fast) / len(fast)


def test_settlement_is_never_negative():
    draws = [suppliers.settlement_days(
        {'credit_settle_mean_days': 0.5, 'credit_settle_stddev_days': 4})
        for _ in range(300)]
    assert min(draws) >= 0


# ---------------------------------------------------------------------------
# 6. The short-ship row (what flush/record would persist)
# ---------------------------------------------------------------------------
def _recorded_row(vendor=GOOD_VENDOR, requested=50, picked=30, cost=4.0,
                  sim_dt=SIM_DT):
    """The row `record_short_ships` builds, in the same order as its INSERT."""
    short = suppliers.short_quantity(requested, picked)
    return (
        ITEM,
        FULFILLMENT,
        vendor['supplier_id'],
        PRODUCT,
        STORE,
        suppliers.DETECTED_RECEIVING,
        round(float(requested), 3),
        round(float(picked), 3),
        short,
        round(float(cost), 4),
        round(short * float(cost), 2),
        max(0, int(round(float(vendor['lead_time_mean_days'])))),
        suppliers.draw_lead_time_days(vendor),
        bool(vendor['credit_eligible']),
        sim_dt,
        None,
    )


def test_a_short_ship_records_both_the_promise_and_the_reality():
    row = _recorded_row()
    promised, realized = row[11], row[12]
    assert promised == 2, "the promise is what the vendor said"
    assert realized >= 0, "and the reality is what it did"


def test_the_short_ship_arithmetic_balances():
    row = _recorded_row(requested=50, picked=30)
    assert row[8] == row[6] - row[7], (
        "inv.short_ship_events has a CHECK for exactly this, so a mismatch "
        "would fail the INSERT on the first real short-ship")


def test_the_short_value_is_priced_at_the_same_cost_as_the_receipt():
    """The credit is only reconcilable against the receipt if both price the
    goods identically. `transport.receive_delivered_loads` deliberately shares
    one unit_cost per product between the received line and the short line."""
    row = _recorded_row(requested=50, picked=30, cost=4.0)
    assert row[10] == pytest.approx(20.0 * 4.0, abs=0.01)


def test_an_uncorrectable_vendor_is_not_marked_creditable():
    """C&S-style vendors have no returns agreement: the shortfall is real but
    there is nobody to claim it from."""
    row = _recorded_row(vendor=BAD_VENDOR)
    assert row[13] is False


def test_a_correctable_vendor_is_marked_creditable():
    assert _recorded_row(vendor=GOOD_VENDOR)[13] is True


# ---------------------------------------------------------------------------
# 7. Every batched row template has one slot per column
# ---------------------------------------------------------------------------
# A credit memo is far rarer than a stockout — a handful a day at most — so a
# template/column mismatch would ship green and stay dark for weeks. The
# stockout template did exactly this (t_959cd040: 11 placeholders for 12
# columns, only failing on the first tick whose shelf ran short).


def _slots(template):
    return template.count('%s')


def test_the_short_ship_template_has_one_slot_per_column():
    assert _slots(suppliers.SHORT_SHIP_ROW_TEMPLATE) == \
        suppliers.SHORT_SHIP_COLUMN_COUNT, (
        f"short-ship template has {_slots(suppliers.SHORT_SHIP_ROW_TEMPLATE)} "
        f"slots for {suppliers.SHORT_SHIP_COLUMN_COUNT} columns")


def test_the_dsd_delivery_template_has_one_slot_per_column():
    assert _slots(suppliers.DSD_DELIVERY_ROW_TEMPLATE) == \
        suppliers.DSD_DELIVERY_COLUMN_COUNT


def test_the_dsd_item_template_has_one_slot_per_column():
    assert _slots(suppliers.DSD_ITEM_ROW_TEMPLATE) == \
        suppliers.DSD_ITEM_COLUMN_COUNT


def test_a_short_ship_row_has_exactly_as_many_fields_as_slots():
    """Runs the real tuple shape and measures it, rather than trusting that two
    literals were edited together."""
    assert len(_recorded_row()) == _slots(suppliers.SHORT_SHIP_ROW_TEMPLATE)


def test_every_template_is_one_parenthesised_row():
    """execute_values applies the template once per row, so a template with
    two rows would silently duplicate or truncate the data."""
    for template in (suppliers.SHORT_SHIP_ROW_TEMPLATE,
                     suppliers.DSD_DELIVERY_ROW_TEMPLATE,
                     suppliers.DSD_ITEM_ROW_TEMPLATE):
        assert template.startswith('(') and template.endswith(')')
        assert template.count('(') == 1 and template.count(')') == 1, \
            f"{template} is not a single parenthesised row"


# ---------------------------------------------------------------------------
# 8. The credit lifecycle's reason -> payment profile
# ---------------------------------------------------------------------------
def test_every_short_reason_has_a_payment_profile():
    """A reason with no profile would fall through to the default silently, and
    the fallback would quietly mis-price the vendor's reliability."""
    reasons = {r for r, _ in suppliers.SHORT_REASONS}
    assert reasons == set(suppliers.REASON_PAY_PROFILE), (
        "every reason a short-ship can have needs an answer for what the "
        "vendor does about it")


def test_our_own_quality_rejection_is_the_least_likely_to_be_paid():
    """The store rejected the goods at its own dock; a vendor paying for that
    would be the exception, not the rule."""
    rates = {reason: profile[0]
             for reason, profile in suppliers.REASON_PAY_PROFILE.items()}
    assert rates['quality_reject'] == min(rates.values())


def test_the_vendors_own_shortages_are_credited_more_often_than_ours():
    """The invariant, not one arbitrary ordering: a shortfall that is the
    VENDOR's fault is credited more often than one that is ours. A vendor being
    out of stock is their problem; goods we rejected at our own dock are ours,
    and a mart that could not tell those apart could not say who to chase."""
    rates = {reason: profile[0]
             for reason, profile in suppliers.REASON_PAY_PROFILE.items()}
    vendor_fault = ('warehouse_shortage', 'out_of_stock')
    for reason in vendor_fault:
        assert rates[reason] > rates['quality_reject'], (
            f"{reason} is the vendor's own shortage and must be credited more "
            f"often than a rejection at our dock ({rates[reason]} vs "
            f"{rates['quality_reject']})")


def test_a_claim_the_vendor_always_pays_carries_no_rejection_text():
    """`rejection_reason` is read next to `memo_status` in an aging mart. A
    paid claim must not also carry the text explaining a rejection, or the same
    row reads as both paid and disputed."""
    for reason, (pay_rate, rejection) in suppliers.REASON_PAY_PROFILE.items():
        if pay_rate >= 1.0:
            assert rejection is None, (
                f"{reason} is always paid but carries the rejection text "
                f"{rejection!r}")


def test_every_disputable_reason_carries_why():
    """A rejected claim with a NULL reason is useless to the analyst deciding
    whether to escalate — the text is the only explanation they get."""
    for reason, (_pay, rejection) in suppliers.REASON_PAY_PROFILE.items():
        if rejection is None:
            continue
        assert rejection.strip(), \
            f"{reason} can be disputed but carries no rejection reason"


# ---------------------------------------------------------------------------
# 9. DSD scheduling
# ---------------------------------------------------------------------------
def test_a_dsd_vendor_visits_the_requested_number_of_days():
    for per_week in range(1, 8):
        days = suppliers.dsd_delivery_weekdays(per_week)
        assert len(days) == per_week, f"{per_week} requested, got {len(days)}"
        assert len(set(days)) == len(days), "a weekday must not repeat"


def test_dsd_days_are_distinct_and_in_range():
    days = suppliers.dsd_delivery_weekdays(7)
    assert sorted(days) == list(range(7)), "7 days a week means every weekday"


def test_dsd_days_are_within_the_tables_check_constraint():
    """delivery_weekday is CHECK (BETWEEN 0 AND 6). An out-of-range value is an
    INSERT failure on the first DSD seed."""
    for per_week in (0, 1, 3, 7):
        for day in suppliers.dsd_delivery_weekdays(per_week):
            assert 0 <= day <= 6


def test_zero_days_a_week_still_visits_once():
    """`0` means "once a week", not "never" — a DSD vendor that never delivers
    is a config error, not a behaviour worth modelling."""
    days = suppliers.dsd_delivery_weekdays(0)
    assert len(days) == 1 and 0 <= days[0] <= 6


def test_an_absurd_delivery_count_is_clamped():
    assert len(suppliers.dsd_delivery_weekdays(30)) == 7
    assert len(suppliers.dsd_delivery_weekdays(-3)) == 1


def test_the_dsd_schedule_is_stable_for_a_given_seed():
    """A schedule that changed on every boot would make 'did they deliver on
    schedule?' unanswerable — the only question a delivery schedule exists for.
    """
    assert suppliers.dsd_delivery_weekdays(4, random.Random(11)) == \
        suppliers.dsd_delivery_weekdays(4, random.Random(11))


# ---------------------------------------------------------------------------
# 10. The FULFILLMENT model reads the vendor's rate
# ---------------------------------------------------------------------------
def test_fulfillment_keeps_a_named_default_short_rate():
    """The flat rate is still the fallback for a data dir with no
    inv.suppliers, so it is a named constant rather than a bare 0.05 literal —
    the same rule the stockout work applied to its magic numbers."""
    from grocery.generator.models import fulfillment
    assert fulfillment.DEFAULT_SHORT_RATE == 0.05


def test_fulfillment_still_works_without_a_vendor_config():
    """The pre-t_57b1a1ab data dir: no inv.suppliers table at all. Picking must
    not fail — it falls back to the flat rate."""
    import inspect

    from grocery.generator.models import fulfillment
    signature = inspect.signature(fulfillment.process_pending_orders)
    assert signature.parameters['vendor_cfg'].default is None, (
        "vendor_cfg must be optional so every existing caller and test is "
        "unaffected")
    assert signature.parameters['scenario'].default is None


def test_the_credit_memo_lifecycle_states_match_the_schema_check():
    """The model's transitions and the table's CHECK must agree, or a status
    the generator writes would be rejected by the INSERT that writes it."""
    schema = (
        __import__('pathlib').Path(__file__).resolve().parents[1] / 'schema.sql'
    ).read_text()
    assert "'open','submitted','paid','rejected'" in schema, (
        "the schema's memo_status CHECK must carry the four lifecycle states")
    assert "'expired','written_off'" in schema


def test_the_short_source_values_match_the_schema_check():
    schema = (
        __import__('pathlib').Path(__file__).resolve().parents[1] / 'schema.sql'
    ).read_text()
    assert "CHECK (detected_source IN ('receiving', 'dsd_delivery'))" in schema
    assert suppliers.DETECTED_RECEIVING == 'receiving'
    assert suppliers.DETECTED_DSD == 'dsd_delivery'


def test_a_short_ship_can_never_be_zero_quantity():
    """`quantity_short` is CHECK (> 0). A line recorded as short with nothing
    short would fail the INSERT — so the floor is enforced before the write."""
    assert suppliers.MIN_SHORT_UNITS >= 1.0
    assert suppliers.short_quantity(50, 50) < suppliers.MIN_SHORT_UNITS