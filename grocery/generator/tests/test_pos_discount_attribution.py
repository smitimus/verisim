"""
Regression tests for verisim card t_0a72c6a5 — the discount money must reach
the line.

Before this card `pos.transaction_items.discount` was 0 on every one of 7.67 M
rows of a normal backfill: the only thing that ever wrote a non-zero value was a
scenario department promotion, while the *header* carried $5.1 M of
`coupon_savings` + `deal_savings` and 334 k lines carried a `coupon_id`. The
money existed, it just never reached the line it belonged to — so four data-lab
marts published a structurally-zero discount measure as if it were a fact.

The contract these tests pin:

    transaction_items.discount   = line-level DOLLAR amount discounted
    transaction_items.line_total = quantity * unit_price - discount
    transactions.subtotal        = sum(line_total) + coupon_savings + deal_savings
    total                        = subtotal + tax - coupon_savings - deal_savings

and, per transaction, `sum(discount)` = `coupon_savings + deal_savings` to the
cent. `subtotal` deliberately keeps its old meaning (line net *before* the
transaction-level coupon/deal) so data-lab's existing revenue identity is
untouched; the gross is `quantity * unit_price`.
"""
from datetime import datetime, date
from unittest.mock import patch

import random

import pytest

import grocery.generator.models.pos as pos
from grocery.generator.config import Config


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, *a, **k):
        pass

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class _FakeConn:
    def cursor(self, *a, **k):
        return _FakeCursor()

    def commit(self):
        pass


def _run(count=1, products=None, coupons=None, deals=None, members=None,
         promotions=None, coupon_use_rate=0.0, combo_use_rate=0.0, promo_tag='normal'):
    """Run the POS generator against a stub connection and return what it wrote.

    Returns ``(headers, items)``: the `pos.transactions` tuples and the
    `pos.transaction_items` tuples, in insert order.
    """
    cfg = Config()
    cfg.coupons.coupon_use_rate = coupon_use_rate
    cfg.combo_deals.combo_use_rate = combo_use_rate

    class _Scenario:
        active_promotions = promotions or []
        coupon_multiplier = 1.0
        loyalty_engagement_modifier = 1.0
        scenario_tag = promo_tag

    headers, items = [], []

    def fake_execute_values(cur, sql, records, template=None):
        if 'transaction_items' in sql:
            items.extend(records)
        elif 'pos.transactions' in sql:
            headers.extend(records)

    conn = _FakeConn()
    with patch('grocery.generator.models.pos.execute_values',
               side_effect=fake_execute_values):
        pos.generate_pos_transactions(
            conn, cfg, datetime(2026, 6, 1, 12, 0, 0), count, _Scenario(),
            [{'location_id': 'loc1', 'location_type': 'store'}],
            products, [{'department': 'store', 'location_type': 'store',
                        'employee_id': 'e1', 'location_id': 'loc1'}],
            members if members is not None else [],
            coupons or [], deals or [],
        )
    return headers, items


def _products(n=4, price=2.00, department='Produce', department_id='d-produce'):
    return [
        {
            'product_id': f'p{i}',
            'department': department,
            'department_id': department_id,
            'uom': 'each',
            'price': price,
            'current_price': price,
        }
        for i in range(n)
    ]


def _coupon(cid='c1', coupon_type='percent_off', value=0.10,
            department_id=None, product_id=None):
    return {
        'coupon_id': cid,
        'coupon_type': coupon_type,
        'discount_value': value,
        'department_id': department_id,
        'product_id': product_id,
        'valid_from': date(2026, 5, 1),
        'valid_until': date(2026, 7, 1),
    }


def _deal(did='d1', trigger_qty=2, deal_price=1.0,
          trigger_department_id=None, trigger_product_id=None):
    return {
        'deal_id': did,
        'deal_type': 'x_for_price',
        'trigger_qty': trigger_qty,
        'trigger_department_id': trigger_department_id,
        'trigger_product_id': trigger_product_id,
        'deal_price': deal_price,
        'valid_from': date(2026, 5, 1),
        'valid_until': date(2026, 7, 1),
    }


def _groups(items):
    """transaction_id -> its line tuples."""
    out = {}
    for rec in items:
        out.setdefault(rec[0], []).append(rec)
    return out


def _assert_lines_balance(rec):
    """One line: line_total = quantity * unit_price - discount, and >= 0."""
    txn_id, product_id, qty, unit_price, discount, coupon_id, deal_id, line_total = rec
    expected = round(pos._line_gross_cents(qty, unit_price) / 100.0 - discount, 2)
    assert round(line_total, 2) == expected, (
        f'line {product_id} of {txn_id}: line_total {line_total} != '
        f'{expected} (qty {qty} x {unit_price} - {discount})'
    )
    assert line_total >= 0, f'line {product_id} went negative: {line_total}'


# ---------------------------------------------------------------------------
# The defect itself
# ---------------------------------------------------------------------------

def test_coupon_money_reaches_the_line(monkeypatch):
    """A coupon-tagged line carries a non-zero discount, and the header agrees."""
    random.seed(3)
    monkeypatch.setattr(random, 'random', lambda: 0.0)  # force member + promo

    headers, items = _run(
        count=4, products=_products(),
        coupons=[_coupon()], members=[{'member_id': 'm1'}],
        coupon_use_rate=1.0,
    )

    assert headers, 'no transactions generated'
    assert items, 'no transaction_items generated'

    tagged = [rec for rec in items if rec[5] is not None]
    assert tagged, 'coupon was applied but no line carries coupon_id'
    assert all(rec[4] > 0 for rec in tagged), (
        'a coupon-tagged line carries discount = 0 — the t_0a72c6a5 defect'
    )

    for rec in items:
        _assert_lines_balance(rec)

    for txn in headers:
        txn_id, subtotal, coupon_savings, deal_savings = txn[0], txn[5], txn[6], txn[7]
        gross_lines = round(sum(r[2] * r[3] for r in _groups(items)[txn_id]), 2)
        discounts = round(sum(r[4] for r in _groups(items)[txn_id]), 2)
        line_totals = round(sum(r[7] for r in _groups(items)[txn_id]), 2)
        assert coupon_savings > 0, f'{txn_id}: coupon not booked on the header'
        # sum(discount) == the header's promotion savings, to the cent
        assert abs(discounts - (coupon_savings + deal_savings)) <= 0.01, (
            f'{txn_id}: line discounts {discounts} != header savings '
            f'{coupon_savings + deal_savings}'
        )
        # and the line net is the header subtotal less those savings
        assert abs(line_totals - (subtotal - coupon_savings - deal_savings)) <= 0.01
        # subtotal is the line net BEFORE the transaction-level promo
        assert abs(subtotal - (gross_lines - discounts + coupon_savings + deal_savings)) <= 0.01


def test_deal_money_reaches_the_line(monkeypatch):
    random.seed(5)
    monkeypatch.setattr(random, 'random', lambda: 0.0)

    headers, items = _run(
        count=4, products=_products(),
        deals=[_deal(trigger_qty=2, deal_price=1.0)],
        combo_use_rate=1.0,
    )

    assert items, 'no transaction_items generated'
    tagged = [rec for rec in items if rec[6] is not None]
    assert tagged, 'deal was applied but no line carries deal_id'
    assert all(rec[4] > 0 for rec in tagged), (
        'a deal-tagged line carries discount = 0 — the t_0a72c6a5 defect'
    )
    for rec in items:
        _assert_lines_balance(rec)

    groups = _groups(items)
    for txn in headers:
        txn_id, subtotal, coupon_savings, deal_savings = txn[0], txn[5], txn[6], txn[7]
        lines = groups[txn_id]
        assert deal_savings > 0, f'{txn_id}: deal not booked on the header'
        assert abs(sum(r[4] for r in lines) - (coupon_savings + deal_savings)) <= 0.01
        assert abs(sum(r[7] for r in lines)
                   - (subtotal - coupon_savings - deal_savings)) <= 0.01


def test_coupon_and_deal_stack_and_still_reconcile(monkeypatch):
    """Two promos on one transaction: every cent is attributed exactly once."""
    random.seed(7)
    monkeypatch.setattr(random, 'random', lambda: 0.0)

    headers, items = _run(
        count=6, products=_products(n=5, price=3.00),
        coupons=[_coupon(value=0.15)],
        deals=[_deal(trigger_qty=2, deal_price=2.0)],
        members=[{'member_id': 'm1'}],
        coupon_use_rate=1.0, combo_use_rate=1.0,
    )

    groups = _groups(items)
    for txn in headers:
        txn_id, subtotal, coupon_savings, deal_savings, tax, total = (
            txn[0], txn[5], txn[6], txn[7], txn[8], txn[9])
        lines = groups[txn_id]
        for rec in lines:
            _assert_lines_balance(rec)
        assert abs(sum(r[4] for r in lines) - (coupon_savings + deal_savings)) <= 0.01
        assert abs(sum(r[7] for r in lines)
                   - (subtotal - coupon_savings - deal_savings)) <= 0.01
        # the pre-existing header identity must not move
        assert abs(total - (subtotal + tax - coupon_savings - deal_savings)) <= 0.01


def test_multi_unit_line_gets_a_dollar_amount_not_a_per_unit_amount(monkeypatch):
    """The discount is LINE-level: a weighed line carries 10% of its own value.

    Under the old per-unit convention `discount` held a per-unit figure while
    every consumer (`mart_product_performance.total_discounts`) summed it as
    dollars — which is only ever right on single-unit lines, and never right on
    a weighed line (0.2 kg .. 3.5 kg).
    """
    random.seed(11)
    monkeypatch.setattr(random, 'random', lambda: 0.0)

    products = _products(n=1, price=10.00)
    products[0]['uom'] = 'lb'
    headers, items = _run(
        count=3, products=products,
        coupons=[_coupon(value=0.10)], members=[{'member_id': 'm1'}],
        coupon_use_rate=1.0,
    )

    assert headers and items
    for rec in items:
        qty, unit_price, discount = rec[2], rec[3], rec[4]
        extended = pos._line_gross_cents(qty, unit_price) / 100.0
        # 10% of the line's extended value, not 10% of one unit
        assert abs(discount - round(extended * 0.10, 2)) <= 0.01, (
            f'line discount {discount} is not 10% of {qty} x {unit_price} = {extended}'
        )
        assert discount != round(unit_price * 0.10, 2) or qty == 1.0
        _assert_lines_balance(rec)


def test_no_line_references_a_promo_without_money(monkeypatch):
    """The acceptance criterion: no coupon/deal reference on a zero-discount line."""
    for seed in range(6):
        random.seed(seed)
        headers, items = _run(
            count=25, products=_products(n=6, price=1.50),
            coupons=[_coupon(value=0.05), _coupon('c2', 'dollar_off', 0.01)],
            deals=[_deal(trigger_qty=2, deal_price=1.0)],
            members=[{'member_id': 'm1'}],
            coupon_use_rate=1.0, combo_use_rate=1.0,
        )
        assert items, f'seed {seed}: nothing generated'
        bad = [rec for rec in items if (rec[5] or rec[6]) and rec[4] <= 0]
        assert not bad, (
            f'seed {seed}: {len(bad)} promo-tagged line(s) carry discount 0'
        )
        for rec in items:
            _assert_lines_balance(rec)
        for txn in headers:
            txn_id, subtotal, coupon_savings, deal_savings = txn[0], txn[5], txn[6], txn[7]
            lines = _groups(items)[txn_id]
            assert abs(sum(r[4] for r in lines)
                       - (coupon_savings + deal_savings)) <= 0.01
            assert abs(sum(r[7] for r in lines)
                       - (subtotal - coupon_savings - deal_savings)) <= 0.01


# ---------------------------------------------------------------------------
# A saving that cannot reach a line is not booked
# ---------------------------------------------------------------------------

def test_scoped_coupon_outside_the_basket_is_not_applied(monkeypatch):
    """"$1 off dairy" on a produce-only basket is not a redemption."""
    random.seed(13)
    monkeypatch.setattr(random, 'random', lambda: 0.0)

    headers, items = _run(
        count=5, products=_products(department='Produce', department_id='d-produce'),
        coupons=[_coupon(coupon_type='dollar_off', value=1.0,
                         department_id='d-dairy')],
        members=[{'member_id': 'm1'}],
        coupon_use_rate=1.0,
    )

    assert headers and items
    assert all(txn[6] == 0 for txn in headers), (
        'a coupon whose scope is not in the basket booked savings on the header '
        'that no line can carry'
    )
    assert not [rec for rec in items if rec[5] is not None], (
        'coupon_id was stamped on a line that received none of its money'
    )


def test_department_promotion_lands_on_the_line():
    """A scenario department promotion is line discount money too."""
    random.seed(17)

    headers, items = _run(
        count=2, products=_products(n=3, price=4.00),
        promotions=[('Produce', 0.25)],
    )

    assert headers and items
    for rec in items:
        qty, unit_price, discount = rec[2], rec[3], rec[4]
        extended = pos._line_gross_cents(qty, unit_price) / 100.0
        assert abs(discount - round(extended * 0.25, 2)) <= 0.01
        _assert_lines_balance(rec)

    groups = _groups(items)
    for txn in headers:
        txn_id, subtotal, coupon_savings, deal_savings = txn[0], txn[5], txn[6], txn[7]
        lines = groups[txn_id]
        assert coupon_savings == 0 and deal_savings == 0
        # the 25 % is inside the lines, and subtotal is already net of it
        assert abs(sum(r[7] for r in lines) - subtotal) <= 0.01
        assert abs(sum(r[4] for r in lines)
                   - (sum(pos._line_gross_cents(r[2], r[3]) for r in lines) / 100.0 - subtotal)) <= 0.01


# ---------------------------------------------------------------------------
# The cents splitter
# ---------------------------------------------------------------------------

def test_half_cent_lines_round_the_way_numeric_does():
    """Weighed lines can land on a half cent; NUMERIC rounds it away from zero.

    2.06 lb x $7.75 = $15.965: the till says 15.97 and so does every SQL
    `ROUND(...)`, while float `round()` says 15.96. 39 lines of one 10-day
    backfill disagreed with SEMA-32 before the arithmetic moved to Decimal.
    """
    assert pos._line_gross_cents(2.06, 7.75) == 1597
    assert pos._line_gross_cents(2.75, 20.46) == 5627
    assert pos._line_gross_cents(1.74, 7.75) == 1349
    assert pos._line_gross_cents(0.9, 14.45) == 1301
    assert pos._line_gross_cents(0.5, 18.15) == 908
    assert pos._line_gross_cents(3, 1.005) == 302      # 3.015 -> 3.02


def test_split_cents_is_exact_and_never_exceeds_a_weight():
    cases = [
        (0, [1, 2, 3]),
        (1, [1, 1, 1]),
        (100, [1, 1, 1]),
        (10, [0, 5, 7]),
        (999, [1, 2, 3, 4, 5, 6, 7]),
        (500, [100, 0, 0]),
    ]
    for total, weights in cases:
        shares = pos._split_cents(total, weights)
        assert sum(shares) == total, f'{total}/{weights} summed to {sum(shares)}'
        assert all(s >= 0 for s in shares)
        if sum(weights) >= total:
            assert all(share <= weight for share, weight
                       in zip(shares, weights)), f'{total}/{weights} -> {shares}'


def test_apply_line_discount_never_takes_a_line_negative():
    recs = [[None, 'p1', 1, 1.00, 0.0, None, None, 1.00],
            [None, 'p2', 2, 1.00, 0.0, None, None, 2.00]]
    # ask for more than the lines are worth
    applied = pos._apply_line_discount(recs, [0, 1], 99.00, coupon_id='c1')
    assert applied == pytest.approx(3.00)
    assert all(rec[7] >= 0 for rec in recs)
    assert recs[0][4] + recs[1][4] == pytest.approx(3.00)
    assert recs[0][5] == recs[1][5] == 'c1'
