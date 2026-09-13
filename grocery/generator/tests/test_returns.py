"""
Unit tests for the returns & refunds model (t_2382c671).

Covers the pure-logic pieces; the full lifecycle (generate_returns against
POS data) is exercised by the standalone integration test and the e2e
full-cycle run.
"""
import random

import grocery.generator.models.returns as returns


class TestRestockAggregation:
    def test_floors_fractional_quantities(self):
        out = returns._restock_by_location([
            {'location_id': 'L1', 'product_id': 'P1', 'quantity': 2.4},
        ])
        assert out == [{'location_id': 'L1', 'product_id': 'P1', 'quantity': 2}]

    def test_rounds_up_over_half(self):
        out = returns._restock_by_location([
            {'location_id': 'L1', 'product_id': 'P1', 'quantity': 0.6},
        ])
        assert out == [{'location_id': 'L1', 'product_id': 'P1', 'quantity': 1}]

    def test_drops_zero_after_floor(self):
        out = returns._restock_by_location([
            {'location_id': 'L1', 'product_id': 'P1', 'quantity': 0.3},
        ])
        assert out == []

    def test_aggregates_same_location_product(self):
        out = returns._restock_by_location([
            {'location_id': 'L1', 'product_id': 'P1', 'quantity': 1.0},
            {'location_id': 'L1', 'product_id': 'P1', 'quantity': 2.6},
            {'location_id': 'L2', 'product_id': 'P1', 'quantity': 1.0},
        ])
        agg = {(r['location_id'], r['product_id']): r['quantity'] for r in out}
        assert agg == {('L1', 'P1'): 4, ('L2', 'P1'): 1}


class TestReturnShape:
    def test_reason_weights_sum_to_one(self):
        total = sum(w for _, w, _ in returns.RETURN_REASONS)
        assert abs(total - 1.0) < 1e-9

    def test_every_reason_declares_restock_flag(self):
        # defective + damaged_in_transit must be write-offs; the rest restock
        writeoffs = {r for r, _, rs in returns.RETURN_REASONS if not rs}
        assert writeoffs == {'defective', 'damaged_in_transit'}

    def test_age_window_ordered(self):
        assert returns.AGE_MIN_DAYS < returns.AGE_MAX_DAYS

    def test_refund_methods_match_schema_check(self):
        # schema.sql CHECK (refund_method IN (...)) — keep the tuples in sync
        assert set(returns.REFUND_METHODS) == {
            'original_payment', 'cash', 'store_credit'}

    def test_reasons_match_schema_check(self):
        assert {r for r, _, _ in returns.RETURN_REASONS} == {
            'defective', 'wrong_item', 'changed_mind', 'damaged_in_transit',
            'price_found_lower', 'other'}

    def test_cumulative_return_rate_in_band(self):
        """With RETURN_PROB_PER_DAY_OLD over the 2-14d window, the expected
        cumulative return share lands in a sane 1-10% band (mock realism)."""
        window = returns.AGE_MAX_DAYS - returns.AGE_MIN_DAYS + 1
        approx_rate = 1 - (1 - returns.RETURN_PROB_PER_DAY_OLD) ** window
        assert 0.01 <= approx_rate <= 0.10
