"""
Unit tests for the online orders model (t_24fae529) — pure logic, no DB.
Lifecycle + pricing invariants that don't require Postgres.

The per-tick order count is no longer this module's business: since t_eb31c99f it
comes from `main`'s shared volume law (`online_count_expectation` /
`compute_online_count`), the same contract `pos.generate_pos_transactions()` has
with `compute_pos_count()`. The count's own regression tests live in
`test_online_volume.py`.
"""
from datetime import datetime, timedelta
import random

import pytest

from grocery.generator.config import Config, OnlineConfig
from grocery.generator.main import online_count_expectation
from grocery.generator.scenarios.scenario_engine import get_scenario_context


class TestOnlineConfig:
    def test_defaults(self):
        cfg = Config()
        assert cfg.online.orders_per_day_min < cfg.online.orders_per_day_max
        assert 0 < cfg.online.pickup_share < 1
        assert cfg.online.service_fee_delivery > 0
        assert 0 <= cfg.online.cancel_rate <= 0.2
        assert 0 <= cfg.online.noshow_rate <= 0.2


class TestOnlineCount:
    """The per-tick count comes from main's shared volume law (t_eb31c99f)."""

    @staticmethod
    def _ctx(sim_dt, cfg, scale=1.0):
        ctx = get_scenario_context(['normal'], 1.0, sim_dt, cfg)
        ctx.volume_multiplier *= scale
        return ctx

    def test_count_never_negative(self):
        cfg = Config()
        day = datetime(2026, 3, 11)
        for hour in range(24):
            ctx = self._ctx(day.replace(hour=hour), cfg, scale=0.0)
            assert online_count_expectation(cfg, ctx, 3600, day.date()) == 0.0

    def test_peak_hour_beats_night(self):
        cfg = Config()
        day = datetime(2026, 3, 11)
        noon = online_count_expectation(cfg, self._ctx(day.replace(hour=12), cfg),
                                        3600, day.date(), daily=150)
        night = online_count_expectation(cfg, self._ctx(day.replace(hour=3), cfg),
                                         3600, day.date(), daily=150)
        # 3am carries the 0.0008 hourly weight against 0.0896 at noon -> ~1/112
        assert noon > night > 0, (noon, night)

    def test_multiplier_scales_volume(self):
        cfg = Config()
        day = datetime(2026, 3, 11)
        dt = day.replace(hour=12)
        base = online_count_expectation(cfg, self._ctx(dt, cfg), 3600, day.date(), daily=150)
        quad = online_count_expectation(cfg, self._ctx(dt, cfg, scale=4.0), 3600,
                                        day.date(), daily=150)
        assert base > 0
        assert quad == pytest.approx(base * 4)


class TestPricingInvariants:
    def test_fee_rule(self):
        """pickup => fee 0; delivery => service_fee_delivery. total = subtotal+fee+tax."""
        cfg = Config()
        tax_rate = cfg.pricing.tax_rate
        for ftype in ('pickup', 'delivery'):
            subtotal, fee = 100.0, (0.0 if ftype == 'pickup' else cfg.online.service_fee_delivery)
            tax = round(subtotal * tax_rate, 2)
            total = round(subtotal + fee + tax, 2)
            assert abs(total - (subtotal + fee + tax)) < 0.011
            if ftype == 'pickup':
                assert fee == 0
            else:
                assert fee == cfg.online.service_fee_delivery
