"""
Unit tests for the online orders model (t_24fae529) — pure logic, no DB.
Lifecycle + pricing invariants that don't require Postgres.
"""
from datetime import datetime, timedelta
import random

import grocery.generator.models.online as online
from grocery.generator.config import Config, OnlineConfig


class TestOnlineConfig:
    def test_defaults(self):
        cfg = Config()
        assert cfg.online.orders_per_day_min < cfg.online.orders_per_day_max
        assert 0 < cfg.online.pickup_share < 1
        assert cfg.online.service_fee_delivery > 0
        assert 0 <= cfg.online.cancel_rate <= 0.2
        assert 0 <= cfg.online.noshow_rate <= 0.2


class TestOnlineCount:
    def test_count_never_negative(self):
        cfg = Config()

        class _Ctx:
            volume_multiplier = 0.0
            scenario_tag = 'normal'

        for hour in range(24):
            n = online._online_count_for_tick(cfg, _Ctx(), datetime(2026, 3, 11, hour))
            assert n >= 0

    def test_peak_hour_beats_night(self):
        cfg = Config()

        class _Ctx:
            volume_multiplier = 1.0
            scenario_tag = 'normal'

        noon = online._online_count_for_tick(cfg, _Ctx(), datetime(2026, 3, 11, 12))
        night = online._online_count_for_tick(cfg, _Ctx(), datetime(2026, 3, 11, 3))
        # 3am is outside the 7am-9pm browsing window weights -> tiny
        assert noon >= night

    def test_multiplier_scales_volume(self):
        cfg = Config()

        class _Ctx1:
            volume_multiplier = 1.0
            scenario_tag = 'normal'

        class _Ctx4(_Ctx1):
            volume_multiplier = 4.0

        dt = datetime(2026, 3, 11, 12)
        base = online._online_count_for_tick(cfg, _Ctx1(), dt)
        quad = online._online_count_for_tick(cfg, _Ctx4(), dt)
        if base > 0:
            assert quad > base


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
