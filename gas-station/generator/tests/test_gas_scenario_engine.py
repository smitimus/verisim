"""
Scenario-engine tests for the gas-station generator — pure logic, no DB.
"""
import os
import sys
import unittest
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
_GEN = os.path.abspath(os.path.join(_HERE, '..'))
for _m in [k for k in sys.modules
           if k == 'config' or k == 'models' or k == 'scenarios'
           or k.startswith('models.') or k.startswith('scenarios.')]:
    sys.modules.pop(_m, None)
while _GEN in sys.path:
    sys.path.remove(_GEN)
sys.path.insert(0, _GEN)

from config import Config  # noqa: E402
from scenarios.scenario_engine import (  # noqa: E402
    get_scenario_context, ScenarioContext,
)


class TestGasScenarioEngine(unittest.TestCase):
    def setUp(self):
        self.cfg = Config()

    def test_rush_hour_stacks(self):
        peak = get_scenario_context('normal', 1.0, datetime(2026, 3, 10, 8), self.cfg)
        off = get_scenario_context('normal', 1.0, datetime(2026, 3, 10, 14), self.cfg)
        self.assertGreater(peak.volume_multiplier, off.volume_multiplier)
        self.assertIn('rush_hour', peak.scenario_tag)

    def test_fuel_spike_modifier(self):
        ctx = get_scenario_context('fuel_spike', 1.0, datetime(2026, 3, 10, 14), self.cfg)
        self.assertAlmostEqual(ctx.fuel_price_modifier,
                               1.0 + self.cfg.scenarios.fuel_spike_increase_pct)
        self.assertIn('fuel_spike', ctx.scenario_tag)

    def test_promotion_sets_categories(self):
        ctx = get_scenario_context('promotion', 1.0, datetime(2026, 3, 10, 14), self.cfg)
        cats = {c for c, _ in ctx.active_promotions}
        self.assertEqual(cats, set(self.cfg.scenarios.promotion_categories))

    def test_weekend_multiplier(self):
        ctx = get_scenario_context('weekend', 1.0, datetime(2026, 3, 10, 14), self.cfg)
        self.assertGreater(ctx.volume_multiplier,
                           get_scenario_context('normal', 1.0,
                                                datetime(2026, 3, 10, 14),
                                                self.cfg).volume_multiplier)

    def test_multiplier_override_scales(self):
        a = get_scenario_context('normal', 1.0, datetime(2026, 3, 10, 14), self.cfg)
        b = get_scenario_context('normal', 2.0, datetime(2026, 3, 10, 14), self.cfg)
        self.assertAlmostEqual(b.volume_multiplier / a.volume_multiplier, 2.0)

    def test_commute_shape(self):
        """Morning commute (8am Tue) beats mid-afternoon (14:00) for fuel volume."""
        am = get_scenario_context('normal', 1.0, datetime(2026, 3, 10, 8), self.cfg)
        pm_mid = get_scenario_context('normal', 1.0, datetime(2026, 3, 10, 14), self.cfg)
        self.assertGreater(am.volume_multiplier, pm_mid.volume_multiplier)

    def test_unknown_scenario_passthrough(self):
        ctx = get_scenario_context('totally_bogus', 1.0, datetime(2026, 3, 10, 14), self.cfg)
        self.assertGreater(ctx.volume_multiplier, 0)
        self.assertEqual(ctx.fuel_price_modifier, 1.0)


if __name__ == '__main__':
    unittest.main()
