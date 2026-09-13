"""
Scenario-engine tests for the support generator — pure logic, no DB.
"""
import unittest
from datetime import datetime

import os
import sys

# Both industries ship same-named packages (config, scenarios, models); when
# the full suite runs in one pytest process the earlier import wins the
# sys.modules cache. Purge + force our own path first.
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
    ScenarioContext, _apply_single_scenario, _calendar_multiplier,
    get_scenario_context,
)


class TestScenarioContext(unittest.TestCase):
    def setUp(self):
        self.cfg = Config()

    def test_normal_single(self):
        ctx = get_scenario_context(['normal'], 1.0,
                                   datetime(2026, 3, 11, 3, 0), self.cfg)  # Wed 3am quiet
        self.assertIn('normal', ctx.scenario_tag)
        self.assertGreater(ctx.volume_multiplier, 0)

    def test_outage_multiplies_volume_and_shifts_sentiment(self):
        base = get_scenario_context(['normal'], 1.0, datetime(2026, 3, 11, 3, 0), self.cfg)
        outage = get_scenario_context(['service_outage'], 1.0, datetime(2026, 3, 11, 3, 0), self.cfg)
        self.assertGreater(outage.volume_multiplier, base.volume_multiplier * 3)
        self.assertGreaterEqual(outage.sentiment_shift,
                                self.cfg.scenarios.outage_sentiment_shift)
        self.assertGreater(outage.call_stress, 1.0)
        self.assertIn('service_outage', outage.scenario_tag)

    def test_scenarios_merge_multiplicatively(self):
        one = get_scenario_context(['product_launch'], 1.0, datetime(2026, 3, 11, 3, 0), self.cfg)
        two = get_scenario_context(['product_launch', 'marketing_blast'], 1.0,
                                   datetime(2026, 3, 11, 3, 0), self.cfg)
        self.assertGreater(two.volume_multiplier, one.volume_multiplier)
        self.assertIn('product_launch', two.scenario_tag)
        self.assertIn('marketing_blast', two.scenario_tag)

    def test_volume_override_applied(self):
        a = get_scenario_context(['normal'], 1.0, datetime(2026, 3, 11, 3, 0), self.cfg)
        b = get_scenario_context(['normal'], 2.0, datetime(2026, 3, 11, 3, 0), self.cfg)
        self.assertAlmostEqual(b.volume_multiplier / a.volume_multiplier, 2.0)

    def test_business_hours_shape(self):
        """Mid-day (Wed 10am) must generate more than deep night (Wed 3am)."""
        day = get_scenario_context(['normal'], 1.0, datetime(2026, 3, 11, 10, 0), self.cfg)
        night = get_scenario_context(['normal'], 1.0, datetime(2026, 3, 11, 3, 0), self.cfg)
        self.assertGreater(day.volume_multiplier, night.volume_multiplier * 10)

    def test_monday_beats_saturday(self):
        mon = get_scenario_context(['normal'], 1.0, datetime(2026, 3, 9, 10, 0), self.cfg)
        sat = get_scenario_context(['normal'], 1.0, datetime(2026, 3, 14, 10, 0), self.cfg)
        self.assertGreater(mon.volume_multiplier, sat.volume_multiplier)

    def test_calendar_tax_week(self):
        mult, tag, sent = _calendar_multiplier(datetime(2026, 4, 10, 10, 0))
        self.assertGreater(mult, 1.0)
        self.assertEqual(tag, 'tax_week')
        mult, tag, _ = _calendar_multiplier(datetime(2026, 5, 10, 10, 0))
        self.assertEqual(mult, 1.0)
        self.assertEqual(tag, '')

    def test_calendar_returns_wave_jan(self):
        mult, tag, _ = _calendar_multiplier(datetime(2026, 1, 5, 10, 0))
        self.assertGreater(mult, 1.0)
        self.assertEqual(tag, 'returns_wave')

    def test_rush_hour_tag_added(self):
        ctx = get_scenario_context(['normal'], 1.0, datetime(2026, 3, 11, 10, 0), self.cfg)
        self.assertIn('rush_hour', ctx.scenario_tag)

    def test_unknown_scenario_is_neutral(self):
        ctx = ScenarioContext()
        before = (ctx.volume_multiplier, ctx.call_stress)
        _apply_single_scenario('nonsense_scenario', self.cfg, ctx)
        self.assertEqual((ctx.volume_multiplier, ctx.call_stress), before)


if __name__ == '__main__':
    unittest.main()
