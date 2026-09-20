"""
Config-loading tests for the gas-station generator.
Mirrors grocery/generator/tests/test_config_loading.py.
"""
import os
import sys
import tempfile
import unittest

# Purge same-named package cache so this file's own config.py wins when the
# full multi-industry suite runs in one pytest process.
_HERE = os.path.dirname(os.path.abspath(__file__))
_GEN = os.path.abspath(os.path.join(_HERE, '..'))
for _m in [k for k in sys.modules
           if k == 'config' or k.startswith('models.') or k.startswith('scenarios.')
           or k == 'models' or k == 'scenarios']:
    sys.modules.pop(_m, None)
while _GEN in sys.path:
    sys.path.remove(_GEN)
sys.path.insert(0, _GEN)

from config import load_config, reload_config, Config  # noqa: E402


FULL_YAML = """
generator:
  tick_interval_seconds: 20
  simulation_minutes_per_tick: 30
locations:
  count: 5
  employees_per_location: {min: 6, max: 12}
  pumps_per_location: {min: 6, max: 10}
volumes:
  pos_transactions_per_day: {min: 300, max: 900}
  fuel_transactions_per_day: {min: 200, max: 600}
loyalty:
  signup_rate: 0.04
  loyalty_usage_rate: 0.30
pricing:
  fuel_price_change_frequency_days: 2.5
  fuel_price_change_pct_max: 0.06
  product_price_change_frequency_days: 21.0
  tax_rate: 0.075
inventory:
  initial_stock_per_product: 200
  restock_threshold_pct: 0.30
scenarios:
  rush_hour:
    volume_multiplier: 2.2
    hours: [6, 7, 8, 17]
  weekend:
    volume_multiplier: 1.4
  promotion:
    discount_pct: 0.20
    affected_categories: [Coffee, Chips]
  fuel_spike:
    price_increase_pct: 0.10
products:
  initial_count: 150
"""


def _write_yaml(content):
    fd, path = tempfile.mkstemp(suffix='.yaml')
    with os.fdopen(fd, 'w') as f:
        f.write(content)
    return path


class TestGasConfig(unittest.TestCase):
    def test_defaults_when_missing_file(self):
        os.environ['CONF_PATH'] = '/nonexistent/config.yaml'
        cfg = load_config()
        self.assertEqual(cfg.volumes.pos_transactions_per_day_min, 500)
        self.assertEqual(cfg.db_name, 'gas_station')

    def test_full_yaml_applies(self):
        path = _write_yaml(FULL_YAML)
        os.environ['CONF_PATH'] = path
        cfg = load_config()
        self.assertEqual(cfg.generator.tick_interval_seconds, 20)
        self.assertEqual(cfg.generator.simulation_minutes_per_tick, 30)
        self.assertEqual(cfg.locations.count, 5)
        self.assertEqual(cfg.locations.pumps_per_location_min, 6)
        self.assertEqual(cfg.volumes.fuel_transactions_per_day_max, 600)
        self.assertAlmostEqual(cfg.pricing.fuel_price_change_frequency_days, 2.5)
        self.assertAlmostEqual(cfg.pricing.tax_rate, 0.075)
        self.assertAlmostEqual(cfg.scenarios.rush_hour_multiplier, 2.2)
        self.assertEqual(cfg.scenarios.rush_hour_hours, [6, 7, 8, 17])
        # scenario keys the loader previously ignored:
        self.assertAlmostEqual(cfg.scenarios.weekend_multiplier, 1.4)
        self.assertAlmostEqual(cfg.scenarios.promotion_discount_pct, 0.20)
        self.assertEqual(cfg.scenarios.promotion_categories, ['Coffee', 'Chips'])
        self.assertAlmostEqual(cfg.scenarios.fuel_spike_increase_pct, 0.10)
        self.assertEqual(cfg.initial_product_count, 150)
        os.remove(path)

    def test_reload_picks_up_changes(self):
        path = _write_yaml("locations:\n  count: 2\n")
        os.environ['CONF_PATH'] = path
        cfg = load_config()
        self.assertEqual(cfg.locations.count, 2)
        with open(path, 'w') as f:
            f.write("locations:\n  count: 9\n")
        cfg2 = reload_config(cfg)
        self.assertEqual(cfg2.locations.count, 9)
        os.remove(path)

    def test_shipped_config_yaml_loads(self):
        """The repo's own config.yaml must parse into sane values. Its raw
        hourly weights sum to ~1.06; the loader normalizes to exactly 1.0."""
        real = os.path.join(_GEN, '..', 'config.yaml')
        if not os.path.exists(real):
            self.skipTest('gas-station config.yaml not found')
        os.environ['CONF_PATH'] = real
        cfg = load_config()
        self.assertGreater(cfg.volumes.fuel_transactions_per_day_max,
                           cfg.volumes.fuel_transactions_per_day_min)
        self.assertEqual(len(cfg.volumes.hourly_weights), 24)
        self.assertAlmostEqual(sum(cfg.volumes.hourly_weights), 1.0, places=6)


if __name__ == '__main__':
    unittest.main()
