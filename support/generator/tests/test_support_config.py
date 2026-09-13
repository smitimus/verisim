"""
Config-loading tests for the support generator.
Mirrors grocery/generator/tests/test_config_loading.py patterns.
"""
import os
import tempfile
import unittest

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

from config import load_config, reload_config, Config  # noqa: E402


FULL_YAML = """
generator:
  tick_interval_seconds: 45
  simulation_minutes_per_tick: 20
locations:
  contact_center_count: 3
  satellite_count: 1
  agents_per_location:
    min: 8
    max: 18
volumes:
  tickets_per_day: {min: 100, max: 200}
  calls_per_day: {min: 200, max: 400}
  chats_per_day: {min: 50, max: 90}
queues:
  phone_share: 0.60
  abandonment_rate: 0.10
customers:
  initial_customer_count: 500
surveys:
  response_rate_chat: 0.55
training:
  qa_assignment_rate: 0.08
scenarios:
  service_outage:
    volume_multiplier: 5.0
    sentiment_shift: 0.5
  rush_hour:
    hours: [8, 9, 17]
"""

MINIMAL_YAML = "volumes:\n  tickets_per_day:\n    min: 10\n    max: 20\n"


def _write_yaml(content):
    fd, path = tempfile.mkstemp(suffix='.yaml')
    with os.fdopen(fd, 'w') as f:
        f.write(content)
    return path


class TestSupportConfig(unittest.TestCase):
    def test_defaults_when_missing_file(self):
        os.environ['CONF_PATH'] = '/nonexistent/config.yaml'
        cfg = load_config()
        self.assertEqual(cfg.volumes.tickets_per_day_min, 180)
        self.assertEqual(cfg.db_name, 'support')

    def test_full_yaml_applies(self):
        path = _write_yaml(FULL_YAML)
        os.environ['CONF_PATH'] = path
        cfg = load_config()
        self.assertEqual(cfg.generator.tick_interval_seconds, 45)
        self.assertEqual(cfg.generator.simulation_minutes_per_tick, 20)
        self.assertEqual(cfg.locations.contact_center_count, 3)
        self.assertEqual(cfg.locations.satellite_count, 1)
        self.assertEqual(cfg.locations.agents_per_location_min, 8)
        self.assertEqual(cfg.locations.agents_per_location_max, 18)
        self.assertEqual(cfg.volumes.tickets_per_day_min, 100)
        self.assertEqual(cfg.volumes.calls_per_day_max, 400)
        self.assertEqual(cfg.volumes.chats_per_day_min, 50)
        self.assertAlmostEqual(cfg.queues.phone_share, 0.60)
        self.assertAlmostEqual(cfg.queues.abandonment_rate, 0.10)
        self.assertEqual(cfg.customers.initial_customer_count, 500)
        self.assertAlmostEqual(cfg.surveys.response_rate_chat, 0.55)
        self.assertAlmostEqual(cfg.training.qa_assignment_rate, 0.08)
        self.assertAlmostEqual(cfg.scenarios.outage_volume_multiplier, 5.0)
        self.assertAlmostEqual(cfg.scenarios.outage_sentiment_shift, 0.5)
        self.assertEqual(cfg.scenarios.rush_hour_hours, [8, 9, 17])
        os.remove(path)

    def test_partial_yaml_keeps_defaults(self):
        path = _write_yaml(MINIMAL_YAML)
        os.environ['CONF_PATH'] = path
        cfg = load_config()
        self.assertEqual(cfg.volumes.tickets_per_day_min, 10)
        self.assertEqual(cfg.volumes.tickets_per_day_max, 20)
        # untouched sections keep defaults
        self.assertEqual(cfg.locations.contact_center_count, 2)
        self.assertAlmostEqual(cfg.queues.phone_share, 0.52)
        os.remove(path)

    def test_reload_picks_up_changes(self):
        path = _write_yaml(MINIMAL_YAML)
        os.environ['CONF_PATH'] = path
        cfg = load_config()
        self.assertEqual(cfg.volumes.tickets_per_day_min, 10)
        with open(path, 'w') as f:
            f.write("volumes:\n  tickets_per_day:\n    min: 77\n    max: 99\n")
        cfg2 = reload_config(cfg)
        self.assertEqual(cfg2.volumes.tickets_per_day_min, 77)
        os.remove(path)

    def test_hourly_weights_sum_to_one(self):
        cfg = Config()
        self.assertAlmostEqual(sum(cfg.volumes.hourly_weights), 1.0, places=2)
        self.assertEqual(len(cfg.volumes.hourly_weights), 24)

    def test_hourly_weights_normalized_from_yaml(self):
        """Weights that don't sum to 1 are normalized (engine assumes sum=1)."""
        path = _write_yaml("volumes:\n  hourly_weights: [2, 2, 2, 2, 2, 2,\n"
                           "    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]\n")
        os.environ['CONF_PATH'] = path
        cfg = load_config()
        self.assertAlmostEqual(sum(cfg.volumes.hourly_weights), 1.0, places=6)
        self.assertAlmostEqual(cfg.volumes.hourly_weights[0], 1 / 24, places=6)
        os.remove(path)


if __name__ == '__main__':
    unittest.main()
