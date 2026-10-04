"""
Config-validation tests for the support generator (t_6081478a).

config.yaml used to be parsed with chains of ``if '<key>' in block`` tests, so a
key the loader did not know was not an error — it was a silently dropped line and
the dataclass default won for the life of the process.
"""
import os
import sys
import tempfile
import unittest

# Both industries ship same-named packages (config, scenarios, models); when
# the full suite runs in one pytest process the earlier import wins the
# sys.modules cache. Purge + force our own path first.
_HERE = os.path.dirname(os.path.abspath(__file__))
_GEN = os.path.abspath(os.path.join(_HERE, '..'))
for _m in [k for k in sys.modules
           if k == 'config' or k == 'config_schema' or k == 'models' or k == 'scenarios'
           or k.startswith('models.') or k.startswith('scenarios.')]:
    sys.modules.pop(_m, None)
while _GEN in sys.path:
    sys.path.remove(_GEN)
sys.path.insert(0, _GEN)

from config import Config, SCHEMA, _apply_yaml, load_config  # noqa: E402
from config_schema import ConfigValidationError, _normalize  # noqa: E402

# Bound at import time on purpose: all three products ship modules named
# `config` and `config_schema`, so a `from config import X` written INSIDE a
# test body re-resolves sys.modules at call time and can hand this file
# another industry's Config.


def _write_yaml(content):
    fd, path = tempfile.mkstemp(suffix='.yaml')
    with os.fdopen(fd, 'w') as f:
        f.write(content)
    return path


def _load(content):
    path = _write_yaml(content)
    os.environ['CONF_PATH'] = path
    try:
        return load_config()
    finally:
        os.remove(path)


def _load_expecting_error(content):
    path = _write_yaml(content)
    os.environ['CONF_PATH'] = path
    try:
        load_config()
    except ConfigValidationError as exc:
        return exc
    finally:
        os.remove(path)
    raise AssertionError('expected a ConfigValidationError, config loaded cleanly')


class TestSupportConfigValidation(unittest.TestCase):
    def test_typo_in_a_key_raises(self):
        exc = _load_expecting_error('queues:\n  abandonement_rate: 0.2\n')
        self.assertIn('queues.abandonment_rate', str(exc))

    def test_unknown_block_raises(self):
        exc = _load_expecting_error('escalation:\n  limit: 5\n')
        self.assertIn('escalation', str(exc))

    def test_error_suggests_the_real_key(self):
        exc = _load_expecting_error('training:\n  qa_assignment_rt: 0.2\n')
        self.assertIn('qa_assignment_rate', str(exc))

    def test_wrong_type_raises(self):
        exc = _load_expecting_error('training:\n  qa_assignment_rate: "high"\n')
        self.assertIn('number', str(exc))

    def test_int_field_rejects_boolean(self):
        with self.assertRaises(ConfigValidationError):
            _load('training:\n  onboarding_within_days: true\n')

    def test_every_problem_reported_at_once(self):
        exc = _load_expecting_error(
            'queues:\n  abandonement_rate: 0.2\ntraining:\n  qa_rt: 0.1\n')
        self.assertIn('2 problems', str(exc))

    def test_rejected_document_applies_nothing(self):
        cfg = Config()
        with self.assertRaises(ConfigValidationError):
            _apply_yaml(cfg, {'queues': {'abandonment_rate': 0.5,
                                         'abandonement_rate': 0.9}})
        self.assertNotEqual(cfg.queues.abandonment_rate, 0.5)

    def test_min_greater_than_max_raises(self):
        exc = _load_expecting_error('volumes:\n  tickets_per_day: {min: 400, max: 100}\n')
        self.assertIn('min (400)', str(exc))

    def test_hourly_weights_must_have_24_entries(self):
        with self.assertRaises(ConfigValidationError) as ctx:
            _load('volumes:\n  hourly_weights: [0.5, 0.5]\n')
        self.assertIn('24', str(ctx.exception))

    def test_open_valued_maps_accept_unknown_day_names(self):
        """day_of_week_multipliers is an open map: a new day key must be
        accepted rather than rejected, because the rule owns the whole subtree."""
        cfg = _load('volumes:\n  day_of_week_multipliers:\n    funday: 1.1\n')
        self.assertAlmostEqual(cfg.volumes.day_of_week_multipliers['funday'], 1.1)

    def test_shipped_config_validates(self):
        real = os.path.join(_GEN, '..', 'config.yaml')
        if not os.path.exists(real):
            self.skipTest('support config.yaml not found')
        os.environ['CONF_PATH'] = real
        cfg = load_config()
        self.assertEqual(len(cfg.volumes.hourly_weights), 24)
        self.assertAlmostEqual(sum(cfg.volumes.hourly_weights), 1.0, places=6)


class TestSupportConfigSchemaAgreesWithDataclasses(unittest.TestCase):
    def test_every_schema_path_names_a_real_field(self):
        import dataclasses

        cfg = Config()
        for _y, (attr_path, subs, _v) in _normalize(SCHEMA).items():
            leaves = ([subs[k] for k in subs] if subs else [None])
            for leaf in leaves:
                path = attr_path + (leaf,) if leaf else attr_path
                obj = cfg
                for name in path[:-1]:
                    obj = getattr(obj, name)
                names = {f.name for f in dataclasses.fields(obj)}
                self.assertIn(path[-1], names, f'schema points at {path}')

    def test_every_scalar_field_is_settable_from_yaml(self):
        import dataclasses

        cfg = Config()
        mapped = set()
        for _y, (attr_path, subs, _v) in _normalize(SCHEMA).items():
            if subs:
                mapped.update(attr_path + (leaf,) for leaf in subs.values())
            else:
                mapped.add(attr_path)

        env_only = {'db_host', 'db_port', 'db_user', 'db_password', 'db_name',
                    'conf_path'}
        missing = [f.name for f in dataclasses.fields(cfg)
                   if not dataclasses.is_dataclass(getattr(cfg, f.name))
                   and f.name not in env_only
                   and (f.name,) not in mapped]
        self.assertEqual(missing, [], f'Config fields not settable from YAML: {missing}')

    def test_schema_copy_matches_the_canonical_base_copy(self):
        import hashlib

        local = os.path.join(_GEN, 'config_schema.py')
        probe = os.path.dirname(_GEN)
        canonical = None
        for _ in range(5):
            candidate = os.path.join(probe, 'base', 'config_schema.py')
            if os.path.exists(candidate):
                canonical = candidate
                break
            probe = os.path.dirname(probe)
        if not canonical:
            self.skipTest('base/config_schema.py not found')

        def digest(p):
            with open(p, 'rb') as f:
                return hashlib.sha256(f.read()).hexdigest()

        self.assertEqual(
            digest(local), digest(canonical),
            'support config_schema.py has drifted from base/config_schema.py')


if __name__ == '__main__':
    unittest.main()
