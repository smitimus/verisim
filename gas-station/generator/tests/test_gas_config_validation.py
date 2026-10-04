"""
Config-validation tests for the gas-station generator (t_6081478a).

config.yaml used to be parsed with chains of ``if '<key>' in block`` tests, so
a key the loader did not know was not an error — it was a silently dropped line
and the dataclass default won for the life of the process. The shipped
gas-station/config.yaml had three such keys.
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
           if k == 'config' or k == 'config_schema' or k.startswith('models.')
           or k.startswith('scenarios.') or k in ('models', 'scenarios')]:
    sys.modules.pop(_m, None)
while _GEN in sys.path:
    sys.path.remove(_GEN)
sys.path.insert(0, _GEN)

from config import (  # noqa: E402
    Config, KNOWN_UNUSED, SCHEMA, _apply_yaml, load_config, reload_config)
from config_schema import ConfigValidationError, _normalize, validate_and_apply  # noqa: E402

# Everything above is bound AT IMPORT TIME on purpose. This repo ships three
# products whose modules share the names `config`, `config_schema`, `models`
# and `scenarios`, so whichever test file pytest imports last wins
# sys.modules. A `from config import SCHEMA` written INSIDE a test body
# re-resolves that entry at call time and silently hands gas-station's test
# support's SCHEMA — which is how this file first failed against itself.


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


class TestGasConfigValidation(unittest.TestCase):
    def test_typo_in_a_key_raises(self):
        """The defect this card exists for. `pumps_per_locaion` used to be
        silently dropped and pumps_per_location_min stayed at its default."""
        exc = _load_expecting_error('locations:\n  pumps_per_locaion: {min: 9, max: 9}\n')
        self.assertIn('locations.pumps_per_locaion', str(exc))

    def test_unknown_block_raises(self):
        exc = _load_expecting_error('fuel:\n  price: 3.10\n')
        self.assertIn('fuel', str(exc))

    def test_error_suggests_the_real_key(self):
        exc = _load_expecting_error('pricing:\n  tax_rt: 0.09\n')
        self.assertIn('tax_rate', str(exc))

    def test_wrong_type_raises(self):
        exc = _load_expecting_error('pricing:\n  tax_rate: "eight percent"\n')
        self.assertIn('number', str(exc))

    def test_int_field_rejects_boolean(self):
        with self.assertRaises(ConfigValidationError):
            _load('locations:\n  count: true\n')

    def test_every_problem_reported_at_once(self):
        exc = _load_expecting_error(
            'locations:\n  counts: 5\npricing:\n  tax_rt: 0.09\n')
        self.assertIn('2 problems', str(exc))

    def test_rejected_document_applies_nothing(self):
        """A half-applied config is worse than a rejected one."""
        cfg = Config()
        with self.assertRaises(ConfigValidationError):
            _apply_yaml(cfg, {'pricing': {'tax_rate': 0.5, 'tax_rt': 0.09}})
        self.assertNotEqual(cfg.pricing.tax_rate, 0.5)

    def test_min_greater_than_max_raises(self):
        exc = _load_expecting_error(
            'locations:\n  pumps_per_location: {min: 12, max: 3}\n')
        self.assertIn('min (12)', str(exc))

    def test_hourly_weights_must_have_24_entries(self):
        with self.assertRaises(ConfigValidationError) as ctx:
            _load('volumes:\n  hourly_weights: [0.5, 0.5]\n')
        self.assertIn('24', str(ctx.exception))

    def test_shipped_hourly_weight_sum_is_rescaled_and_logged(self):
        """The shipped gas-station config sums to 1.06. It was rescaled away
        silently before; the rescale is kept (the intent is legible) but is now
        logged, so the file and the running generator cannot disagree quietly."""
        real = os.path.join(_GEN, '..', 'config.yaml')
        if not os.path.exists(real):
            self.skipTest('gas-station config.yaml not found')
        os.environ['CONF_PATH'] = real
        cfg = load_config()
        self.assertAlmostEqual(sum(cfg.volumes.hourly_weights), 1.0, places=6)

    def test_known_unused_key_warns_but_does_not_fail(self):
        """duration_hours is shipped and inert. It must not brick an existing
        install, and it must not look effective either."""
        cfg = Config()
        warnings = validate_and_apply(
            cfg,
            {'scenarios': {'fuel_spike': {'duration_hours': 48,
                                         'price_increase_pct': 0.2}}},
            SCHEMA, unused=KNOWN_UNUSED)

        self.assertEqual(len(warnings), 1)
        self.assertIn('fuel_spike.duration_hours', warnings[0])
        # The valid sibling in the same block still applies.
        self.assertAlmostEqual(cfg.scenarios.fuel_spike_increase_pct, 0.2)

    def test_reload_raises_on_a_broken_config(self):
        """A reload must not quietly keep the old config: the caller is a
        generator mid-run and would never learn the file was broken."""
        path = _write_yaml('locations:\n  count: 2\n')
        os.environ['CONF_PATH'] = path
        try:
            cfg = load_config()
            self.assertEqual(cfg.locations.count, 2)
            with open(path, 'w') as f:
                f.write('locations:\n  count: 9\n  counts: 3\n')
            with self.assertRaises(ConfigValidationError):
                reload_config(cfg)
        finally:
            os.remove(path)


class TestGasConfigSchemaAgreesWithDataclasses(unittest.TestCase):
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
        """A new knob added to Config but not to SCHEMA would be invisible to
        operators and silently stuck at its default — the exact defect."""
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
            'gas-station config_schema.py has drifted from base/config_schema.py')


if __name__ == '__main__':
    unittest.main()
