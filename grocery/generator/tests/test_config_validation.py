"""
Tests for the declarative config validation introduced by t_6081478a.

The bug these exist for: config.yaml was parsed with chains of
``if '<key>' in block`` tests, so a key the loader did not know was not an
error — it was a silently dropped line, and the dataclass default won for the
life of the process. The shipped grocery/config.yaml had four such keys, one of
them (``scenarios.promotion.discount_pct``) sitting in a file with a comment
implying it was live.

Each test below pins one behaviour:
  * a typo'd key raises, and names the key
  * a wrong type raises, and says what it wanted
  * every problem is reported at once, not one per restart
  * a rejected document applies nothing (no half-applied config)
  * the shipped config.yaml validates clean
  * the schema and the dataclasses cannot drift apart
"""
import dataclasses
import hashlib
import os

import pytest
import yaml

from grocery.generator import config as gc

# The exception must be imported under the SAME module name config.py used.
# This repo loads generator modules twice — once flat (`from config_schema
# import ...`, which works because grocery/generator is on sys.path) and once as
# `grocery.generator.config_schema` (a PEP 420 namespace package). Same file,
# two module objects, two distinct class objects, so pytest.raises against the
# wrong one never matches. Existing modules/models/*.py already carry this
# shape; the tests just have to import the way the code under test does.
from config_schema import ConfigValidationError, validate_and_apply


def _write(tmp_path, text):
    p = tmp_path / 'config.yaml'
    p.write_text(text)
    return str(p)


def _load(tmp_path, text, monkeypatch=None):
    """load_config() with CONF_PATH pointed at a temp file."""
    import os
    path = _write(tmp_path, text)
    old = os.environ.get('CONF_PATH')
    os.environ['CONF_PATH'] = path
    try:
        return gc.load_config()
    finally:
        if old is None:
            os.environ.pop('CONF_PATH', None)
        else:
            os.environ['CONF_PATH'] = old


# ---------------------------------------------------------------------------
# the defect
# ---------------------------------------------------------------------------

def test_typo_in_key_raises(tmp_path):
    """The whole point: `tick_interval_secondz` used to be silently ignored.

    Pre-t_6081478a this loaded cleanly and left tick_interval_seconds at its
    default of 30 while the operator believed it was 999 — a generator running
    at a third of the intended cadence with no error anywhere.
    """
    with pytest.raises(ConfigValidationError) as exc:
        _load(tmp_path, 'generator:\n  tick_interval_secondz: 999\n')

    assert 'generator.tick_interval_secondz' in str(exc.value)


def test_unknown_top_level_block_raises(tmp_path):
    with pytest.raises(ConfigValidationError) as exc:
        _load(tmp_path, 'volume:\n  store_count: 9\n')

    assert 'volume' in str(exc.value)


def test_unknown_key_inside_known_block_raises(tmp_path):
    """The block is known but the key is not — the subtle case, and the one
    that bit the shipped file."""
    with pytest.raises(ConfigValidationError) as exc:
        _load(tmp_path, 'pricing:\n  tax_rt: 0.09\n')

    assert 'pricing.tax_rt' in str(exc.value)


def test_error_suggests_the_real_key(tmp_path):
    """A typo has to be actionable without opening the source."""
    with pytest.raises(ConfigValidationError) as exc:
        _load(tmp_path, 'pricing:\n  tax_rat: 0.09\n')

    assert 'tax_rate' in str(exc.value)


def test_bad_subkey_in_a_min_max_block_raises(tmp_path):
    with pytest.raises(ConfigValidationError) as exc:
        _load(tmp_path, 'volumes:\n  pos_transactions_per_day:\n    minimum: 5\n')

    assert 'minimum' in str(exc.value)


def test_min_greater_than_max_raises(tmp_path):
    """An inverted range otherwise draws from an empty interval, which shows up
    as a quietly wrong row count rather than an error."""
    with pytest.raises(ConfigValidationError) as exc:
        _load(tmp_path, 'volumes:\n  pos_transactions_per_day:\n    min: 900\n    max: 100\n')

    assert 'min (900)' in str(exc.value)
    assert 'max (100)' in str(exc.value)


# ---------------------------------------------------------------------------
# types
# ---------------------------------------------------------------------------

def test_wrong_type_raises_with_expected_type(tmp_path):
    with pytest.raises(ConfigValidationError) as exc:
        _load(tmp_path, 'pricing:\n  tax_rate: "seven percent"\n')

    msg = str(exc.value)
    assert 'pricing.tax_rate' in msg
    assert 'number' in msg


def test_int_field_rejects_a_boolean(tmp_path):
    """`store_count: true` is a bool in YAML, and bool is a subclass of int —
    a naive int() would read it as 1."""
    with pytest.raises(ConfigValidationError):
        _load(tmp_path, 'locations:\n  store_count: true\n')


def test_list_field_rejects_a_scalar(tmp_path):
    with pytest.raises(ConfigValidationError) as exc:
        _load(tmp_path, 'volumes:\n  hourly_weights: 0.5\n')

    assert 'list' in str(exc.value)


def test_int_field_accepts_a_whole_float(tmp_path):
    """YAML has no int type, so `30.0` for an int key is legitimate."""
    cfg = _load(tmp_path, 'generator:\n  tick_interval_seconds: 30.0\n')
    assert cfg.generator.tick_interval_seconds == 30


# ---------------------------------------------------------------------------
# collected errors, and all-or-nothing application
# ---------------------------------------------------------------------------

def test_every_problem_is_reported_at_once(tmp_path):
    """Three typos must cost one edit cycle, not three restarts."""
    with pytest.raises(ConfigValidationError) as exc:
        _load(tmp_path, (
            'generator:\n'
            '  tick_interval_secondz: 1\n'
            'pricing:\n'
            '  tax_rt: 0.09\n'
            'loyalty:\n'
            '  signup_rt: 0.1\n'
        ))

    msg = str(exc.value)
    assert '3 problems' in msg
    for key in ('generator.tick_interval_secondz', 'pricing.tax_rt',
                'loyalty.signup_rt'):
        assert key in msg


def test_rejected_document_applies_nothing(tmp_path):
    """A half-applied config is worse than a rejected one: the operator fixes
    one key, restarts, and gets a different silent failure from the next."""
    good = {'pricing': {'tax_rate': 0.5}}
    bad = {'pricing': {'tax_rate': 0.5, 'tax_rt': 0.09}}

    cfg_ok = gc.Config()
    gc._apply_yaml(cfg_ok, good)
    assert cfg_ok.pricing.tax_rate == 0.5

    cfg_bad = gc.Config()
    with pytest.raises(ConfigValidationError):
        gc._apply_yaml(cfg_bad, bad)

    # The valid key in the rejected document must NOT have landed.
    assert cfg_bad.pricing.tax_rate != 0.5


# ---------------------------------------------------------------------------
# known-unused keys
# ---------------------------------------------------------------------------

def test_known_unused_key_warns_but_does_not_fail(tmp_path):
    """A shipped-but-inert key must not brick an existing install — but it must
    not look effective either, so it comes back as a warning rather than an
    error."""
    cfg = gc.Config()
    warnings = validate_and_apply(
        cfg,
        {'scenarios': {'holiday_week': {'coupon_use_boost': 1.5}}},
        gc.SCHEMA, unused=gc.KNOWN_UNUSED)

    assert len(warnings) == 1
    assert 'holiday_week.coupon_use_boost' in warnings[0]


def test_known_unused_key_still_applies_its_valid_siblings(tmp_path):
    """A warning is not a rejection: the rest of the block must still land."""
    cfg = gc.Config()
    validate_and_apply(
        cfg,
        {'scenarios': {'holiday_week': {'coupon_use_boost': 1.5,
                                        'volume_multiplier': 2.5}}},
        gc.SCHEMA, unused=gc.KNOWN_UNUSED)

    assert cfg.scenarios.holiday_week_multiplier == 2.5


# ---------------------------------------------------------------------------
# the shipped files
# ---------------------------------------------------------------------------

def _generator_dir():
    """The directory holding config.py — however this run imported it.

    `gc.__file__` is not reliable here: pytest puts grocery/generator on
    sys.path (only tests/ has an __init__.py), so config may be loaded as the
    flat module `config` OR as `grocery.generator.config`. Both name the same
    file, so resolve the directory from __file__ and search upward for the
    shipped config.yaml rather than assuming a depth.
    """
    here = os.path.dirname(os.path.abspath(gc.__file__))
    for up in range(4):
        candidate = os.path.abspath(os.path.join(here, *(['..'] * up)))
        if os.path.exists(os.path.join(candidate, 'config.yaml')):
            return here, candidate
    return here, os.path.abspath(os.path.join(here, '..'))


def _shipped_yaml():
    """The repo's own grocery/config.yaml — the file the compose stack mounts
    read-only into the generator, and the one an operator edits."""
    _gen_dir, product_dir = _generator_dir()
    path = os.path.join(product_dir, 'config.yaml')
    if not os.path.exists(path):
        pytest.skip(f'{path} not found')
    with open(path) as f:
        return yaml.safe_load(f)


def test_shipped_config_yaml_validates():
    """The repo's own config.yaml must pass. If this fails, the file and the
    loader disagree and that is a bug in one of them."""
    data = _shipped_yaml()

    cfg = gc.Config()
    gc._apply_yaml(cfg, data)
    assert cfg.generator.tick_interval_seconds == 30


def test_shipped_config_yaml_hourly_weights_sum_to_one():
    """The scenario engine scales volume by weight x 24, so a sum of anything
    but 1.0 rescales a whole day of volume."""
    weights = _shipped_yaml()['volumes']['hourly_weights']
    assert len(weights) == 24
    assert abs(sum(weights) - 1.0) < 1e-6


def test_shipped_standalone_config_yaml_also_validates():
    """The standalone image bakes in its own config.yaml, which is a strict
    subset of the mounted one. If it drifts out of the schema the all-in-one
    image would fail to boot with no compose stack to debug against."""
    _gen_dir, product_dir = _generator_dir()
    path = os.path.join(product_dir, 'standalone', 'config.yaml')
    if not os.path.exists(path):
        pytest.skip('standalone config not found')
    with open(path) as f:
        data = yaml.safe_load(f)

    cfg = gc.Config()
    gc._apply_yaml(cfg, data)


# ---------------------------------------------------------------------------
# schema / dataclass agreement
# ---------------------------------------------------------------------------

def test_every_schema_attr_path_names_a_real_field():
    """The schema's whole value is that it is complete. A rule pointing at a
    field that does not exist raises at validation time — but only once that
    particular key is in someone's config file, which is the worst possible
    moment to find out the schema was wrong."""
    cfg = gc.Config()
    from config_schema import _normalize

    for _yaml_path, (attr_path, subs, _v) in _normalize(gc.SCHEMA).items():
        if subs is not None:
            for leaf in subs.values():
                _resolve(cfg, attr_path + (leaf,))
        else:
            _resolve(cfg, attr_path)


def _resolve(cfg, attr_path):
    obj = cfg
    for name in attr_path[:-1]:
        obj = getattr(obj, name)
    names = {f.name for f in dataclasses.fields(obj)}
    assert attr_path[-1] in names, (
        f'schema points at {attr_path} but the dataclass has {sorted(names)}')


def test_every_dataclass_config_field_is_in_the_schema():
    """The converse of the test above, and the one that keeps a new knob from
    being added to Config and quietly never read from the YAML."""
    from config_schema import _normalize

    cfg = gc.Config()
    mapped = set()
    for _y, (attr_path, subs, _v) in _normalize(gc.SCHEMA).items():
        if subs is not None:
            mapped.update(attr_path + (leaf,) for leaf in subs.values())
        else:
            mapped.add(attr_path)

    # Fields set from the environment, not from YAML.
    env_only = {'db_host', 'db_port', 'db_user', 'db_password', 'db_name', 'conf_path'}

    missing = []
    for f in dataclasses.fields(cfg):
        if dataclasses.is_dataclass(getattr(cfg, f.name)) or f.name in env_only:
            continue
        if (f.name,) not in mapped:
            missing.append(f.name)

    assert not missing, (
        f'Config fields nothing reads from config.yaml: {missing}. Either add '
        f'them to SCHEMA (if an operator should be able to set them) or '
        f'delete the field.')


def test_config_schema_copies_are_identical():
    """Each generator image is built with `context: ./generator`, so this module
    is copied into each product. The copies must not drift.

    The repo root is found by walking up for base/config_schema.py rather than
    assuming a fixed depth — a depth assumption is exactly the kind of thing
    that makes this test skip in a container and pass in CI.
    """
    gen_dir, _product_dir = _generator_dir()

    repo_root = None
    probe = os.path.abspath(gen_dir)
    for _ in range(6):
        if os.path.exists(os.path.join(probe, 'base', 'config_schema.py')):
            repo_root = probe
            break
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent

    if repo_root is None:
        pytest.skip('base/config_schema.py not found above the generator dir')

    def digest(p):
        with open(p, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()

    canonical = os.path.join(repo_root, 'base', 'config_schema.py')
    copies = [
        os.path.join(repo_root, product, 'generator', 'config_schema.py')
        for product in ('grocery', 'gas-station', 'support')
    ]
    copies = [c for c in copies if os.path.exists(c)]

    want = digest(canonical)
    drifted = [c for c in copies if digest(c) != want]
    assert not drifted, (
        f'these config_schema.py copies have drifted from base/config_schema.py: '
        f'{drifted}')
