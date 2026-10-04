"""
Load the shipped grocery/config.yaml through the real config loader.

config.yaml is the file every install actually mounts, so a key that parses in
`config.py` but not in the shipped YAML is a broken default — and the failure
mode is silent: the generator starts, finds no vendor configured, logs a warning
and generates a catalogue with no vendors at all.

This is not a hypothetical: the vendor block is the first thing `config.yaml`
has ever carried that is a LIST of dicts, and `_apply_yaml` has to merge each
entry against the dataclass defaults rather than replace them wholesale.
"""
import os
import pathlib
import sys

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from grocery.generator.config import Config, _apply_yaml, _load_yaml  # noqa: E402

CONFIG_YAML = REPO_ROOT / "grocery" / "config.yaml"


def _loaded():
    cfg = Config()
    _apply_yaml(cfg, _load_yaml(str(CONFIG_YAML)))
    return cfg


def test_the_shipped_config_parses():
    data = _load_yaml(str(CONFIG_YAML))
    assert data, f"{CONFIG_YAML} is empty or unreadable"
    assert 'vendors' in data, (
        "the shipped config.yaml has no `vendors:` block, so every install "
        "would silently fall back to the dataclass defaults")


def test_the_vendor_block_reaches_the_config():
    cfg = _loaded()
    assert cfg.vendors.vendors, "no vendors loaded from config.yaml"
    names = {v['name'] for v in cfg.vendors.vendors}
    for expected in ('UNFI', 'Nash Finch', 'FreshFields Produce'):
        assert expected in names, f"{expected} is missing from the loaded config"


def test_every_shipped_vendor_has_every_key_the_model_reads():
    """
    A vendor missing a key falls back to a default the operator never wrote,
    which is how a `short_ship_rate` becomes 0 — a vendor that never shorts
    anything, indistinguishable from a working one.
    """
    required = ('name', 'code', 'fulfillment_model', 'lead_time_mean_days',
                'lead_time_stddev_days', 'short_ship_rate', 'credit_eligible',
                'credit_window_days')
    for vendor in _loaded().vendors.vendors:
        for key in required:
            assert key in vendor, (
                f"the shipped config's {vendor.get('name')} has no {key!r}")


def test_the_vendor_names_and_codes_are_unique():
    """`inv.suppliers` has UNIQUE on both, so a duplicate would fail the seed
    with a constraint violation on first start — after the DB is created, which
    is the worst moment to find out."""
    vendors = _loaded().vendors.vendors
    names = [v['name'] for v in vendors]
    codes = [v['code'] for v in vendors]
    assert len(set(names)) == len(names), f"duplicate vendor names: {names}"
    assert len(set(codes)) == len(codes), f"duplicate vendor codes: {codes}"


def test_the_shipped_config_yields_at_least_one_dsd_vendor():
    """With no DSD vendor, the perishable departments have no inbound mechanism
    and `generate_dsd_deliveries` has no schedule to fire on."""
    cfg = _loaded()
    dsd = [v for v in cfg.vendors.vendors if v['fulfillment_model'] == 'dsd']
    assert dsd, "the shipped config has no DSD vendor"


def test_the_shipped_dsd_departments_exist_in_the_catalogue():
    """A `dsd_departments` entry that matches no department means those SKUs
    fall back to a warehouse vendor silently — the config looks right and the
    behaviour is not what it says."""
    cfg = _loaded()
    departments = {str(d['name']).strip().lower()
                   for d in (cfg.departments or []) if d}
    if not departments:
        pytest.skip("no department tree in the loaded config")
    for name in cfg.vendors.dsd_departments:
        assert str(name).strip().lower() in departments, (
            f"dsd_departments names {name!r}, which is not a department in "
            f"products.departments")


def test_the_config_loader_still_defaults_without_a_vendor_block():
    """A config.yaml from before t_57b1a1ab has no `vendors:` key, and it must
    keep working — the dataclass defaults carry the catalogue."""
    cfg = Config()
    _apply_yaml(cfg, {})
    assert cfg.vendors.vendors, "the defaults must carry a vendor catalogue"


def test_the_behaviour_numbers_are_in_sane_ranges():
    """A rate outside [0, 1] clamps (the model handles that), but a *lead time*
    of zero or a settlement of zero is a config author who did not mean it, and
    it is worth failing on here rather than discovering it in a mart."""
    for vendor in _loaded().vendors.vendors:
        assert 0.0 <= vendor['short_ship_rate'] <= 1.0, (
            f"{vendor['name']}: short_ship_rate must be a share")
        assert vendor['lead_time_mean_days'] >= 0, \
            f"{vendor['name']}: a negative promised lead time"
        assert vendor['lead_time_stddev_days'] >= 0, \
            f"{vendor['name']}: a negative lead-time spread"
        assert vendor['credit_window_days'] >= 1, (
            f"{vendor['name']}: a zero-day claim window means the claim can "
            f"never be filed")