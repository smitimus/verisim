import builtins
from unittest.mock import patch

from grocery.generator.config import load_config


def test_config_loading_parses_yaml(monkeypatch):
    sample_yaml = {
        'generator': {'tick_interval_seconds': 60, 'simulation_minutes_per_tick': 5},
        'volumes': {'hourly_weights': [1.0 for _ in range(24)], 'day_of_week_multipliers': {
            'monday': 1.0, 'tuesday': 1.0, 'wednesday': 1.0, 'thursday': 1.0,
            'friday': 1.0, 'saturday': 1.0, 'sunday': 1.0
        }},
        'locations': {'store_count': 2},
        'pricing': {'tax_rate': 0.085},
        'products': {'initial_count': 10, 'departments': [{'name': 'Produce', 'categories': []}]}
    }

    with patch('grocery.generator.config._load_yaml', return_value=sample_yaml):
        cfg = load_config()

    # Basic validations of loaded config
    assert cfg.generator.tick_interval_seconds == 60
    assert isinstance(cfg.volumes.hourly_weights, list) and len(cfg.volumes.hourly_weights) == 24
    assert abs(cfg.pricing.tax_rate - 0.085) < 1e-6
    assert isinstance(cfg.departments, list)
