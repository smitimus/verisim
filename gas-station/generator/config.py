"""
Configuration loader for the gas-station data generator.
Reads /config/config.yaml (mounted from /opt/conf/data-generator/config.yaml)
and merges with environment variables for DB connection.

Validation (t_6081478a)
-----------------------
The YAML is applied through the declarative SCHEMA below rather than a chain of
``if '<key>' in block`` tests, so a typo is a startup/reload error instead of a
silently ignored line. SCHEMA is the authoritative list of keys this generator
reads: when you add a field to Config, add it to SCHEMA too. KNOWN_UNUSED holds
keys we deliberately accept but do not read, each with the reason, so a
shipped-but-inert key warns rather than fails.

Types are not restated in the table. Each value is coerced to the annotation on
the dataclass field SCHEMA points at, so the schema cannot drift from the
dataclass it describes.
"""
import logging
import os

import yaml
from dataclasses import dataclass, field
from typing import Dict, List

from config_schema import length, validate_and_apply

log = logging.getLogger(__name__)


@dataclass
class VolumeConfig:
    pos_transactions_per_day_min: int = 500
    pos_transactions_per_day_max: int = 2000
    fuel_transactions_per_day_min: int = 300
    fuel_transactions_per_day_max: int = 1000
    hourly_weights: List[float] = field(default_factory=lambda: [
        0.01, 0.01, 0.01, 0.01, 0.02, 0.03, 0.05, 0.08,
        0.09, 0.07, 0.05, 0.05, 0.06, 0.05, 0.04, 0.05,
        0.07, 0.09, 0.07, 0.05, 0.04, 0.03, 0.02, 0.01
    ])
    day_of_week_multipliers: Dict[str, float] = field(default_factory=lambda: {
        'monday': 0.90, 'tuesday': 0.88, 'wednesday': 0.92,
        'thursday': 0.95, 'friday': 1.15, 'saturday': 1.20, 'sunday': 1.00
    })


@dataclass
class LocationConfig:
    count: int = 3
    employees_per_location_min: int = 8
    employees_per_location_max: int = 15
    pumps_per_location_min: int = 4
    pumps_per_location_max: int = 8


@dataclass
class LoyaltyConfig:
    signup_rate: float = 0.05
    loyalty_usage_rate: float = 0.25


@dataclass
class PricingConfig:
    fuel_price_change_frequency_days: float = 3.5
    fuel_price_change_pct_max: float = 0.08
    product_price_change_frequency_days: float = 30.0
    tax_rate: float = 0.08


@dataclass
class InventoryConfig:
    initial_stock_per_product: int = 150
    restock_check_frequency_hours: int = 24
    restock_threshold_pct: float = 0.20


@dataclass
class GeneratorConfig:
    tick_interval_seconds: int = 30
    simulation_minutes_per_tick: int = 15


@dataclass
class ScenarioConfig:
    rush_hour_multiplier: float = 2.5
    rush_hour_hours: List[int] = field(default_factory=lambda: [7, 8, 16, 17, 18])
    weekend_multiplier: float = 1.3
    promotion_discount_pct: float = 0.15
    promotion_categories: List[str] = field(default_factory=lambda: ['Snacks', 'Beverages'])
    fuel_spike_increase_pct: float = 0.12


@dataclass
class Config:
    db_host: str = 'localhost'
    db_port: int = 5432
    db_user: str = 'verisim'
    db_password: str = 'verisim'
    db_name: str = 'gas_station'
    conf_path: str = '/config/config.yaml'

    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    locations: LocationConfig = field(default_factory=LocationConfig)
    volumes: VolumeConfig = field(default_factory=VolumeConfig)
    loyalty: LoyaltyConfig = field(default_factory=LoyaltyConfig)
    pricing: PricingConfig = field(default_factory=PricingConfig)
    inventory: InventoryConfig = field(default_factory=InventoryConfig)
    scenarios: ScenarioConfig = field(default_factory=ScenarioConfig)

    # Product categories (name → list of subcategories)
    product_categories: List[Dict] = field(default_factory=lambda: [
        {'name': 'Beverages', 'subcategories': ['Coffee', 'Fountain', 'Bottled Water', 'Energy Drinks', 'Juice', 'Sports Drinks']},
        {'name': 'Snacks',    'subcategories': ['Chips', 'Candy', 'Nuts', 'Crackers', 'Jerky']},
        {'name': 'Food',      'subcategories': ['Hot Dogs', 'Sandwiches', 'Pizza Slices', 'Pastries']},
        {'name': 'Tobacco',   'subcategories': ['Cigarettes', 'Cigars', 'Chewing Tobacco', 'Vape']},
        {'name': 'Automotive','subcategories': ['Motor Oil', 'Wiper Fluid', 'Air Fresheners', 'Car Wash']},
        {'name': 'Health & Beauty', 'subcategories': ['Pain Relievers', 'Bandages', 'Chapstick', 'Sunscreen']},
        {'name': 'Grocery',   'subcategories': ['Bread', 'Dairy', 'Eggs', 'Canned Goods']},
    ])
    initial_product_count: int = 200


# ---------------------------------------------------------------------------
# Schema — every key this generator reads, as (yaml_path, attr_path).
# See grocery/generator/config.py for the full explanation of the rule shapes.
# ---------------------------------------------------------------------------
SCHEMA = (
    # generator
    (('generator', 'tick_interval_seconds'), ('generator', 'tick_interval_seconds')),
    (('generator', 'simulation_minutes_per_tick'), ('generator', 'simulation_minutes_per_tick')),
    # locations
    (('locations', 'count'), ('locations', 'count')),
    (('locations', 'employees_per_location'), ('locations',),
     {'min': 'employees_per_location_min', 'max': 'employees_per_location_max'}),
    (('locations', 'pumps_per_location'), ('locations',),
     {'min': 'pumps_per_location_min', 'max': 'pumps_per_location_max'}),
    # volumes
    (('volumes', 'pos_transactions_per_day'), ('volumes',),
     {'min': 'pos_transactions_per_day_min', 'max': 'pos_transactions_per_day_max'}),
    (('volumes', 'fuel_transactions_per_day'), ('volumes',),
     {'min': 'fuel_transactions_per_day_min', 'max': 'fuel_transactions_per_day_max'}),
    # The scenario engine scales volume by weight x 24, which assumes the 24
    # weights sum to 1.0. The shipped gas-station config.yaml sums to 1.06, and
    # the loader normalized it away silently; the sum is now checked here, with
    # the fix (normalize) applied in _apply_yaml so the check and the
    # correction cannot disagree.
    (('volumes', 'hourly_weights'), ('volumes', 'hourly_weights'), None, length(24)),
    (('volumes', 'day_of_week_multipliers'), ('volumes', 'day_of_week_multipliers')),
    # loyalty
    (('loyalty', 'signup_rate'), ('loyalty', 'signup_rate')),
    (('loyalty', 'loyalty_usage_rate'), ('loyalty', 'loyalty_usage_rate')),
    # pricing
    (('pricing', 'fuel_price_change_frequency_days'), ('pricing', 'fuel_price_change_frequency_days')),
    (('pricing', 'fuel_price_change_pct_max'), ('pricing', 'fuel_price_change_pct_max')),
    (('pricing', 'product_price_change_frequency_days'), ('pricing', 'product_price_change_frequency_days')),
    (('pricing', 'tax_rate'), ('pricing', 'tax_rate')),
    # inventory
    (('inventory', 'initial_stock_per_product'), ('inventory', 'initial_stock_per_product')),
    (('inventory', 'restock_threshold_pct'), ('inventory', 'restock_threshold_pct')),
    # scenarios
    (('scenarios', 'rush_hour', 'volume_multiplier'), ('scenarios', 'rush_hour_multiplier')),
    (('scenarios', 'rush_hour', 'hours'), ('scenarios', 'rush_hour_hours')),
    (('scenarios', 'weekend', 'volume_multiplier'), ('scenarios', 'weekend_multiplier')),
    (('scenarios', 'promotion', 'discount_pct'), ('scenarios', 'promotion_discount_pct')),
    (('scenarios', 'promotion', 'affected_categories'), ('scenarios', 'promotion_categories')),
    (('scenarios', 'fuel_spike', 'price_increase_pct'), ('scenarios', 'fuel_spike_increase_pct')),
    # products
    (('products', 'initial_count'), ('initial_product_count',)),
    (('products', 'categories'), ('product_categories',)),
)

# Keys this product accepts but does not read. They WARN (logged on every
# reload) rather than failing, because installs already ship them and removing
# them is a config change of its own — but nothing reads them, so they must not
# be left looking effective. Measured dead as of t_6081478a:
#   scenarios.promotion.duration_hours
#   scenarios.fuel_spike.duration_hours
#   inventory.restock_check_frequency_hours
#   (the last is a dataclass field with no reader anywhere in this product)
KNOWN_UNUSED = {
    ('scenarios', 'promotion', 'duration_hours'):
        'not read; the promotion scenario lasts as long as the scenario is active',
    ('scenarios', 'fuel_spike', 'duration_hours'):
        'not read; the fuel_spike scenario lasts as long as the scenario is active',
    ('inventory', 'restock_check_frequency_hours'):
        'not read by this generator (no ordering loop); the dataclass field '
        'exists but nothing consults it',
}


def _load_yaml(path: str) -> dict:
    try:
        with open(path, 'r') as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}


def load_config() -> Config:
    conf_path = os.environ.get('CONF_PATH', '/config/config.yaml')
    cfg = Config(
        db_host=os.environ.get('POSTGRES_HOST', 'localhost'),
        db_port=int(os.environ.get('POSTGRES_PORT', 5432)),
        db_user=os.environ.get('POSTGRES_USER', 'verisim'),
        db_password=os.environ.get('POSTGRES_PASSWORD', 'verisim'),
        db_name=os.environ.get('POSTGRES_DB', 'gas_station'),
        conf_path=conf_path,
    )
    _apply_yaml(cfg, _load_yaml(conf_path))
    return cfg


def reload_config(cfg: Config) -> Config:
    """Re-read the YAML file and return an updated Config (keeps DB env vars).

    A rejected document RAISES rather than returning: the caller is a generator
    mid-run, and quietly continuing on the previous config would hide the very
    misconfiguration being reported.
    """
    new_cfg = Config(
        db_host=cfg.db_host,
        db_port=cfg.db_port,
        db_user=cfg.db_user,
        db_password=cfg.db_password,
        db_name=cfg.db_name,
        conf_path=cfg.conf_path,
    )
    _apply_yaml(new_cfg, _load_yaml(cfg.conf_path))
    return new_cfg


def _apply_yaml(cfg: Config, data: dict) -> None:
    if not data:
        return
    data = _normalize_hourly_weights(data)
    for warning in validate_and_apply(cfg, data, SCHEMA, unused=KNOWN_UNUSED,
                                      path_hint=cfg.conf_path, product='gas-station'):
        log.warning('config.yaml: %s', warning)


def _normalize_hourly_weights(data: dict) -> dict:
    """Rescale the 24 hourly weights to sum to 1.0 before validation.

    The pre-t_6081478a loader did this silently, which meant a config whose
    weights did not sum to 1.0 looked configured and quietly ran a whole day
    of volume 6% off (the shipped gas-station config.yaml summed to 1.06). It is
    still normalized rather than rejected — the intent is legible and the
    correction is unambiguous — but the correction is now logged, so the file
    and the running generator cannot silently disagree.
    """
    vol = data.get('volumes')
    if not isinstance(vol, dict):
        return data
    raw = vol.get('hourly_weights')
    if not isinstance(raw, list) or len(raw) != 24:
        return data
    try:
        weights = [float(x) for x in raw]
    except (TypeError, ValueError):
        return data  # let the schema report the type error, with its own message
    total = sum(weights)
    if total <= 0 or abs(total - 1.0) <= 1e-6:
        return data
    data = dict(data)
    data['volumes'] = dict(vol)
    data['volumes']['hourly_weights'] = [w / total for w in weights]
    log.warning('config.yaml: volumes.hourly_weights summed to %.6f, not 1.0 — '
                'rescaled to 1.0 (volume is weight x 24, so the sum sets the '
                'day\'s total).', total)
    return data
