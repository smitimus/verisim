"""
Configuration loader for the data generator.
Reads /config/config.yaml (mounted from /opt/conf/data-generator/config.yaml)
and merges with environment variables for DB connection.
"""
import os
import yaml
from dataclasses import dataclass, field
from typing import List, Dict


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
    db_name: str = 'gasstation'
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
        db_name=os.environ.get('POSTGRES_DB', 'gasstation'),
        conf_path=conf_path,
    )
    _apply_yaml(cfg, _load_yaml(conf_path))
    return cfg


def reload_config(cfg: Config) -> Config:
    """Re-read the YAML file and return an updated Config (keeps DB env vars)."""
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

    gen = data.get('generator', {})
    if 'tick_interval_seconds' in gen:
        cfg.generator.tick_interval_seconds = int(gen['tick_interval_seconds'])
    if 'simulation_minutes_per_tick' in gen:
        cfg.generator.simulation_minutes_per_tick = int(gen['simulation_minutes_per_tick'])

    loc = data.get('locations', {})
    if 'count' in loc:
        cfg.locations.count = int(loc['count'])
    epl = loc.get('employees_per_location', {})
    if 'min' in epl:
        cfg.locations.employees_per_location_min = int(epl['min'])
    if 'max' in epl:
        cfg.locations.employees_per_location_max = int(epl['max'])
    ppl = loc.get('pumps_per_location', {})
    if 'min' in ppl:
        cfg.locations.pumps_per_location_min = int(ppl['min'])
    if 'max' in ppl:
        cfg.locations.pumps_per_location_max = int(ppl['max'])

    vol = data.get('volumes', {})
    pos_d = vol.get('pos_transactions_per_day', {})
    if 'min' in pos_d:
        cfg.volumes.pos_transactions_per_day_min = int(pos_d['min'])
    if 'max' in pos_d:
        cfg.volumes.pos_transactions_per_day_max = int(pos_d['max'])
    fuel_d = vol.get('fuel_transactions_per_day', {})
    if 'min' in fuel_d:
        cfg.volumes.fuel_transactions_per_day_min = int(fuel_d['min'])
    if 'max' in fuel_d:
        cfg.volumes.fuel_transactions_per_day_max = int(fuel_d['max'])
    if 'hourly_weights' in vol:
        cfg.volumes.hourly_weights = [float(x) for x in vol['hourly_weights']]
    if 'day_of_week_multipliers' in vol:
        cfg.volumes.day_of_week_multipliers = {k: float(v) for k, v in vol['day_of_week_multipliers'].items()}

    loy = data.get('loyalty', {})
    if 'signup_rate' in loy:
        cfg.loyalty.signup_rate = float(loy['signup_rate'])
    if 'loyalty_usage_rate' in loy:
        cfg.loyalty.loyalty_usage_rate = float(loy['loyalty_usage_rate'])

    pri = data.get('pricing', {})
    if 'fuel_price_change_frequency_days' in pri:
        cfg.pricing.fuel_price_change_frequency_days = float(pri['fuel_price_change_frequency_days'])
    if 'fuel_price_change_pct_max' in pri:
        cfg.pricing.fuel_price_change_pct_max = float(pri['fuel_price_change_pct_max'])
    if 'product_price_change_frequency_days' in pri:
        cfg.pricing.product_price_change_frequency_days = float(pri['product_price_change_frequency_days'])
    if 'tax_rate' in pri:
        cfg.pricing.tax_rate = float(pri['tax_rate'])

    inv = data.get('inventory', {})
    if 'initial_stock_per_product' in inv:
        cfg.inventory.initial_stock_per_product = int(inv['initial_stock_per_product'])
    if 'restock_threshold_pct' in inv:
        cfg.inventory.restock_threshold_pct = float(inv['restock_threshold_pct'])

    sc = data.get('scenarios', {})
    rh = sc.get('rush_hour', {})
    if 'volume_multiplier' in rh:
        cfg.scenarios.rush_hour_multiplier = float(rh['volume_multiplier'])
    if 'hours' in rh:
        cfg.scenarios.rush_hour_hours = list(rh['hours'])
    if 'products' in data:
        prods = data['products']
        if 'initial_count' in prods:
            cfg.initial_product_count = int(prods['initial_count'])
        if 'categories' in prods:
            cfg.product_categories = prods['categories']
