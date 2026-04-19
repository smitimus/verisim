"""
Configuration loader for the grocery data generator.
Reads /config/config.yaml (mounted from stacks/verisim-grocery/config.yaml)
and merges with environment variables for DB connection.
"""
import os
import yaml
from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class VolumeConfig:
    pos_transactions_per_day_min: int = 800
    pos_transactions_per_day_max: int = 3000
    hourly_weights: List[float] = field(default_factory=lambda: [
        0.00, 0.00, 0.00, 0.00, 0.00, 0.01, 0.02, 0.04,
        0.06, 0.08, 0.09, 0.09, 0.08, 0.07, 0.07, 0.07,
        0.08, 0.09, 0.08, 0.06, 0.04, 0.03, 0.01, 0.00
    ])
    day_of_week_multipliers: Dict[str, float] = field(default_factory=lambda: {
        'monday': 0.88, 'tuesday': 0.85, 'wednesday': 0.90,
        'thursday': 0.95, 'friday': 1.10, 'saturday': 1.25, 'sunday': 1.15
    })


@dataclass
class LocationConfig:
    store_count: int = 3
    warehouse_count: int = 1
    store_employees_per_location_min: int = 20
    store_employees_per_location_max: int = 40
    warehouse_employees_per_location_min: int = 10
    warehouse_employees_per_location_max: int = 20


@dataclass
class LoyaltyConfig:
    signup_rate: float = 0.06
    loyalty_usage_rate: float = 0.40


@dataclass
class PricingConfig:
    product_price_change_frequency_days: float = 14.0
    tax_rate: float = 0.07


@dataclass
class InventoryConfig:
    initial_stock_per_product: int = 200
    restock_check_frequency_hours: int = 24
    restock_threshold_pct: float = 0.25


@dataclass
class GeneratorConfig:
    tick_interval_seconds: int = 30
    simulation_minutes_per_tick: int = 15


@dataclass
class CouponConfig:
    active_at_any_time: int = 8
    valid_duration_days: int = 14
    coupon_use_rate: float = 0.20


@dataclass
class ComboDealConfig:
    active_at_any_time: int = 4
    valid_duration_days: int = 7
    combo_use_rate: float = 0.15


@dataclass
class ScenarioConfig:
    rush_hour_multiplier: float = 2.0
    rush_hour_hours: List[int] = field(default_factory=lambda: [9, 10, 11, 17, 18, 19])
    weekend_multiplier: float = 1.3
    promotion_discount_pct: float = 0.15
    promotion_departments: List[str] = field(default_factory=lambda: ['Produce', 'Dairy & Eggs', 'Snacks & Candy'])
    holiday_week_multiplier: float = 1.6
    double_coupon_multiplier: float = 2.0


@dataclass
class Config:
    db_host: str = 'localhost'
    db_port: int = 5432
    db_user: str = 'verisim'
    db_password: str = 'verisim'
    db_name: str = 'grocery'
    conf_path: str = '/config/config.yaml'

    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    locations: LocationConfig = field(default_factory=LocationConfig)
    volumes: VolumeConfig = field(default_factory=VolumeConfig)
    loyalty: LoyaltyConfig = field(default_factory=LoyaltyConfig)
    pricing: PricingConfig = field(default_factory=PricingConfig)
    inventory: InventoryConfig = field(default_factory=InventoryConfig)
    coupons: CouponConfig = field(default_factory=CouponConfig)
    combo_deals: ComboDealConfig = field(default_factory=ComboDealConfig)
    scenarios: ScenarioConfig = field(default_factory=ScenarioConfig)

    # Department/product catalog (populated from YAML)
    departments: List[Dict] = field(default_factory=list)
    initial_product_count: int = 500


def _load_yaml(path: str) -> dict:
    try:
        with open(path, 'r') as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}


def load_config() -> 'Config':
    conf_path = os.environ.get('CONF_PATH', '/config/config.yaml')
    cfg = Config(
        db_host=os.environ.get('POSTGRES_HOST', 'localhost'),
        db_port=int(os.environ.get('POSTGRES_PORT', 5432)),
        db_user=os.environ.get('POSTGRES_USER', 'verisim'),
        db_password=os.environ.get('POSTGRES_PASSWORD', 'verisim'),
        db_name=os.environ.get('POSTGRES_DB', 'grocery'),
        conf_path=conf_path,
    )
    _apply_yaml(cfg, _load_yaml(conf_path))
    return cfg


def reload_config(cfg: 'Config') -> 'Config':
    new_cfg = Config(
        db_host=cfg.db_host, db_port=cfg.db_port,
        db_user=cfg.db_user, db_password=cfg.db_password,
        db_name=cfg.db_name, conf_path=cfg.conf_path,
    )
    _apply_yaml(new_cfg, _load_yaml(cfg.conf_path))
    return new_cfg


def _apply_yaml(cfg: 'Config', data: dict) -> None:
    if not data:
        return

    gen = data.get('generator', {})
    if 'tick_interval_seconds' in gen:
        cfg.generator.tick_interval_seconds = int(gen['tick_interval_seconds'])
    if 'simulation_minutes_per_tick' in gen:
        cfg.generator.simulation_minutes_per_tick = int(gen['simulation_minutes_per_tick'])

    loc = data.get('locations', {})
    if 'store_count' in loc:
        cfg.locations.store_count = int(loc['store_count'])
    if 'warehouse_count' in loc:
        cfg.locations.warehouse_count = int(loc['warehouse_count'])
    sepl = loc.get('store_employees_per_location', {})
    if 'min' in sepl:
        cfg.locations.store_employees_per_location_min = int(sepl['min'])
    if 'max' in sepl:
        cfg.locations.store_employees_per_location_max = int(sepl['max'])
    wepl = loc.get('warehouse_employees_per_location', {})
    if 'min' in wepl:
        cfg.locations.warehouse_employees_per_location_min = int(wepl['min'])
    if 'max' in wepl:
        cfg.locations.warehouse_employees_per_location_max = int(wepl['max'])

    vol = data.get('volumes', {})
    pos_d = vol.get('pos_transactions_per_day', {})
    if 'min' in pos_d:
        cfg.volumes.pos_transactions_per_day_min = int(pos_d['min'])
    if 'max' in pos_d:
        cfg.volumes.pos_transactions_per_day_max = int(pos_d['max'])
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
    if 'product_price_change_frequency_days' in pri:
        cfg.pricing.product_price_change_frequency_days = float(pri['product_price_change_frequency_days'])
    if 'tax_rate' in pri:
        cfg.pricing.tax_rate = float(pri['tax_rate'])

    inv = data.get('inventory', {})
    if 'initial_stock_per_product' in inv:
        cfg.inventory.initial_stock_per_product = int(inv['initial_stock_per_product'])
    if 'restock_threshold_pct' in inv:
        cfg.inventory.restock_threshold_pct = float(inv['restock_threshold_pct'])

    cpn = data.get('coupons', {})
    if 'active_at_any_time' in cpn:
        cfg.coupons.active_at_any_time = int(cpn['active_at_any_time'])
    if 'valid_duration_days' in cpn:
        cfg.coupons.valid_duration_days = int(cpn['valid_duration_days'])
    if 'coupon_use_rate' in cpn:
        cfg.coupons.coupon_use_rate = float(cpn['coupon_use_rate'])

    cdl = data.get('combo_deals', {})
    if 'active_at_any_time' in cdl:
        cfg.combo_deals.active_at_any_time = int(cdl['active_at_any_time'])
    if 'valid_duration_days' in cdl:
        cfg.combo_deals.valid_duration_days = int(cdl['valid_duration_days'])
    if 'combo_use_rate' in cdl:
        cfg.combo_deals.combo_use_rate = float(cdl['combo_use_rate'])

    sc = data.get('scenarios', {})
    rh = sc.get('rush_hour', {})
    if 'volume_multiplier' in rh:
        cfg.scenarios.rush_hour_multiplier = float(rh['volume_multiplier'])
    if 'hours' in rh:
        cfg.scenarios.rush_hour_hours = list(rh['hours'])

    prods = data.get('products', {})
    if 'initial_count' in prods:
        cfg.initial_product_count = int(prods['initial_count'])
    if 'departments' in prods:
        cfg.departments = prods['departments']
