"""
Configuration loader for the customer-support data generator.
Reads /config/config.yaml and merges with environment variables for DB connection.
"""
import os
import yaml
from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class VolumeConfig:
    tickets_per_day_min: int = 180
    tickets_per_day_max: int = 320
    calls_per_day_min: int = 400
    calls_per_day_max: int = 750
    chats_per_day_min: int = 120
    chats_per_day_max: int = 260
    hourly_weights: List[float] = field(default_factory=lambda: [
        0.0113, 0.0057, 0.0038, 0.0028, 0.0038, 0.0085,
        0.0236, 0.0491, 0.0736, 0.0868, 0.0913, 0.0849,
        0.0717, 0.0679, 0.0736, 0.0717, 0.0642, 0.0547,
        0.0425, 0.0330, 0.0264, 0.0208, 0.0160, 0.0123]
    )
    day_of_week_multipliers: Dict[str, float] = field(default_factory=lambda: {
        'monday': 1.30, 'tuesday': 1.05, 'wednesday': 0.98,
        'thursday': 0.97, 'friday': 1.02, 'saturday': 0.62, 'sunday': 0.55
    })


@dataclass
class LocationConfig:
    contact_center_count: int = 2
    satellite_count: int = 2
    agents_per_location_min: int = 12
    agents_per_location_max: int = 25


@dataclass
class QueueConfig:
    ticket_weight: Dict[str, float] = field(default_factory=lambda: {
        'billing': 0.28, 'technical': 0.24, 'account': 0.16,
        'shipping': 0.14, 'returns': 0.12, 'escalations': 0.06
    })
    phone_share: float = 0.52
    chat_share: float = 0.20
    email_share: float = 0.18
    web_share: float = 0.07
    social_share: float = 0.05
    abandonment_rate: float = 0.065
    transfer_rate: float = 0.14
    first_contact_resolution_rate: float = 0.62
    reopen_rate: float = 0.08


@dataclass
class CustomerConfig:
    initial_customer_count: int = 2500
    new_customer_daily_min: int = 4
    new_customer_daily_max: int = 12
    repeat_contact_rate: float = 0.45   # chance a contact comes from an existing customer


@dataclass
class SurveyConfig:
    response_rate_voice: float = 0.30
    response_rate_chat: float = 0.45
    response_rate_ticket: float = 0.22
    detractor_shift: float = 0.0        # +N pushes scores down (outage scenario)


@dataclass
class TrainingConfig:
    qa_assignment_rate: float = 0.05    # per tick chance a low-CSAT agent gets remedial training
    onboarding_within_days: int = 7     # new hires must finish onboarding this fast


@dataclass
class GeneratorConfig:
    tick_interval_seconds: int = 30
    simulation_minutes_per_tick: int = 15


@dataclass
class ScenarioConfig:
    outage_volume_multiplier: float = 4.0
    outage_sentiment_shift: float = 0.45
    product_launch_multiplier: float = 1.8
    holiday_multiplier: float = 1.4
    weather_outage_multiplier: float = 2.5
    marketing_blast_multiplier: float = 2.2
    rush_hour_multiplier: float = 1.6
    rush_hour_hours: List[int] = field(default_factory=lambda: [9, 10, 11, 14, 15, 16])
    weekend_multiplier: float = 0.65


@dataclass
class Config:
    db_host: str = 'localhost'
    db_port: int = 5432
    db_user: str = 'verisim'
    db_password: str = 'verisim'
    db_name: str = 'support'
    conf_path: str = '/config/config.yaml'

    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    locations: LocationConfig = field(default_factory=LocationConfig)
    volumes: VolumeConfig = field(default_factory=VolumeConfig)
    queues: QueueConfig = field(default_factory=QueueConfig)
    customers: CustomerConfig = field(default_factory=CustomerConfig)
    surveys: SurveyConfig = field(default_factory=SurveyConfig)
    training: TrainingConfig = field(default_factory=TrainingConfig)
    scenarios: ScenarioConfig = field(default_factory=ScenarioConfig)


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
        db_name=os.environ.get('POSTGRES_DB', 'support'),
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
    if 'contact_center_count' in loc:
        cfg.locations.contact_center_count = int(loc['contact_center_count'])
    if 'satellite_count' in loc:
        cfg.locations.satellite_count = int(loc['satellite_count'])
    apl = loc.get('agents_per_location', {})
    if 'min' in apl:
        cfg.locations.agents_per_location_min = int(apl['min'])
    if 'max' in apl:
        cfg.locations.agents_per_location_max = int(apl['max'])

    vol = data.get('volumes', {})
    for key, attr in [('tickets_per_day', 'tickets_per_day'),
                      ('calls_per_day', 'calls_per_day'),
                      ('chats_per_day', 'chats_per_day')]:
        d = vol.get(key, {})
        if 'min' in d:
            setattr(cfg.volumes, f'{attr}_min', int(d['min']))
        if 'max' in d:
            setattr(cfg.volumes, f'{attr}_max', int(d['max']))
    if 'hourly_weights' in vol:
        # The scenario engine scales volume by (weight * 24), which assumes the
        # 24 weights sum to 1.0. Normalize whatever the user provided.
        raw = [float(x) for x in vol['hourly_weights']]
        total = sum(raw)
        if len(raw) == 24 and total > 0:
            cfg.volumes.hourly_weights = [w / total for w in raw]
    if 'day_of_week_multipliers' in vol:
        cfg.volumes.day_of_week_multipliers = {k: float(v) for k, v in vol['day_of_week_multipliers'].items()}

    q = data.get('queues', {})
    if 'ticket_weight' in q:
        cfg.queues.ticket_weight = {k: float(v) for k, v in q['ticket_weight'].items()}
    for key in ['phone_share', 'chat_share', 'email_share', 'web_share', 'social_share',
                'abandonment_rate', 'transfer_rate', 'first_contact_resolution_rate', 'reopen_rate']:
        if key in q:
            setattr(cfg.queues, key, float(q[key]))

    cust = data.get('customers', {})
    if 'initial_customer_count' in cust:
        cfg.customers.initial_customer_count = int(cust['initial_customer_count'])
    nc = cust.get('new_customer_daily', {})
    if 'min' in nc:
        cfg.customers.new_customer_daily_min = int(nc['min'])
    if 'max' in nc:
        cfg.customers.new_customer_daily_max = int(nc['max'])
    if 'repeat_contact_rate' in cust:
        cfg.customers.repeat_contact_rate = float(cust['repeat_contact_rate'])

    srv = data.get('surveys', {})
    for key in ['response_rate_voice', 'response_rate_chat', 'response_rate_ticket']:
        if key in srv:
            setattr(cfg.surveys, key, float(srv[key]))
    if 'detractor_shift' in srv:
        cfg.surveys.detractor_shift = float(srv['detractor_shift'])

    trn = data.get('training', {})
    if 'qa_assignment_rate' in trn:
        cfg.training.qa_assignment_rate = float(trn['qa_assignment_rate'])
    if 'onboarding_within_days' in trn:
        cfg.training.onboarding_within_days = int(trn['onboarding_within_days'])

    sc = data.get('scenarios', {})
    mapping = {
        'service_outage': {'volume_multiplier': 'outage_volume_multiplier',
                           'sentiment_shift': 'outage_sentiment_shift'},
        'product_launch': {'volume_multiplier': 'product_launch_multiplier'},
        'holiday_week': {'volume_multiplier': 'holiday_multiplier'},
        'weather_outage': {'volume_multiplier': 'weather_outage_multiplier'},
        'marketing_blast': {'volume_multiplier': 'marketing_blast_multiplier'},
        'rush_hour': {'volume_multiplier': 'rush_hour_multiplier', 'hours': 'rush_hour_hours'},
        'weekend': {'volume_multiplier': 'weekend_multiplier'},
    }
    for scen, keys in mapping.items():
        block = sc.get(scen, {})
        for ykey, attr in keys.items():
            if ykey in block:
                val = block[ykey]
                if ykey == 'hours':
                    setattr(cfg.scenarios, attr, list(val))
                else:
                    setattr(cfg.scenarios, attr, float(val))
