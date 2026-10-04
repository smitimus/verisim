"""
Configuration loader for the customer-support data generator.
Reads /config/config.yaml and merges with environment variables for DB connection.

Validation (t_6081478a)
-----------------------
The YAML is applied through the declarative SCHEMA below rather than a chain of
``if '<key>' in block`` tests, so a typo is a startup/reload error instead of a
silently ignored line. SCHEMA is the authoritative list of keys this generator
reads: when you add a field to Config, add it to SCHEMA too.

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
        0.0425, 0.0330, 0.0264, 0.0208, 0.0160, 0.0123])
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


# ---------------------------------------------------------------------------
# Schema — every key this generator reads, as (yaml_path, attr_path).
# See grocery/generator/config.py for the full explanation of the rule shapes.
# ---------------------------------------------------------------------------
SCHEMA = (
    # generator
    (('generator', 'tick_interval_seconds'), ('generator', 'tick_interval_seconds')),
    (('generator', 'simulation_minutes_per_tick'), ('generator', 'simulation_minutes_per_tick')),
    # locations
    (('locations', 'contact_center_count'), ('locations', 'contact_center_count')),
    (('locations', 'satellite_count'), ('locations', 'satellite_count')),
    (('locations', 'agents_per_location'), ('locations',),
     {'min': 'agents_per_location_min', 'max': 'agents_per_location_max'}),
    # volumes
    (('volumes', 'tickets_per_day'), ('volumes',),
     {'min': 'tickets_per_day_min', 'max': 'tickets_per_day_max'}),
    (('volumes', 'calls_per_day'), ('volumes',),
     {'min': 'calls_per_day_min', 'max': 'calls_per_day_max'}),
    (('volumes', 'chats_per_day'), ('volumes',),
     {'min': 'chats_per_day_min', 'max': 'chats_per_day_max'}),
    # The scenario engine scales volume by weight x 24, which assumes the 24
    # weights sum to 1.0. The pre-t_6081478a loader normalized a bad sum away
    # silently; it is still normalized (see _normalize_hourly_weights) but now
    # logged, so the file and the running generator cannot disagree quietly.
    (('volumes', 'hourly_weights'), ('volumes', 'hourly_weights'), None, length(24)),
    (('volumes', 'day_of_week_multipliers'), ('volumes', 'day_of_week_multipliers')),
    # queues
    (('queues', 'ticket_weight'), ('queues', 'ticket_weight')),
    (('queues', 'phone_share'), ('queues', 'phone_share')),
    (('queues', 'chat_share'), ('queues', 'chat_share')),
    (('queues', 'email_share'), ('queues', 'email_share')),
    (('queues', 'web_share'), ('queues', 'web_share')),
    (('queues', 'social_share'), ('queues', 'social_share')),
    (('queues', 'abandonment_rate'), ('queues', 'abandonment_rate')),
    (('queues', 'transfer_rate'), ('queues', 'transfer_rate')),
    (('queues', 'first_contact_resolution_rate'), ('queues', 'first_contact_resolution_rate')),
    (('queues', 'reopen_rate'), ('queues', 'reopen_rate')),
    # customers
    (('customers', 'initial_customer_count'), ('customers', 'initial_customer_count')),
    (('customers', 'new_customer_daily'), ('customers',),
     {'min': 'new_customer_daily_min', 'max': 'new_customer_daily_max'}),
    (('customers', 'repeat_contact_rate'), ('customers', 'repeat_contact_rate')),
    # surveys
    (('surveys', 'response_rate_voice'), ('surveys', 'response_rate_voice')),
    (('surveys', 'response_rate_chat'), ('surveys', 'response_rate_chat')),
    (('surveys', 'response_rate_ticket'), ('surveys', 'response_rate_ticket')),
    (('surveys', 'detractor_shift'), ('surveys', 'detractor_shift')),
    # training
    (('training', 'qa_assignment_rate'), ('training', 'qa_assignment_rate')),
    (('training', 'onboarding_within_days'), ('training', 'onboarding_within_days')),
    # scenarios
    (('scenarios', 'service_outage', 'volume_multiplier'), ('scenarios', 'outage_volume_multiplier')),
    (('scenarios', 'service_outage', 'sentiment_shift'), ('scenarios', 'outage_sentiment_shift')),
    (('scenarios', 'product_launch', 'volume_multiplier'), ('scenarios', 'product_launch_multiplier')),
    (('scenarios', 'holiday_week', 'volume_multiplier'), ('scenarios', 'holiday_multiplier')),
    (('scenarios', 'weather_outage', 'volume_multiplier'), ('scenarios', 'weather_outage_multiplier')),
    (('scenarios', 'marketing_blast', 'volume_multiplier'), ('scenarios', 'marketing_blast_multiplier')),
    (('scenarios', 'rush_hour', 'volume_multiplier'), ('scenarios', 'rush_hour_multiplier')),
    (('scenarios', 'rush_hour', 'hours'), ('scenarios', 'rush_hour_hours')),
    (('scenarios', 'weekend', 'volume_multiplier'), ('scenarios', 'weekend_multiplier')),
)

# Keys this product accepts but does not read. None as of t_6081478a — every
# key in the shipped support/config.yaml reached an attribute. The table stays
# because a new inert key belongs here, documented, rather than being deleted
# from the file and forgotten.
KNOWN_UNUSED: Dict[tuple, str] = {}


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
    """Re-read the YAML and return an updated Config, keeping the DB env vars.

    A rejected document RAISES rather than returning: the caller is a generator
    mid-run, and quietly continuing on the previous config would hide the very
    misconfiguration being reported.
    """
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
    data = _normalize_hourly_weights(data)
    for warning in validate_and_apply(cfg, data, SCHEMA, unused=KNOWN_UNUSED,
                                      path_hint=cfg.conf_path, product='support'):
        log.warning('config.yaml: %s', warning)


def _normalize_hourly_weights(data: dict) -> dict:
    """Rescale the 24 hourly weights to sum to 1.0 before validation.

    The pre-t_6081478a loader did this silently. It is still normalized rather
    than rejected — the intent is legible and the correction unambiguous — but
    the correction is now logged, so the file and the running generator cannot
    silently disagree.
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
