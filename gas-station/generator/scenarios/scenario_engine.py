"""
Scenario engine — determines the active generation context for each tick.

A ScenarioContext bundles together:
  - volume_multiplier  : float applied to base transaction counts
  - active_promotions  : list of (category, discount_pct) for POS discounts
  - fuel_price_modifier: float applied on top of current DB fuel prices (1.0 = no change)
  - scenario_tag       : string written to every transaction for downstream analysis
"""
from dataclasses import dataclass, field
from typing import List, Tuple
from datetime import datetime

from config import Config


@dataclass
class ScenarioContext:
    volume_multiplier: float = 1.0
    active_promotions: List[Tuple[str, float]] = field(default_factory=list)
    fuel_price_modifier: float = 1.0
    scenario_tag: str = 'normal'


def get_scenario_context(
    active_scenario: str,
    volume_multiplier_override: float,
    simulation_dt: datetime,
    cfg: Config,
) -> ScenarioContext:
    """
    Build a ScenarioContext for this tick.

    Arguments:
        active_scenario         : scenario name from control.generator_state
        volume_multiplier_override : raw multiplier from control.generator_state
        simulation_dt           : the datetime being simulated (wall clock in realtime)
        cfg                     : loaded Config object
    """
    ctx = ScenarioContext()
    ctx.scenario_tag = active_scenario

    # --- Base scenario logic ---
    if active_scenario == 'promotion':
        ctx.volume_multiplier = volume_multiplier_override
        ctx.active_promotions = [
            (cat, cfg.scenarios.promotion_discount_pct)
            for cat in cfg.scenarios.promotion_categories
        ]

    elif active_scenario == 'fuel_spike':
        ctx.volume_multiplier = volume_multiplier_override
        ctx.fuel_price_modifier = 1.0 + cfg.scenarios.fuel_spike_increase_pct

    elif active_scenario == 'weekend':
        ctx.volume_multiplier = volume_multiplier_override * cfg.scenarios.weekend_multiplier

    else:
        # normal, rush_hour, or any custom scenario
        ctx.volume_multiplier = volume_multiplier_override

    # --- Compound: rush hour stacks on top of whatever is active ---
    if simulation_dt.hour in cfg.scenarios.rush_hour_hours:
        ctx.volume_multiplier *= cfg.scenarios.rush_hour_multiplier
        if ctx.scenario_tag == 'normal':
            ctx.scenario_tag = 'rush_hour'
        else:
            ctx.scenario_tag = f'{ctx.scenario_tag}+rush_hour'

    # --- Time-of-day weight from hourly_weights list ---
    hour_weight = cfg.volumes.hourly_weights[simulation_dt.hour]
    ctx.volume_multiplier *= (hour_weight * 24)  # normalise: mean weight = 1/24, multiply back

    # --- Day-of-week multiplier ---
    dow = simulation_dt.strftime('%A').lower()
    ctx.volume_multiplier *= cfg.volumes.day_of_week_multipliers.get(dow, 1.0)

    return ctx
