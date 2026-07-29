"""
Scenario coverage tests — verisim#12.

Tests that every scenario (existing + new) produces a well-formed
ScenarioContext with non-degenerate values, and that scenario effects
propagate correctly to the target domain models.
"""
from datetime import datetime

from grocery.generator.config import Config
from grocery.generator.scenarios.scenario_engine import (
    ScenarioContext,
    get_scenario_context,
)


# ---------------------------------------------------------------------------
# Helper: build a fresh config and a default scenario context for a scenario
# ---------------------------------------------------------------------------

def _ctx_for(scenario_name: str, hour: int = 12) -> ScenarioContext:
    cfg = Config()
    # flatten weights so volume_multiplier is easy to reason about
    cfg.volumes.hourly_weights = [1.0 for _ in range(24)]
    cfg.volumes.day_of_week_multipliers = {
        'monday': 1.0, 'tuesday': 1.0, 'wednesday': 1.0,
        'thursday': 1.0, 'friday': 1.0, 'saturday': 1.0, 'sunday': 1.0,
    }
    sim_dt = datetime(2026, 7, 6, hour, 0, 0)  # Monday
    return get_scenario_context([scenario_name], 1.0, sim_dt, cfg)


# ---------------------------------------------------------------------------
# Existing scenarios — should still work identically
# ---------------------------------------------------------------------------

def test_normal():
    ctx = _ctx_for('normal')
    assert ctx.scenario_tag == 'normal'
    assert ctx.volume_multiplier == 24.0  # 1.0 * 24 (hourly norm) * 1.0 (dow)
    assert ctx.coupon_multiplier == 1.0
    assert ctx.price_modifier == 1.0
    assert not ctx.supply_disruption


def test_weekend():
    ctx = _ctx_for('weekend', hour=10)
    assert 'weekend' in ctx.scenario_tag or ctx.scenario_tag == 'normal'
    assert ctx.volume_multiplier > 24.0  # weekend multiplier applied
    assert ctx.labor_multiplier >= 1.1


def test_promotion():
    ctx = _ctx_for('promotion', hour=10)
    assert ctx.volume_multiplier > 1.0
    assert len(ctx.active_promotions) > 0
    assert ctx.labor_multiplier >= 1.15


def test_holiday_week():
    ctx = _ctx_for('holiday_week', hour=10)
    assert ctx.volume_multiplier > 24.0
    assert ctx.labor_multiplier >= 1.2
    assert ctx.coupon_multiplier >= 1.5


def test_double_coupons():
    ctx = _ctx_for('double_coupons', hour=10)
    assert ctx.coupon_multiplier == 2.0
    assert ctx.loyalty_engagement_modifier >= 1.5


def test_rush_hour():
    ctx = _ctx_for('normal', hour=9)  # rush hour auto-detected
    assert 'rush_hour' in ctx.scenario_tag
    assert ctx.volume_multiplier > 24.0


# ---------------------------------------------------------------------------
# New scenarios — verisim#12
# ---------------------------------------------------------------------------

def test_inflation_pressure():
    ctx = _ctx_for('inflation_pressure')
    assert 'inflation_pressure' in ctx.scenario_tag
    assert ctx.price_modifier == 1.15
    assert ctx.loyalty_engagement_modifier == 0.85
    assert ctx.volume_multiplier > 0  # non-degenerate


def test_severe_weather():
    ctx = _ctx_for('severe_weather')
    assert 'severe_weather' in ctx.scenario_tag
    assert ctx.attendance_modifier == 0.75
    assert ctx.supply_disruption is True
    # volume should be lower than normal (weather * 24)
    assert ctx.volume_multiplier < 24.0 * 1.1  # less than normal with margin


def test_supplier_disruption():
    ctx = _ctx_for('supplier_disruption')
    assert 'supplier_disruption' in ctx.scenario_tag
    assert ctx.supply_disruption is True
    assert ctx.shrinkage_modifier == 1.3


def test_deep_discount():
    ctx = _ctx_for('deep_discount')
    assert 'deep_discount' in ctx.scenario_tag
    assert ctx.price_modifier == 0.8
    assert ctx.loyalty_engagement_modifier >= 1.3


def test_regional_peak():
    ctx = _ctx_for('regional_peak')
    assert 'regional_peak' in ctx.scenario_tag
    assert isinstance(ctx.per_store_multipliers, dict)


# ---------------------------------------------------------------------------
# Compound scenarios — multiple scenarios active simultaneously
# ---------------------------------------------------------------------------

def test_compound_severe_weather_plus_supplier():
    ctx = get_scenario_context(
        ['severe_weather', 'supplier_disruption'],
        1.0,
        datetime(2026, 7, 6, 12, 0, 0),
        Config(),
    )
    assert ctx.supply_disruption is True
    # both scenarios set supply_disruption → still True
    assert ctx.attendance_modifier == 0.75  # from severe_weather
    assert ctx.shrinkage_modifier == 1.3    # from supplier_disruption
    assert '+' in ctx.scenario_tag


def test_compound_inflation_plus_promotion():
    ctx = get_scenario_context(
        ['inflation_pressure', 'promotion'],
        1.0,
        datetime(2026, 7, 6, 12, 0, 0),
        Config(),
    )
    # inflation raises prices, promotion adds active_promotions
    assert ctx.price_modifier == 1.15
    assert len(ctx.active_promotions) > 0
    assert '+' in ctx.scenario_tag


# ---------------------------------------------------------------------------
# Non-degeneracy: all scenarios must produce sensible values
# ---------------------------------------------------------------------------

ALL_SCENARIOS = [
    'normal',
    'promotion',
    'holiday_week',
    'double_coupons',
    'weekend',
    'inflation_pressure',
    'severe_weather',
    'supplier_disruption',
    'deep_discount',
    'regional_peak',
]


def test_all_scenarios_produce_valid_context():
    """Every scenario must produce a non-degenerate context."""
    for name in ALL_SCENARIOS:
        ctx = _ctx_for(name)
        assert ctx.volume_multiplier >= 0, f"{name}: negative volume_multiplier"
        assert ctx.scenario_tag, f"{name}: empty scenario_tag"
        assert 0.5 <= ctx.price_modifier <= 2.0, f"{name}: price_modifier out of range"
        assert 0.0 <= ctx.attendance_modifier <= 1.0, f"{name}: attendance_modifier out of range"
        assert 0.5 <= ctx.labor_multiplier <= 2.0, f"{name}: labor_multiplier out of range"
        assert 0.5 <= ctx.shrinkage_modifier <= 5.0, f"{name}: shrinkage_modifier out of range"
