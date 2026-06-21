from datetime import datetime
from grocery.generator.config import Config
from grocery.generator.scenarios.scenario_engine import get_scenario_context


def test_hourly_weight_selection(monkeypatch):
    cfg = Config()
    # Ensure hourly weights are simple and predictable
    cfg.volumes.hourly_weights = [1.0 for _ in range(24)]
    # Day-of-week multipliers default to values for Monday-Sunday; test on a Monday
    sim_dt = datetime(2026, 1, 5, 7, 0, 0)  # Pick a date; code derives day name from this
    ctx = get_scenario_context(['normal'], 1.0, sim_dt, cfg)

    # hour=7 -> hourly_weight 1.0, so final multiplier should be 1.0 * 24 * day_multiplier
    dow = sim_dt.strftime('%A').lower()
    expected = 1.0 * 24 * cfg.volumes.day_of_week_multipliers.get(dow, 1.0)
    assert abs(ctx.volume_multiplier - expected) < 1e-6
