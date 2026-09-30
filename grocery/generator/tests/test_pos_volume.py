"""
Per-tick POS volume law — verisim card t_94bbf1ce.

`compute_pos_count()` sized the tick from `simulation_minutes_per_tick` (15
simulated minutes in a 96-tick day) while realtime really ticks every
`control.generator_state.tick_interval_seconds` (30 s wall clock = 2880 ticks a
day). 2880/96 is exactly 30, so every realtime day since the seed day ran ~30x
the configured volume — measured on the dev slot 2026-09-30, POS per day
3,354 (backfill days) vs 70,584 / 76,533 / 80,949 / 92,282 (realtime days).

What these tests pin:

1. a realtime day totals the configured `pos_transactions_per_day` band shaped
   by the hour weights and the day-of-week multiplier — not 30x it;
2. the realtime day and the backfill day of the same date write the same volume,
   which is the property that makes the two writers interchangeable (the backfill
   law was never wrong, so it is the reference);
3. a tick carries its share of the hour, and that share follows the interval the
   loop actually sleeps (`tick_interval_seconds`, settable 5–3600 s from the API),
   so changing the cadence does not change the day;
4. the low-traffic hours survive: at 30 s a tick carries well under one
   transaction, so plain `round()` would round the whole quiet half of the night
   away;
5. the day's budget is a pure function of the date, and
   `simulation_minutes_per_tick` — the lying config key — can no longer move the
   volume.
"""
import random
from datetime import date, datetime, timedelta

import pytest

from grocery.generator.config import Config
from grocery.generator.main import (
    SIM_HOUR_SECONDS,
    compute_pos_count,
    daily_pos_target,
    pos_count_expectation,
    realtime_tick_seconds,
)
from grocery.generator.scenarios.scenario_engine import get_scenario_context

# Monday 2026-09-21 (dow 0.88) and Saturday 2026-09-26 (dow 1.25): the quietest
# and the busiest day-of-week multipliers in config.yaml.
MONDAY = datetime(2026, 9, 21, 0, 0, 0)
SATURDAY = datetime(2026, 9, 26, 0, 0, 0)
SEED = 20260921


def _ctx(cfg, sim_dt):
    """The context `main.run_tick` builds: 'normal', no manual override."""
    return get_scenario_context(['normal'], 1.0, sim_dt, cfg)


def realtime_day_hours(cfg, day, tick_seconds=None, seed=SEED):
    """One simulated day as the realtime loop writes it: dict hour -> count."""
    random.seed(seed)
    tick_seconds = tick_seconds or cfg.generator.tick_interval_seconds
    per_hour = {hour: 0 for hour in range(24)}
    for hour in range(24):
        base = day.replace(hour=hour)
        for offset in range(0, SIM_HOUR_SECONDS, tick_seconds):
            sim_dt = base + timedelta(seconds=offset)
            per_hour[hour] += compute_pos_count(cfg, _ctx(cfg, sim_dt),
                                                tick_seconds, day.date())
    return per_hour


def realtime_day_total(cfg, day, tick_seconds=None, seed=SEED):
    return sum(realtime_day_hours(cfg, day, tick_seconds, seed).values())


def backfill_day_total(cfg, day, seed=SEED):
    """The same day as the backfill writes it: one tick per simulated hour."""
    random.seed(seed)
    return sum(compute_pos_count(cfg, _ctx(cfg, day.replace(hour=hour)),
                                 SIM_HOUR_SECONDS, day.date())
               for hour in range(24))


def expected_day_total(cfg, day):
    """Exact expectation: the date's budget x the day's hour shape."""
    daily = daily_pos_target(cfg, day.date())
    return sum(daily / 24.0 * _ctx(cfg, day.replace(hour=hour)).volume_multiplier
               for hour in range(24))


def test_realtime_day_totals_the_configured_volume_not_30x_it():
    cfg = Config()
    assert cfg.generator.tick_interval_seconds == 30
    for day in (MONDAY, SATURDAY):
        total = realtime_day_total(cfg, day)
        expected = expected_day_total(cfg, day)
        # Tolerances are measured, not guessed: over 200 rounding seeds the
        # realtime day spans ±3.4% of the expectation (2880 stochastic ticks),
        # so 6% is ~2x headroom — a Monte Carlo day is never exact, but the old
        # divisor wrote 30x this and any return to it fails the line below.
        assert total == pytest.approx(expected, rel=0.06), (day, total, expected)
        assert total < expected * 1.5, (day, total, expected)
        # And the configured band is what the day is built from.
        assert cfg.volumes.pos_transactions_per_day_min <= daily_pos_target(cfg, day.date())
        assert daily_pos_target(cfg, day.date()) <= cfg.volumes.pos_transactions_per_day_max


def test_realtime_and_backfill_write_the_same_day_at_the_same_volume():
    cfg = Config()
    # Measured over 200 seeds: |realtime - backfill| max 3.4% (Monday), mean
    # 0.9% — the two writers now size the same day from the same budget.
    for day in (MONDAY, SATURDAY):
        assert realtime_day_total(cfg, day) == pytest.approx(backfill_day_total(cfg, day), rel=0.06)


def test_a_tick_carries_its_share_of_the_hour():
    """120 ticks of 30 s == the backfill's one hour tick, exactly."""
    cfg = Config()
    ctx = _ctx(cfg, MONDAY.replace(hour=18))
    ticks = SIM_HOUR_SECONDS // cfg.generator.tick_interval_seconds
    assert ticks == 120
    hour = pos_count_expectation(cfg, ctx, SIM_HOUR_SECONDS, MONDAY.date(), daily=2000)
    tick = pos_count_expectation(cfg, ctx, cfg.generator.tick_interval_seconds,
                                 MONDAY.date(), daily=2000)
    assert tick * ticks == pytest.approx(hour)
    assert pos_count_expectation(cfg, ctx, 60, MONDAY.date(), daily=2000) == pytest.approx(tick * 2)


def test_volume_follows_the_interval_the_loop_sleeps():
    cfg = Config()
    # run_tick sleeps control.generator_state.tick_interval_seconds (5-3600 s,
    # settable from the API/UI); that is the cadence the volume must track.
    assert realtime_tick_seconds(cfg, {'tick_interval_seconds': 30}) == 30
    assert realtime_tick_seconds(cfg, {'tick_interval_seconds': 300}) == 300
    assert realtime_tick_seconds(cfg, {'tick_interval_seconds': None}) == cfg.generator.tick_interval_seconds
    assert realtime_tick_seconds(cfg, {}) == cfg.generator.tick_interval_seconds

    # Ten times the interval, ten times the volume per tick — same day.
    assert realtime_day_total(cfg, MONDAY, tick_seconds=300) == pytest.approx(
        expected_day_total(cfg, MONDAY), rel=0.03)


def test_quiet_hours_are_thin_but_not_rounded_away():
    cfg = Config()
    quiet = _ctx(cfg, MONDAY.replace(hour=3))          # weight 0.0008, the floor
    per_hour = pos_count_expectation(cfg, quiet, SIM_HOUR_SECONDS, MONDAY.date(), daily=1900)
    assert 0 < per_hour < 2, per_hour                   # ~1.3 transactions in the hour
    assert realtime_day_hours(cfg, MONDAY)[3] >= 1      # round() would write 0 here


def test_the_day_keeps_its_hourly_shape():
    cfg = Config()
    hours = realtime_day_hours(cfg, MONDAY)
    assert hours[18] > hours[3] * 10, hours             # evening peak vs 03:00
    assert sum(hours.values()) == realtime_day_total(cfg, MONDAY)
    assert max(hours, key=lambda hour: hours[hour]) in (17, 18)


def test_the_day_budget_is_a_pure_function_of_the_date():
    cfg = Config()
    picked = {}
    for day in (date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 26)):
        random.seed(1)
        random.random()                                  # whatever else drew first
        picked[day] = daily_pos_target(cfg, day)
        assert daily_pos_target(cfg, day) == picked[day]  # stable across calls
        assert cfg.volumes.pos_transactions_per_day_min <= picked[day]
        assert picked[day] <= cfg.volumes.pos_transactions_per_day_max
    # Different dates still draw different budgets from the band.
    assert len({daily_pos_target(cfg, date(2026, 9, day)) for day in range(1, 29)}) > 5


def test_simulation_minutes_per_tick_cannot_size_realtime_volume():
    """The config key that lied: realtime volume ignores the simulated clock."""
    cfg = Config()
    ctx = _ctx(cfg, MONDAY.replace(hour=18))
    baseline = pos_count_expectation(cfg, ctx, 30, MONDAY.date(), daily=2000)
    for minutes in (5, 15, 30, 60):
        cfg.generator.simulation_minutes_per_tick = minutes
        assert pos_count_expectation(cfg, ctx, 30, MONDAY.date(), daily=2000) == baseline
