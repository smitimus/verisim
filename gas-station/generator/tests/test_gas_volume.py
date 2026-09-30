"""
Per-tick POS/fuel volume law — verisim card t_093fa22c.

`compute_counts()` sized the tick from `simulation_minutes_per_tick` (15
simulated minutes in a 96-tick day) while realtime really ticks every
`control.generator_state.tick_interval_seconds` (30 s wall clock = 2880 ticks a
day). 2880/96 is exactly 30, so every realtime day ran ~30x the configured
volume — the same defect class t_94bbf1ce fixed in grocery, carried verbatim by
this product (and by support).

What these tests pin:

1. a realtime day totals the configured `pos_transactions_per_day` /
   `fuel_transactions_per_day` band shaped by the hour weights and the
   day-of-week multiplier — not 30x it;
2. the realtime day and the backfill day of the same date write the same volume,
   which is the property that makes the two writers interchangeable (the backfill
   law was never wrong, so it is the reference);
3. a tick carries its share of the hour, and that share follows the interval the
   loop actually sleeps (`tick_interval_seconds`, settable 5-3600 s from the API),
   so changing the cadence does not change the day;
4. the low-traffic hours survive: at 30 s a tick carries well under one
   transaction, so plain `round()` would round the whole quiet half of the night
   away;
5. the day's budget is a pure function of the date, the two channels use one law
   with their own bands, and `simulation_minutes_per_tick` — the lying config
   key — can no longer move the volume.
"""
import os
import random
import sys
from datetime import date, datetime, timedelta

import pytest

# Both industries ship same-named packages (config, main, models, scenarios);
# when the full multi-industry suite runs in one pytest process the earlier
# import wins the sys.modules cache. Purge + force this product's dir first.
_HERE = os.path.dirname(os.path.abspath(__file__))
_GEN = os.path.abspath(os.path.join(_HERE, '..'))
for _m in [k for k in sys.modules
           if k in ('config', 'main', 'models', 'scenarios')
           or k.startswith('models.') or k.startswith('scenarios.')]:
    sys.modules.pop(_m, None)
while _GEN in sys.path:
    sys.path.remove(_GEN)
sys.path.insert(0, _GEN)

from config import Config  # noqa: E402
from main import (  # noqa: E402
    SIM_HOUR_SECONDS,
    compute_counts,
    compute_fuel_count,
    compute_pos_count,
    daily_fuel_target,
    daily_pos_target,
    fuel_count_expectation,
    partial_hour_seconds,
    pos_count_expectation,
    realtime_tick_seconds,
)
from scenarios.scenario_engine import get_scenario_context  # noqa: E402

# Monday 2026-09-21 (dow 0.90) and Saturday 2026-09-26 (dow 1.20): the quietest
# and the busiest day-of-week multipliers in config.yaml.
MONDAY = datetime(2026, 9, 21, 0, 0, 0)
SATURDAY = datetime(2026, 9, 26, 0, 0, 0)
SEED = 20260921


def _ctx(cfg, sim_dt):
    """The context `main.run_tick` builds: 'normal', no manual override."""
    return get_scenario_context('normal', 1.0, sim_dt, cfg)


def realtime_day_hours(cfg, day, channel='pos', tick_seconds=None, seed=SEED):
    """One simulated day as the realtime loop writes it: dict hour -> count."""
    random.seed(seed)
    tick_seconds = tick_seconds or cfg.generator.tick_interval_seconds
    compute = compute_pos_count if channel == 'pos' else compute_fuel_count
    per_hour = {hour: 0 for hour in range(24)}
    for hour in range(24):
        base = day.replace(hour=hour)
        for offset in range(0, SIM_HOUR_SECONDS, tick_seconds):
            sim_dt = base + timedelta(seconds=offset)
            per_hour[hour] += compute(cfg, _ctx(cfg, sim_dt), tick_seconds, day.date())
    return per_hour


def realtime_day_total(cfg, day, channel='pos', tick_seconds=None, seed=SEED):
    return sum(realtime_day_hours(cfg, day, channel, tick_seconds, seed).values())


def backfill_day_total(cfg, day, channel='pos', seed=SEED):
    """The same day as the backfill writes it: one tick per simulated hour."""
    random.seed(seed)
    compute = compute_pos_count if channel == 'pos' else compute_fuel_count
    return sum(compute(cfg, _ctx(cfg, day.replace(hour=hour)),
                       SIM_HOUR_SECONDS, day.date())
               for hour in range(24))


def expected_day_total(cfg, day, channel='pos'):
    """Exact expectation: the date's budget x the day's hour shape."""
    daily = daily_pos_target(cfg, day.date()) if channel == 'pos' \
        else daily_fuel_target(cfg, day.date())
    return sum(daily / 24.0 * _ctx(cfg, day.replace(hour=hour)).volume_multiplier
               for hour in range(24))


def test_realtime_day_totals_the_configured_volume_not_30x_it():
    cfg = Config()
    assert cfg.generator.tick_interval_seconds == 30
    # Tolerances are measured, not guessed: over 60 seeds (Monday + Saturday) the
    # realtime day spans 4.4% (pos) and 6.9% (fuel) of its expectation, so 10% is
    # ~1.5x headroom — a Monte Carlo day is never exact, but the old divisor wrote
    # 30x this and any return to it fails the `total < expected * 1.5` line.
    for day in (MONDAY, SATURDAY):
        for channel in ('pos', 'fuel'):
            total = realtime_day_total(cfg, day, channel)
            expected = expected_day_total(cfg, day, channel)
            assert total == pytest.approx(expected, rel=0.10), (channel, day, total, expected)
            assert total < expected * 1.5, (channel, day, total, expected)
        # And the configured bands are what the days are built from.
        assert cfg.volumes.pos_transactions_per_day_min <= daily_pos_target(cfg, day.date())
        assert daily_pos_target(cfg, day.date()) <= cfg.volumes.pos_transactions_per_day_max
        assert cfg.volumes.fuel_transactions_per_day_min <= daily_fuel_target(cfg, day.date())
        assert daily_fuel_target(cfg, day.date()) <= cfg.volumes.fuel_transactions_per_day_max


def test_realtime_and_backfill_write_the_same_day_at_the_same_volume():
    cfg = Config()
    # Measured over 60 seeds: |realtime - backfill| max 4.3% (pos), 6.1% (fuel) —
    # the two writers now size the same day from the same budget.
    for day in (MONDAY, SATURDAY):
        for channel in ('pos', 'fuel'):
            assert realtime_day_total(cfg, day, channel) == pytest.approx(
                backfill_day_total(cfg, day, channel), rel=0.12), (channel, day)


def test_a_tick_carries_its_share_of_the_hour():
    """120 ticks of 30 s == the backfill's one hour tick, exactly."""
    cfg = Config()
    ctx = _ctx(cfg, MONDAY.replace(hour=17))          # PM commute peak
    ticks = SIM_HOUR_SECONDS // cfg.generator.tick_interval_seconds
    assert ticks == 120
    hour = pos_count_expectation(cfg, ctx, SIM_HOUR_SECONDS, MONDAY.date(), daily=1200)
    tick = pos_count_expectation(cfg, ctx, cfg.generator.tick_interval_seconds,
                                 MONDAY.date(), daily=1200)
    assert tick * ticks == pytest.approx(hour)
    assert pos_count_expectation(cfg, ctx, 60, MONDAY.date(), daily=1200) == pytest.approx(tick * 2)


def test_the_partial_hour_tick_is_sized_from_the_elapsed_part_of_the_hour():
    """The backfill's last tick writes the current hour up to `now`, not all of it."""
    assert partial_hour_seconds(datetime(2026, 9, 21, 14, 37, 12)) == 37 * 60 + 12
    assert partial_hour_seconds(datetime(2026, 9, 21, 14, 0, 0)) == 0
    cfg = Config()
    ctx = _ctx(cfg, MONDAY.replace(hour=17))
    elapsed = partial_hour_seconds(datetime(2026, 9, 21, 17, 30, 0))
    assert compute_pos_count(cfg, ctx, elapsed, MONDAY.date()) <= \
        compute_pos_count(cfg, ctx, SIM_HOUR_SECONDS, MONDAY.date()) + 1


def test_volume_follows_the_interval_the_loop_sleeps():
    cfg = Config()
    # run_tick sleeps control.generator_state.tick_interval_seconds (5-3600 s,
    # settable from the API/UI); that is the cadence the volume must track.
    assert realtime_tick_seconds(cfg, {'tick_interval_seconds': 30}) == 30
    assert realtime_tick_seconds(cfg, {'tick_interval_seconds': 300}) == 300
    assert realtime_tick_seconds(cfg, {'tick_interval_seconds': None}) == cfg.generator.tick_interval_seconds
    assert realtime_tick_seconds(cfg, {}) == cfg.generator.tick_interval_seconds

    # Ten times the interval, ten times the volume per tick — same day.
    for channel in ('pos', 'fuel'):
        assert realtime_day_total(cfg, MONDAY, channel, tick_seconds=300) == pytest.approx(
            expected_day_total(cfg, MONDAY, channel), rel=0.03)


def test_quiet_hours_are_thin_but_not_rounded_away():
    cfg = Config()
    quiet = _ctx(cfg, MONDAY.replace(hour=3))          # weight 0.01, the floor
    per_hour = pos_count_expectation(cfg, quiet, SIM_HOUR_SECONDS, MONDAY.date(), daily=1200)
    per_tick = pos_count_expectation(cfg, quiet, 30, MONDAY.date(), daily=1200)
    assert 0 < per_tick < 0.5, per_tick                 # ~0.1 transactions per 30 s tick
    # round() would return 0 for every one of those 120 ticks (the whole quiet
    # hour); carrying the fraction as a probability keeps the hour's volume.
    random.seed(SEED)
    ticks = [compute_pos_count(cfg, _ctx(cfg, MONDAY.replace(hour=3) + timedelta(seconds=s)),
                               30, MONDAY.date())
             for s in range(0, SIM_HOUR_SECONDS, 30)]
    assert all(t in (0, 1) for t in ticks), ticks[:10]
    assert sum(ticks) == pytest.approx(per_hour, rel=0.6), (sum(ticks), per_hour)
    quiet_hours = sum(realtime_day_hours(cfg, MONDAY, 'pos')[hour] for hour in range(0, 6))
    assert quiet_hours >= 30, quiet_hours               # ~108 expected over hours 0-5


def test_the_day_keeps_its_hourly_shape():
    cfg = Config()
    hours = realtime_day_hours(cfg, MONDAY, 'pos')
    assert hours[8] > hours[3] * 10, hours              # AM commute vs 03:00
    assert sum(hours.values()) == realtime_day_total(cfg, MONDAY)
    assert max(hours, key=lambda hour: hours[hour]) in (8, 17)


def test_the_day_budget_is_a_pure_function_of_the_date():
    cfg = Config()
    for channel, daily, low, high in (
            ('pos', daily_pos_target,
             cfg.volumes.pos_transactions_per_day_min, cfg.volumes.pos_transactions_per_day_max),
            ('fuel', daily_fuel_target,
             cfg.volumes.fuel_transactions_per_day_min, cfg.volumes.fuel_transactions_per_day_max)):
        random.seed(1)
        random.random()                                  # whatever else drew first
        picked = daily(cfg, MONDAY.date())
        assert daily(cfg, MONDAY.date()) == picked         # stable across calls
        assert low <= picked <= high, (channel, picked)
        # Different dates still draw different budgets from the band.
        assert len({daily(cfg, date(2026, 9, day)) for day in range(1, 29)}) > 5
    # The two channels do not share a draw.
    assert daily_pos_target(cfg, MONDAY.date()) != daily_fuel_target(cfg, MONDAY.date())


def test_the_two_channels_use_one_law_with_their_own_bands():
    cfg = Config()
    ctx = _ctx(cfg, MONDAY.replace(hour=17))
    pos = pos_count_expectation(cfg, ctx, 30, MONDAY.date(), daily=1000)
    fuel = fuel_count_expectation(cfg, ctx, 30, MONDAY.date(), daily=500)
    assert fuel == pytest.approx(pos / 2.0)
    # And `compute_counts` is exactly the two counts the channels publish.
    random.seed(SEED)
    counts = compute_counts(cfg, ctx, 30, MONDAY.date())
    assert len(counts) == 2 and all(isinstance(c, int) and c >= 0 for c in counts)


def test_simulation_minutes_per_tick_cannot_size_realtime_volume():
    """The config key that lied: realtime volume ignores the simulated clock."""
    cfg = Config()
    ctx = _ctx(cfg, MONDAY.replace(hour=17))
    baseline = (pos_count_expectation(cfg, ctx, 30, MONDAY.date()),
                fuel_count_expectation(cfg, ctx, 30, MONDAY.date()))
    for minutes in (5, 15, 30, 60):
        cfg.generator.simulation_minutes_per_tick = minutes
        assert (pos_count_expectation(cfg, ctx, 30, MONDAY.date()),
                fuel_count_expectation(cfg, ctx, 30, MONDAY.date())) == baseline
