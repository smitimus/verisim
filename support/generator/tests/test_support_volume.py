"""
Per-tick contact volume law — verisim card t_093fa22c.

`_daily_volumes()` sized the tick from `simulation_minutes_per_tick` (15
simulated minutes in a 96-tick day) while realtime really ticks every
`control.generator_state.tick_interval_seconds` (30 s wall clock = 2880 ticks a
day). 2880/96 is exactly 30, so every realtime day ran ~30x the configured
tickets/calls/chats band — the same defect class t_94bbf1ce fixed in grocery,
carried verbatim by this product (and by gas-station).

What these tests pin:

1. a realtime day totals the configured `tickets_per_day` / `calls_per_day` /
   `chats_per_day` band shaped by the hour weights and the day-of-week
   multiplier — not 30x it;
2. the realtime day and the backfill day of the same date write the same volume,
   which is the property that makes the two writers interchangeable (the backfill
   law was never wrong, so it is the reference);
3. a tick carries its share of the hour, and that share follows the interval the
   loop actually sleeps (`tick_interval_seconds`, settable 5-3600 s from the API),
   so changing the cadence does not change the day;
4. the quiet hours survive: at 30 s a tick carries well under one contact, so
   plain `round()` would round the small hours away;
5. the day's budgets are pure functions of the date, the three channels use one
   law with their own bands, and `simulation_minutes_per_tick` — the lying config
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
    calls_count_expectation,
    chats_count_expectation,
    compute_counts,
    daily_calls_target,
    daily_chats_target,
    daily_tickets_target,
    partial_hour_seconds,
    realtime_tick_seconds,
    tickets_count_expectation,
)
from scenarios.scenario_engine import get_scenario_context  # noqa: E402

# Monday 2026-09-21 (dow 1.30) and Saturday 2026-09-26 (dow 0.62): the busiest
# and the quietest day-of-week multipliers in config.yaml.
MONDAY = datetime(2026, 9, 21, 0, 0, 0)
SATURDAY = datetime(2026, 9, 26, 0, 0, 0)
SEED = 20260921

CHANNELS = ('tickets', 'calls', 'chats')
_BAND_ATTRS = {
    'tickets': 'tickets_per_day',
    'calls': 'calls_per_day',
    'chats': 'chats_per_day',
}


def _ctx(cfg, sim_dt):
    """The context the generator builds for a tick with no active scenario."""
    return get_scenario_context(['normal'], 1.0, sim_dt, cfg)


def _daily_target(cfg, day, channel):
    sim_date = day.date() if hasattr(day, 'date') else day
    return {'tickets': daily_tickets_target,
            'calls': daily_calls_target,
            'chats': daily_chats_target}[channel](cfg, sim_date)


def _expectation(cfg, ctx, simulated_seconds, sim_date, channel):
    return {'tickets': tickets_count_expectation,
            'calls': calls_count_expectation,
            'chats': chats_count_expectation}[channel](
        cfg, ctx, simulated_seconds, sim_date)


def realtime_day_hours(cfg, day, channel='tickets', tick_seconds=None, seed=SEED):
    """One simulated day as the realtime loop writes it: dict hour -> count."""
    random.seed(seed)
    index = CHANNELS.index(channel)
    tick_seconds = tick_seconds or cfg.generator.tick_interval_seconds
    per_hour = {hour: 0 for hour in range(24)}
    for hour in range(24):
        base = day.replace(hour=hour)
        for offset in range(0, SIM_HOUR_SECONDS, tick_seconds):
            sim_dt = base + timedelta(seconds=offset)
            per_hour[hour] += compute_counts(cfg, _ctx(cfg, sim_dt), tick_seconds,
                                             day.date())[index]
    return per_hour


def realtime_day_total(cfg, day, channel='tickets', tick_seconds=None, seed=SEED):
    return sum(realtime_day_hours(cfg, day, channel, tick_seconds, seed).values())


def backfill_day_total(cfg, day, channel='tickets', seed=SEED):
    """The same day as the backfill writes it: one tick per simulated hour."""
    random.seed(seed)
    index = CHANNELS.index(channel)
    return sum(compute_counts(cfg, _ctx(cfg, day.replace(hour=hour)),
                              SIM_HOUR_SECONDS, day.date())[index]
               for hour in range(24))


def expected_day_total(cfg, day, channel='tickets'):
    """Exact expectation: the date's budget x the day's hour shape."""
    daily = _daily_target(cfg, day, channel)
    return sum(daily / 24.0 * _ctx(cfg, day.replace(hour=hour)).volume_multiplier
               for hour in range(24))


def test_realtime_day_totals_the_configured_volume_not_30x_it():
    cfg = Config()
    assert cfg.generator.tick_interval_seconds == 30
    # Tolerances are measured, not guessed: over 60 seeds (Monday + Saturday) the
    # realtime day spans 13.5% (tickets), 10.5% (calls) and 21.6% (chats) of its
    # expectation — a contact centre carries ~100-560 contacts a day, so one
    # realisation is Poisson-noisy. 30% is headroom over the worst of those; the
    # guard that matters is `total < expected * 1.5`: the old 96-tick divisor wrote
    # 30x this and any return to it fails both lines.
    for day in (MONDAY, SATURDAY):
        for channel in CHANNELS:
            total = realtime_day_total(cfg, day, channel)
            expected = expected_day_total(cfg, day, channel)
            assert total == pytest.approx(expected, rel=0.30), (channel, day, total, expected)
            assert total < expected * 1.5, (channel, day, total, expected)
            # And the configured band is what the day is built from.
            low = getattr(cfg.volumes, _BAND_ATTRS[channel] + '_min')
            high = getattr(cfg.volumes, _BAND_ATTRS[channel] + '_max')
            assert low <= _daily_target(cfg, day, channel) <= high


def test_realtime_and_backfill_write_the_same_day_at_the_same_volume():
    cfg = Config()
    # Measured over 60 seeds: |realtime - backfill| max 12.9% (tickets), 10.9%
    # (calls), 17.5% (chats) — the two writers now size the same day from the same
    # date-keyed budget, and the gap is the Monte Carlo noise of one day, not a
    # different law (it was 30x before).
    for day in (MONDAY, SATURDAY):
        for channel in CHANNELS:
            assert realtime_day_total(cfg, day, channel) == pytest.approx(
                backfill_day_total(cfg, day, channel), rel=0.25), (channel, day)


def test_a_tick_carries_its_share_of_the_hour():
    """120 ticks of 30 s == the backfill's one hour tick, exactly."""
    cfg = Config()
    ctx = _ctx(cfg, MONDAY.replace(hour=10))          # midday peak
    ticks = SIM_HOUR_SECONDS // cfg.generator.tick_interval_seconds
    assert ticks == 120
    hour = tickets_count_expectation(cfg, ctx, SIM_HOUR_SECONDS, MONDAY.date(), daily=250)
    tick = tickets_count_expectation(cfg, ctx, cfg.generator.tick_interval_seconds,
                                     MONDAY.date(), daily=250)
    assert tick * ticks == pytest.approx(hour)
    assert tickets_count_expectation(cfg, ctx, 60, MONDAY.date(), daily=250) == pytest.approx(tick * 2)


def test_the_partial_hour_tick_is_sized_from_the_elapsed_part_of_the_hour():
    """The backfill's last tick writes the current hour up to `now`, not all of it."""
    assert partial_hour_seconds(datetime(2026, 9, 21, 14, 37, 12)) == 37 * 60 + 12
    assert partial_hour_seconds(datetime(2026, 9, 21, 14, 0, 0)) == 0
    cfg = Config()
    ctx = _ctx(cfg, MONDAY.replace(hour=10))
    elapsed = partial_hour_seconds(datetime(2026, 9, 21, 10, 30, 0))
    assert compute_counts(cfg, ctx, elapsed, MONDAY.date())[0] <= \
        compute_counts(cfg, ctx, SIM_HOUR_SECONDS, MONDAY.date())[0] + 1


def test_volume_follows_the_interval_the_loop_sleeps():
    cfg = Config()
    # run_tick sleeps control.generator_state.tick_interval_seconds (5-3600 s,
    # settable from the API/UI); that is the cadence the volume must track.
    assert realtime_tick_seconds(cfg, {'tick_interval_seconds': 30}) == 30
    assert realtime_tick_seconds(cfg, {'tick_interval_seconds': 300}) == 300
    assert realtime_tick_seconds(cfg, {'tick_interval_seconds': None}) == cfg.generator.tick_interval_seconds
    assert realtime_tick_seconds(cfg, {}) == cfg.generator.tick_interval_seconds

    # Ten times the interval, ten times the volume per tick — same day.
    for channel in CHANNELS:
        assert realtime_day_total(cfg, MONDAY, channel, tick_seconds=300) == pytest.approx(
            expected_day_total(cfg, MONDAY, channel), rel=0.03)


def test_quiet_hours_are_thin_but_not_rounded_away():
    cfg = Config()
    # 03:00 is the floor of the hourly weights (0.0028, a 33x skew).
    quiet = _ctx(cfg, MONDAY.replace(hour=3))
    per_hour = tickets_count_expectation(cfg, quiet, SIM_HOUR_SECONDS, MONDAY.date(), daily=250)
    assert 0 < per_hour < 2, per_hour                   # ~0.9 tickets in the hour
    # round() would write 0 for every one of those 120 ticks; the stochastic tick
    # writes 1 whenever the fraction lands, so the small hours keep their volume.
    quiet_hours = sum(realtime_day_hours(cfg, MONDAY, 'tickets')[hour] for hour in range(0, 6))
    assert quiet_hours >= 1, quiet_hours


def test_the_day_keeps_its_hourly_shape():
    cfg = Config()
    hours = realtime_day_hours(cfg, MONDAY, 'tickets')
    assert hours[10] > hours[3] * 10, hours             # midday peak vs 03:00
    assert sum(hours.values()) == realtime_day_total(cfg, MONDAY, 'tickets')
    assert max(hours, key=lambda hour: hours[hour]) in (9, 10, 11)


def test_the_day_budgets_are_pure_functions_of_the_date():
    cfg = Config()
    for channel in CHANNELS:
        random.seed(1)
        random.random()                                  # whatever else drew first
        picked = _daily_target(cfg, MONDAY, channel)
        assert _daily_target(cfg, MONDAY, channel) == picked   # stable across calls
        low = getattr(cfg.volumes, _BAND_ATTRS[channel] + '_min')
        high = getattr(cfg.volumes, _BAND_ATTRS[channel] + '_max')
        assert low <= picked <= high
        # Different dates still draw different budgets from the band.
        assert len({_daily_target(cfg, date(2026, 9, day), channel)
                    for day in range(1, 29)}) > 5
    # The three channels do not share a draw.
    assert len({_daily_target(cfg, MONDAY, channel) for channel in CHANNELS}) == 3


def test_the_three_channels_use_one_law_with_their_own_bands():
    cfg = Config()
    ctx = _ctx(cfg, MONDAY.replace(hour=10))
    tickets = _expectation(cfg, ctx, 30, MONDAY.date(), 'tickets')
    calls = _expectation(cfg, ctx, 30, MONDAY.date(), 'calls')
    chats = _expectation(cfg, ctx, 30, MONDAY.date(), 'chats')
    # Same law, so each channel's expectation is its own date-keyed budget times
    # the one shared shape (the scenario multiplier cancels between channels).
    assert calls == pytest.approx(tickets * _daily_target(cfg, MONDAY, 'calls')
                                  / _daily_target(cfg, MONDAY, 'tickets'))
    assert chats == pytest.approx(tickets * _daily_target(cfg, MONDAY, 'chats')
                                  / _daily_target(cfg, MONDAY, 'tickets'))
    # And `compute_counts` is exactly the three counts the channels publish.
    random.seed(SEED)
    counts = compute_counts(cfg, ctx, 30, MONDAY.date())
    assert len(counts) == 3 and all(isinstance(c, int) and c >= 0 for c in counts)


def test_simulation_minutes_per_tick_cannot_size_realtime_volume():
    """The config key that lied: realtime volume ignores the simulated clock."""
    cfg = Config()
    ctx = _ctx(cfg, MONDAY.replace(hour=10))
    baseline = tuple(_expectation(cfg, ctx, 30, MONDAY.date(), channel)
                     for channel in CHANNELS)
    for minutes in (5, 15, 30, 60):
        cfg.generator.simulation_minutes_per_tick = minutes
        assert tuple(_expectation(cfg, ctx, 30, MONDAY.date(), channel)
                     for channel in CHANNELS) == baseline
