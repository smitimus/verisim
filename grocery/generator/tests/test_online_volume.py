"""
Per-tick online-order volume law — verisim card t_eb31c99f.

`online._online_count_for_tick()` was

    daily  = random.randint(cfg.online.orders_per_day_min, cfg.online.orders_per_day_max)
    hour_w = cfg.volumes.hourly_weights[sim_dt.hour]
    return max(0, round(daily * hour_w * ctx.volume_multiplier))

Two independent errors in that one line, and they compound:

1. **No cadence divisor.** `ctx.volume_multiplier` is what `scenario_engine`
   builds from a *whole hour* of demand (`hourly_weights[hour] * 24`), so a tick
   has to carry the share of the hour it writes (`simulated_seconds / 3600`) —
   that is what `compute_pos_count()` does since t_94bbf1ce. Online had no
   divisor at all, so every one of the 120 ticks in an hour wrote a full hour's
   orders: realtime ran ~120x the law the backfill was writing.
2. **The hour weight applied twice.** `ctx.volume_multiplier` already contains
   `hourly_weights[hour] * 24`, so multiplying by `hour_w` again scaled the day
   by `24 * sum(w**2)` = 1.637 — and this one hit the backfill too (measured on
   the dev slot 2026-09-30: backfill days 306/352/375/397/406 orders against a
   configured 90..170).

Measured over the four dates in the card, seeded the same way, with the law
driven exactly as `main.py` drives it (one tick per simulated hour for the
backfill; 2880 ticks of 30 s for realtime):

    date        dow   old backfill   old realtime   budget x shape (target)
    2026-09-16  Wed            303        36,422        141 x 1.338 = 189
    2026-09-18  Fri            372        44,586        139 x 1.635 = 227
    2026-09-22  Tue            286        34,391        161 x 1.263 = 203
    2026-09-26  Sat            422        50,665         95 x 1.858 = 177

so realtime ran 169x-287x the day's own budget and the backfill 1.6x-1.7x of it
(counts are 20-seed means; `analyze_online_law.py` reproduces the table).

What these tests pin:

1. a realtime day totals what the date's configured budget and the day's shape
   imply — not ~120x it;
2. the hour weight is applied once: the expectation ratio between two hours is
   `w[a] / w[b]`, not `(w[a] / w[b]) ** 2`;
3. the realtime day and the backfill day of the same date write the same volume
   (they are the same law against the same date-pure budget);
4. a tick carries its share of the hour, and that share follows the interval the
   loop actually sleeps (`tick_interval_seconds`, settable 5-3600 s from the
   API), so changing the cadence does not change the day;
5. the quiet hours survive: at 30 s an online tick carries ~0.0008 orders in the
   dead hours and ~0.18 at the peak, so plain `round()` would write a stream of
   zeros and shrink the day;
6. the day's budget is a pure function of the date, is drawn from
   `online.orders_per_day` (not the POS band), and no longer redraws per tick;
7. there is no hard ordering window in the law — the skew to browsing hours is
   the shared hourly weights, which `ONLINE_HOURS` (a constant nothing ever
   referenced) pretended to be.
"""
import random
from datetime import date, datetime, timedelta

import pytest

from grocery.generator.config import Config
from grocery.generator.main import (
    SIM_HOUR_SECONDS,
    compute_online_count,
    daily_online_target,
    daily_pos_target,
    online_count_expectation,
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


def _shape(cfg, day):
    """The day's shape factor: the hour weights x dow x the rush boost.

    Computed from the config here rather than from the law under test, so it is
    an independent oracle for "the two channels shape a day the same way".
    """
    dow = day.strftime('%A').lower()
    dow_mult = cfg.volumes.day_of_week_multipliers[dow]
    total = 0.0
    for hour, weight in enumerate(cfg.volumes.hourly_weights):
        mult = 24.0 * weight * dow_mult
        if hour in cfg.scenarios.rush_hour_hours:
            mult *= cfg.scenarios.rush_hour_multiplier
        total += mult / 24.0
    return total


def expected_day_total(cfg, day):
    """Exact expectation: the date's budget x the day's shape."""
    return daily_online_target(cfg, day.date()) * _shape(cfg, day)


def realtime_day_hours(cfg, day, tick_seconds=None, seed=SEED):
    """One simulated day as the realtime loop writes it: dict hour -> count."""
    random.seed(seed)
    tick_seconds = tick_seconds or cfg.generator.tick_interval_seconds
    per_hour = {hour: 0 for hour in range(24)}
    for hour in range(24):
        base = day.replace(hour=hour)
        for offset in range(0, SIM_HOUR_SECONDS, tick_seconds):
            sim_dt = base + timedelta(seconds=offset)
            per_hour[hour] += compute_online_count(cfg, _ctx(cfg, sim_dt),
                                                   tick_seconds, day.date())
    return per_hour


def realtime_day_total(cfg, day, tick_seconds=None, seed=SEED):
    return sum(realtime_day_hours(cfg, day, tick_seconds, seed).values())


def backfill_day_total(cfg, day, seed=SEED):
    """The same day as the backfill writes it: one tick per simulated hour."""
    random.seed(seed)
    return sum(compute_online_count(cfg, _ctx(cfg, day.replace(hour=hour)),
                                    SIM_HOUR_SECONDS, day.date())
               for hour in range(24))


def test_realtime_day_totals_the_configured_volume_not_120x_it():
    cfg = Config()
    assert cfg.generator.tick_interval_seconds == 30
    for day in (MONDAY, SATURDAY):
        total = realtime_day_total(cfg, day)
        expected = expected_day_total(cfg, day)
        # Tolerances are measured, not guessed: over 60 seeds the realtime day
        # spans -15%..+10% of the expectation (sd 5-6%) — a day of ~200 orders
        # in 2880 stochastic ticks is never exact. The old law wrote 120 ticks of
        # a whole hour each, i.e. ~120x the backfill's own day, so the second
        # line below is the regression that matters.
        assert total == pytest.approx(expected, rel=0.25), (day, total, expected)
        assert total < expected * 2, (day, total, expected)
        # And the budget the day is built from is the configured band.
        assert cfg.online.orders_per_day_min <= daily_online_target(cfg, day.date())
        assert daily_online_target(cfg, day.date()) <= cfg.online.orders_per_day_max


def test_the_hour_weight_is_applied_once():
    """The double weight: the expectation ratio is w[a]/w[b], not its square."""
    cfg = Config()
    w = cfg.volumes.hourly_weights
    # Hours 12 and 03: neither is in rush_hour_hours, so the ratio is pure weight.
    assert 12 not in cfg.scenarios.rush_hour_hours
    assert 3 not in cfg.scenarios.rush_hour_hours
    noon = online_count_expectation(cfg, _ctx(cfg, MONDAY.replace(hour=12)),
                                    SIM_HOUR_SECONDS, MONDAY.date(), daily=240)
    night = online_count_expectation(cfg, _ctx(cfg, MONDAY.replace(hour=3)),
                                     SIM_HOUR_SECONDS, MONDAY.date(), daily=240)
    ratio = noon / night
    # w[12] = 0.0896, w[3] = 0.0008 -> 112; squaring it would give 12,544.
    assert ratio == pytest.approx(w[12] / w[3], rel=0.01), ratio
    assert ratio < 1000, ratio

    # With a context that carries no hour shaping of its own, an hour tick is
    # exactly 1/24 of the day's budget and a 30 s tick is 1/120 of the hour — the
    # hour enters once, through the context, and nowhere else.
    class _Flat:
        volume_multiplier = 1.0
        scenario_tag = 'normal'

    assert online_count_expectation(cfg, _Flat(), SIM_HOUR_SECONDS,
                                    MONDAY.date(), daily=240) == pytest.approx(240 / 24.0)
    assert online_count_expectation(cfg, _Flat(), 30, MONDAY.date(),
                                    daily=240) == pytest.approx(240 / 24.0 / 120)


def test_realtime_and_backfill_write_the_same_day_at_the_same_volume():
    cfg = Config()
    for day in (MONDAY, SATURDAY):
        total = realtime_day_total(cfg, day)
        backfill = backfill_day_total(cfg, day)
        assert total == pytest.approx(backfill, rel=0.25), (day, total, backfill)


def test_a_tick_carries_its_share_of_the_hour():
    """120 ticks of 30 s == the backfill's one hour tick, exactly."""
    cfg = Config()
    ctx = _ctx(cfg, MONDAY.replace(hour=18))
    ticks = SIM_HOUR_SECONDS // cfg.generator.tick_interval_seconds
    assert ticks == 120
    hour = online_count_expectation(cfg, ctx, SIM_HOUR_SECONDS, MONDAY.date(), daily=160)
    tick = online_count_expectation(cfg, ctx, cfg.generator.tick_interval_seconds,
                                    MONDAY.date(), daily=160)
    assert tick * ticks == pytest.approx(hour)
    assert online_count_expectation(cfg, ctx, 60, MONDAY.date(), daily=160) == pytest.approx(tick * 2)


def test_volume_follows_the_interval_the_loop_sleeps():
    cfg = Config()
    # run_tick sleeps control.generator_state.tick_interval_seconds (5-3600 s,
    # settable from the API/UI); that is the cadence the volume must track.
    assert realtime_tick_seconds(cfg, {'tick_interval_seconds': 30}) == 30
    assert realtime_tick_seconds(cfg, {'tick_interval_seconds': 300}) == 300

    # Ten times the interval, ten times the volume per tick — same day.
    total = realtime_day_total(cfg, SATURDAY, tick_seconds=300)
    assert total == pytest.approx(expected_day_total(cfg, SATURDAY), rel=0.25), total


def test_quiet_hours_are_thin_but_not_rounded_away():
    cfg = Config()
    daily = daily_online_target(cfg, MONDAY.date())
    quiet = _ctx(cfg, MONDAY.replace(hour=3))
    per_hour = online_count_expectation(cfg, quiet, SIM_HOUR_SECONDS, MONDAY.date(),
                                        daily=daily)
    per_tick = online_count_expectation(cfg, quiet, 30, MONDAY.date(), daily=daily)
    # Under one order in the whole dead hour — and far under one per 30 s tick.
    # `round()` returned 0 for every one of those ticks, so the quiet hours were
    # erased rather than thinned.
    assert 0 < per_hour < 1, per_hour
    assert 0 < per_tick < 0.01, per_tick
    # Carried as a probability, the hour still yields what the expectation says.
    draws = 400
    mean = sum(compute_online_count(cfg, quiet, SIM_HOUR_SECONDS, MONDAY.date())
               for _ in range(draws)) / draws
    assert mean > 0, mean
    assert mean == pytest.approx(per_hour, rel=0.5), (mean, per_hour)


def test_the_peak_hour_still_carries_its_share():
    cfg = Config()
    daily = daily_online_target(cfg, MONDAY.date())
    peak = online_count_expectation(cfg, _ctx(cfg, MONDAY.replace(hour=18)),
                                   SIM_HOUR_SECONDS, MONDAY.date(), daily=daily)
    assert 5 < peak < 40, peak                      # ~21 orders in the dinner hour
    draws = 400
    mean = sum(compute_online_count(cfg, _ctx(cfg, MONDAY.replace(hour=18)),
                                    SIM_HOUR_SECONDS, MONDAY.date())
               for _ in range(draws)) / draws
    assert mean == pytest.approx(peak, rel=0.3), (mean, peak)


def test_the_day_keeps_its_hourly_shape():
    cfg = Config()
    hours = realtime_day_hours(cfg, MONDAY)
    assert sum(hours.values()) == realtime_day_total(cfg, MONDAY)
    # Shape through the expectation (a ~170-order day realises the dead hours as
    # zeroes often enough that the realisation alone is a weak oracle).
    daily = daily_online_target(cfg, MONDAY.date())
    exp = {hour: online_count_expectation(cfg, _ctx(cfg, MONDAY.replace(hour=hour)),
                                          SIM_HOUR_SECONDS, MONDAY.date(), daily=daily)
           for hour in range(24)}
    assert max(exp, key=lambda hour: exp[hour]) == 18
    assert exp[18] > exp[3] * 50, exp
    assert exp[8] > exp[3] * 20, exp


def test_the_day_budget_is_a_pure_function_of_the_date():
    cfg = Config()
    picked = {}
    for day in (date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 26)):
        random.seed(1)
        random.random()                                  # whatever else drew first
        picked[day] = daily_online_target(cfg, day)
        assert daily_online_target(cfg, day) == picked[day]  # stable across calls
        assert cfg.online.orders_per_day_min <= picked[day]
        assert picked[day] <= cfg.online.orders_per_day_max
    # Different dates still draw different budgets from the band.
    assert len({daily_online_target(cfg, date(2026, 9, day)) for day in range(1, 29)}) > 5


def test_the_budget_comes_from_the_online_band_not_the_pos_one():
    """`online.orders_per_day` is the channel's own day budget."""
    cfg = Config()
    day = date(2026, 9, 21)
    baseline = daily_online_target(cfg, day)
    for low, high in ((5, 9), (400, 450)):
        cfg.online.orders_per_day_min, cfg.online.orders_per_day_max = low, high
        drawn = daily_online_target(cfg, day)
        assert low <= drawn <= high, drawn
        assert drawn != baseline, drawn
    # The POS band cannot move it: the online day is sized from the online config.
    cfg.online.orders_per_day_min, cfg.online.orders_per_day_max = 90, 170
    assert daily_online_target(cfg, day) == baseline
    cfg.volumes.pos_transactions_per_day_min = 1
    cfg.volumes.pos_transactions_per_day_max = 2
    assert daily_online_target(cfg, day) == baseline


def test_no_hard_ordering_window_in_the_law():
    """`ONLINE_HOURS` was dead: the browsing-hours skew is the hourly weights.

    Ordering runs all day, shaped by the shared weights (0.0008 at 03:00 against
    0.0938 at 18:00 — a 117x skew). What the law must not do is cut the day at
    the hardcoded 7am-9pm boundary that constant described: every hour of the day
    would then be *exactly* zero, which is not what a weight of 0.0008 means.
    """
    cfg = Config()
    daily = daily_online_target(cfg, MONDAY.date())
    for hour in range(24):
        expected = online_count_expectation(cfg, _ctx(cfg, MONDAY.replace(hour=hour)),
                                            SIM_HOUR_SECONDS, MONDAY.date(), daily=daily)
        assert expected > 0, (hour, expected)


def test_the_two_channels_shape_a_day_the_same_way():
    """Online and POS must not drift into two different laws again."""
    cfg = Config()
    for day in (MONDAY, SATURDAY):
        online_shape = 0.0
        pos_shape = 0.0
        for hour in range(24):
            online_shape += online_count_expectation(
                cfg, _ctx(cfg, day.replace(hour=hour)), SIM_HOUR_SECONDS, day.date(),
                daily=daily_online_target(cfg, day.date()))
            pos_shape += pos_count_expectation(
                cfg, _ctx(cfg, day.replace(hour=hour)), SIM_HOUR_SECONDS, day.date(),
                daily=daily_pos_target(cfg, day.date()))
        assert (online_shape / daily_online_target(cfg, day.date())
                == pytest.approx(pos_shape / daily_pos_target(cfg, day.date())))
