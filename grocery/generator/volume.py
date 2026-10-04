"""
The volume law — how many transactions a tick writes, for both channels.

One law, two channels: a tick carries `simulated_seconds / 3600` of the hour's
demand, the hour's demand is the date's budget draw shaped by the scenario
context, and the date's budget is a pure function of the date. `pos` and
`online` differ only in which config band their budget comes from — the two
private laws that drifted apart are exactly how t_94bbf1ce (POS, 30x) and
t_eb31c99f (online, ~120x plus the hour weight twice) happened.

Split out of `generator/main.py` (t_c2eca5dd); `main.py` re-exports every name.
`tests/test_pos_volume.py` and `test_online_volume.py` pin this law.
"""
import logging
import random

log = logging.getLogger('grocery-generator')


# ---------------------------------------------------------------------------
# the volume law
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Volume calculation
#
# One law, two channels: a tick carries `simulated_seconds / 3600` of the hour's
# demand, the hour's demand is the date's budget draw shaped by the scenario
# context, and the date's budget is a pure function of the date. `pos` and
# `online` differ only in which config band their budget comes from — the two
# private laws that drifted apart are exactly how t_94bbf1ce (POS, 30x) and
# t_eb31c99f (online, ~120x plus the hour weight twice) happened.
# ---------------------------------------------------------------------------

# One simulated hour of demand. The backfill writes exactly one tick per
# simulated hour; realtime writes one tick per `tick_interval_seconds` of wall
# clock, so the same hour arrives in 120 ticks at the default 30 s cadence
# (2880 a day) — that ratio is what the per-tick volume must be divided by.
SIM_HOUR_SECONDS = 3600

DEFAULT_TICK_INTERVAL_SECONDS = 30


def realtime_tick_seconds(cfg, state) -> int:
    """Seconds of simulated time one realtime tick writes.

    The main loop sleeps `control.generator_state.tick_interval_seconds` (seeded
    from config, settable 5-3600 s from the API/UI) — that is the cadence the
    generator really runs at, so it is the one the volume law must use, not
    `simulation_minutes_per_tick`.
    """
    value = (state or {}).get('tick_interval_seconds') or cfg.generator.tick_interval_seconds
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return DEFAULT_TICK_INTERVAL_SECONDS
    return seconds if seconds > 0 else DEFAULT_TICK_INTERVAL_SECONDS


def daily_volume_target(sim_date, seed_prefix, low, high) -> int:
    """A channel's day budget — a pure function of the date.

    Neither writer may own the day's volume: the backfill replays a day one
    simulated hour at a time (and resumes a partial day after a restart), while
    realtime writes it 120 times an hour. Drawing the band once per call would
    hand the same date a different total depending on who wrote it, and a resumed
    day a different total from an uninterrupted one — the same reasoning that made
    `timeclock.plan_shift()` a pure function of (date, employee id) (t_ca6642e0).
    The seed is the channel plus the ISO date, so it is stable across processes
    and runs (and the two channels do not draw the same number); different dates
    still draw different budgets from the configured band.
    """
    rng = random.Random('%s-%s' % (seed_prefix, sim_date.isoformat()))
    return rng.randint(low, high)


def daily_pos_target(cfg, sim_date) -> int:
    """The day's configured POS transaction budget (see `daily_volume_target`)."""
    return daily_volume_target(sim_date, 'verisim-pos',
                               cfg.volumes.pos_transactions_per_day_min,
                               cfg.volumes.pos_transactions_per_day_max)


def daily_online_target(cfg, sim_date) -> int:
    """The day's configured online-order budget (see `daily_volume_target`).

    `online.orders_per_day` is the channel's baseline day: the realised day is
    that draw shaped by the same hour weights, day-of-week multiplier and
    automatic rush-hour boost POS uses, so a day for either channel is
    `budget x shape` and neither channel can drift into its own convention.
    """
    return daily_volume_target(sim_date, 'verisim-online',
                               cfg.online.orders_per_day_min,
                               cfg.online.orders_per_day_max)


def per_tick_volume_expectation(daily, scenario_ctx, simulated_seconds) -> float:
    """Un-rounded volume for ONE tick, before sampling — the shared law.

    `scenario_ctx.volume_multiplier` already carries the hour-of-day weight
    (`hourly_weights[hour] * 24`) and the day-of-week / rush-hour multipliers —
    see `scenario_engine.get_scenario_context` — so one simulated hour's demand is
    `daily * volume_multiplier / 24`, and a tick carries that hour's share:

        per_tick = (daily / 24) * volume_multiplier * simulated_seconds / 3600

    Sizing a tick from anything else is how both channel defects happened:
    `simulation_minutes_per_tick` (a 96-tick day) while realtime ticks 2880 times
    a day made every realtime POS day exactly 30x the configured volume
    (t_94bbf1ce); online had no divisor at all *and* multiplied by
    `hourly_weights[hour]` a second time on top of the one already inside the
    multiplier, which inflated its backfill days 1.6x as well as its realtime days
    ~120x (t_eb31c99f).
    """
    per_hour = (daily / 24.0) * scenario_ctx.volume_multiplier
    return max(0.0, per_hour * simulated_seconds / SIM_HOUR_SECONDS)


def pos_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date, daily=None) -> float:
    """POS volume for one tick, from the date's POS budget.

    `daily` is the date's draw from `volumes.pos_transactions_per_day`; callers
    pass it only to make the arithmetic testable.
    """
    if daily is None:
        daily = daily_pos_target(cfg, sim_date)
    return per_tick_volume_expectation(daily, scenario_ctx, simulated_seconds)


def online_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date, daily=None) -> float:
    """Online-order volume for one tick, from the date's online budget.

    Sibling of `pos_count_expectation`, same law, its own configured band.
    """
    if daily is None:
        daily = daily_online_target(cfg, sim_date)
    return per_tick_volume_expectation(daily, scenario_ctx, simulated_seconds)


def compute_pos_count(cfg, scenario_ctx, simulated_seconds, sim_date) -> int:
    """Integer POS transaction count for one tick."""
    return _unbiased_count(
        pos_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date))


def compute_online_count(cfg, scenario_ctx, simulated_seconds, sim_date) -> int:
    """Integer online-order count for one tick.

    Same sampling as POS, and here it is not a nicety: at 30 s an online tick
    carries ~0.0008 orders in the dead hours and ~0.18 at the 18:00 peak against
    the configured 90..170 band, so `round()` returned 0 for every tick of the
    quiet half of the day and shrank the day (t_eb31c99f).
    """
    return _unbiased_count(
        online_count_expectation(cfg, scenario_ctx, simulated_seconds, sim_date))


def _unbiased_count(expected) -> int:
    """Round a fractional count to an int without biasing the mean.

    A 30 s tick carries well under one transaction outside the peaks (0.011 in
    the dead hours, 2.6 at the 18:00 peak for POS; 0.0008 and 0.18 for online),
    so plain `round()` would return 0 for every tick of the quiet half of the
    night and quietly shrink the day. Carrying the fraction as a probability keeps
    the hour's expected volume exact while still writing an integer per tick.
    """
    if expected <= 0:
        return 0
    whole = int(expected)
    return whole + (1 if random.random() < expected - whole else 0)

