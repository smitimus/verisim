"""
Tick cadence, deadline lag and overrun: is the generator keeping up?

`control.generation_stats.wall_clock_ms` has recorded what each tick COST since
the ledger existed, and nothing ever asked whether that cost was acceptable. The
failure mode is the one `grocery/test-cycles-final-report.md` documents as a
risk without ever measuring it.

Two facts drive the design, and the second one is why the naive version of this
feature is wrong:

* **The loop's sleep is unconditional.** `run_tick(...)` then
  `time.sleep(interval)` means one iteration is `interval + work`, not
  `interval`. So lag measured against a plain `interval` schedule grows without
  bound on a *healthy* box — 0.5s per tick is 24 minutes per simulated day, and
  5s per tick is 4 hours. A fixed absolute threshold on that number either
  fires forever or never. Pinned below by replaying the loop's own wall clock.
* **What the operator actually asks is "is it keeping up *for this box*".** The
  deadline is therefore `interval + this generator's recent tick cost` (an
  EWMA), which is bounded by construction and settles at ~0 on a healthy
  generator, while still catching a step change.

The overrun ratio is the signal that needs no tuning at all: it compares one
tick's cost to the interval directly, so a generator that takes 45s per tick
against a 30s cadence trips it on every tick even though its deadline lag reads
zero.

A fake clock drives all of it: every test runs in microseconds and nothing here
sleeps.
"""
from datetime import datetime

import pytest

from grocery.generator.observability import (
    COST_EWMA_ALPHA,
    DEFAULT_ALERT_LAG_SECONDS,
    TickCadence,
)

INTERVAL = 30


class FakeClock:
    """A hand-cranked monotonic clock. `advance` moves it forward only."""

    def __init__(self, start=1000.0):
        self.now = float(start)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += float(seconds)
        return self.now


def _tick(cadence, clock, cost, interval=INTERVAL):
    """One loop iteration in the order main.py actually runs it.

    begin -> do the work -> end -> sleep. Getting that order wrong is how a test
    ends up asserting a schedule the process does not run, so it is written once
    here and every test below uses it.
    """
    cadence.begin(interval)
    clock.advance(cost)
    cadence.end()
    clock.advance(interval)


def _replay(clock, cadence, work_per_tick, n_ticks, interval=INTERVAL):
    """Run the main loop's own wall clock: begin, work, end, sleep."""
    for _ in range(n_ticks):
        _tick(cadence, clock, work_per_tick, interval)
    return cadence


# ---------------------------------------------------------------------------
# the healthy case
# ---------------------------------------------------------------------------

def test_a_tick_that_takes_exactly_its_interval_is_on_time():
    """The floor of the model: cost == interval means the loop keeps up exactly."""
    clock = FakeClock()
    cadence = _replay(clock, TickCadence(clock=clock), INTERVAL, 5)
    assert cadence.lag_seconds == 0.0
    assert cadence.lateness_seconds == 0.0


def test_the_ordinal_counts_ticks_and_the_prefix_names_the_simulated_stamp():
    clock = FakeClock()
    cadence = TickCadence(clock=clock)

    cadence.begin(INTERVAL)
    cadence.end()
    clock.advance(INTERVAL + 1)
    cadence.begin(INTERVAL)
    cadence.end()

    assert cadence.ticks == 2
    stamp = datetime(2026, 10, 4, 14, 30, 0)
    assert cadence.prefix(stamp) == "[tick 2][2026-10-04 14:30:00]"


def test_the_prefix_carries_the_simulated_stamp_not_the_wall_clock():
    """The prefix has to identify a BACKFILLED hour as backfilled.

    In realtime sim_dt is now(), so a wall-clock prefix would be identical — and
    would only start lying during a backfill, which is exactly when the log line
    has to tell history apart from live. A test that could not tell them apart
    would pass for the wrong reason.
    """
    clock = FakeClock(start=1_700_000_000.0)          # 2023-11-14, say
    cadence = TickCadence(clock=clock)
    cadence.begin(INTERVAL)
    cadence.end()

    backfilled_hour = datetime(2026, 8, 22, 3, 0, 0)   # the hour being WRITTEN
    assert "[2026-08-22 03:00:00]" in cadence.prefix(backfilled_hour)


def test_duration_is_the_wall_clock_cost_of_the_tick():
    clock = FakeClock()
    cadence = TickCadence(clock=clock)
    cadence.begin(INTERVAL)
    clock.advance(1.25)
    assert cadence.end() == 1250
    assert cadence.snapshot()["duration_ms"] == 1250


# ---------------------------------------------------------------------------
# the fact the whole design turns on
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("work", [0.05, 0.5, 5.0])
def test_cheap_ticks_do_not_accrue_lag_over_a_whole_simulated_day(work):
    """A healthy generator stays on time for 2880 ticks (one simulated day).

    Replay the loop exactly — begin, work, end, sleep — and assert the lag at the
    END of the day. A schedule anchored at a bare `interval` would report 2.4
    minutes here at 50ms/tick and 4 hours at 5s/tick, on a generator doing
    nothing wrong, and a 120s threshold would fire on the second day forever.
    That is the bug this model exists to avoid, so it is pinned on the whole day
    rather than on a few ticks.
    """
    clock = FakeClock()
    cadence = _replay(clock, TickCadence(clock=clock), work, 2880)

    assert cadence.lag_seconds < 1.0, (
        f"a generator whose ticks cost {work}s against a {INTERVAL}s cadence "
        f"reported {cadence.lag_seconds:.1f}s of lag after one simulated day")
    assert cadence.overran is False


def test_a_slow_but_steady_generator_is_late_only_until_the_deadline_catches_up():
    """45s ticks against a 30s cadence: slow, but not 'falling behind'.

    The deadline absorbs the cost, so the lag is a transient that the EWMA
    settles out of — and this is exactly the case a bare-`interval` schedule
    would report as 36 hours behind after one day. The signal that IS
    legitimately alarming here is the overrun, asserted separately below.
    """
    clock = FakeClock()
    cadence = _replay(clock, TickCadence(clock=clock), 45.0, 400)

    assert cadence.lag_seconds < 60.0, (
        f"a steady 45s/tick generator reported {cadence.lag_seconds:.1f}s of lag")
    assert cadence.overran is True           # ...but it does overrun, every tick
    assert cadence.overrun_ratio == pytest.approx(1.5)


# ---------------------------------------------------------------------------
# the defect the card exists for: a tick that overruns
# ---------------------------------------------------------------------------

def test_a_tick_above_the_recent_average_is_late_by_exactly_the_difference():
    """What an overrun actually costs in lateness: cost minus the average.

    Not "nothing" and not "the whole cost". The loop's sleep is unconditional,
    so an iteration advances the wall clock by `cost + interval`; the deadline
    only advances by `interval + ewma`. A tick therefore lands late by
    `cost - ewma` — the part of its cost that exceeded what this generator has
    been averaging, which is the only part that is genuinely 'extra'.

    That is the whole design in one line: a steady generator converges its EWMA
    onto its cost and accrues nothing, so the number cannot fire on a slow box,
    while a spike shows up immediately and decays as the average catches up.
    """
    clock = FakeClock()
    cadence = TickCadence(clock=clock)

    for _ in range(5):
        _tick(cadence, clock, 0.5)         # cheap baseline: ewma -> 0.5s
    ewma_before = cadence.snapshot()["cost_ewma_seconds"]
    assert ewma_before == pytest.approx(0.5)

    # A 120s tick where 0.5s was expected. `end()` folds the cost into the EWMA
    # before the next tick is measured, so the reference the deadline uses is
    # already partway to 120s — 0.5 + 0.05 * (120 - 0.5) = 6.475s. Measuring
    # against the pre-spike average instead would over-state the debt by exactly
    # that contribution and make the alert fire harder on a one-off blip.
    cadence.begin(INTERVAL)
    clock.advance(120.0)
    cadence.end()
    assert cadence.overran is True
    ewma_after = cadence.snapshot()["cost_ewma_seconds"]
    assert ewma_after == pytest.approx(0.5 + 0.05 * (120.0 - 0.5))

    clock.advance(INTERVAL)                # the loop's unconditional sleep

    lateness = cadence.begin(INTERVAL)
    assert lateness == pytest.approx(120.0 - ewma_after, abs=0.01)
    assert cadence.lag_seconds == lateness

    # ...and it decays as the average climbs toward the expensive cost.
    seen = [lateness]
    for _ in range(80):
        _tick(cadence, clock, 0.5)
        seen.append(cadence.lateness_seconds)
    assert seen[-1] < seen[0]
    assert cadence.lag_seconds == 0.0


def test_a_run_of_ticks_above_the_recent_average_reports_as_lag():
    """The lag signal is 'costing more than this generator's recent average'.

    Once the EWMA is high, a tick that stays above it accumulates real debt: each
    such iteration is longer than the deadline the previous one set. This is the
    case deadline lag exists to catch, and it is distinguishable from "this box is
    simply slow" precisely because the deadline has already learned the slow
    cost.
    """
    clock = FakeClock()
    cadence = TickCadence(clock=clock)

    for _ in range(10):
        _tick(cadence, clock, 0.5)        # cheap baseline
    assert cadence.lag_seconds < 1.0

    for _ in range(10):
        _tick(cadence, clock, 30.0)       # ticks well above that baseline

    assert cadence.lag_seconds > 0.0, (
        f"ticks costing 30s against a cheap baseline reported no lag at all "
        f"(lateness={cadence.lateness_seconds})")


def test_lag_decays_as_the_deadline_learns_the_new_cost():
    """A slow spell is a transient, not a permanent sentence.

    Once the ticks go back to cheap, the EWMA falls, the deadline comes back
    down, and the lag must clear — otherwise one bad patch (a checkpoint, a
    vacuum, a co-tenant) would pin the generator above the alert threshold for
    the rest of the process's life, which is how an alert gets switched off.

    Both halves of the recovery are asserted: the lag drops below its peak
    promptly, and it reaches a bounded residual rather than oscillating — both
    the real period and the deadline period converge on `interval + cost`, so a
    generator that keeps up settles instead of chasing its own tail.
    """
    clock = FakeClock()
    cadence = TickCadence(clock=clock)

    for _ in range(10):
        _tick(cadence, clock, 0.5)
    for _ in range(10):
        _tick(cadence, clock, 30.0)
    assert cadence.lag_seconds > 0.0
    peak = cadence.lag_seconds

    seen = []
    for _ in range(400):
        _tick(cadence, clock, 0.5)
        seen.append(cadence.lateness_seconds)

    assert seen[0] < peak, f"the lag did not start falling (was {peak:.1f}s)"
    assert cadence.lag_seconds == 0.0, f"the lag never cleared (was {peak:.1f}s)"
    # And the lateness crossed zero on the way, i.e. the generator got ahead of a
    # deadline that no longer fit it — the deadline really did come back down.
    assert min(seen) < 0, "lateness never went negative — debt cannot be repaid"


def test_a_tick_that_runs_fast_reaps_debt_and_lateness_goes_negative():
    """Signed lateness is the point of a deadline model.

    After a stall, ticks that keep up while the debt is repaid read NEGATIVE
    lateness — ahead of where the deadline says they should be. A metric that
    clamped at zero would show a generator stuck at its peak lag forever, which
    is a different (and wrong) diagnosis.
    """
    clock = FakeClock()
    cadence = TickCadence(clock=clock)

    for _ in range(5):
        _tick(cadence, clock, 0.5)      # cheap baseline
    _tick(cadence, clock, 300.0)       # a five-minute tick: a real stall

    seen = []
    for _ in range(80):
        _tick(cadence, clock, 0.5)
        seen.append(cadence.lateness_seconds)

    assert max(seen) > 0, "no lag was ever accrued to repay"
    assert min(seen) < 0, "lateness never went negative — debt cannot be repaid"
    assert cadence.lag_seconds >= 0.0      # the operator-facing number floors at 0


def test_reset_drops_the_schedule_and_the_stale_cost_reference():
    """A generator that is not generating owes realtime nothing.

    Two things must go, and the second is easy to miss: without dropping the cost
    EWMA, a generator that was writing 120s ticks before a pause and 50ms ticks
    after it is measured against a deadline that no longer describes its box, and
    reports an hour of lag that describes the pause rather than its health. An
    operator who learns to trust the number learns to ignore it.
    """
    clock = FakeClock()
    cadence = TickCadence(clock=clock)

    # A cheap baseline, then a sustained expensive spell — the shape that actually
    # accrues lag (a single expensive tick is absorbed by the loop's own sleep).
    for _ in range(10):
        _tick(cadence, clock, 0.5)
    for _ in range(10):
        _tick(cadence, clock, 30.0)
    assert cadence.lag_seconds > 0.0

    cadence.reset()
    assert cadence.lag_seconds == 0.0
    assert cadence.lateness_seconds == 0.0
    assert cadence.overran is False
    assert cadence.snapshot()["cost_ewma_seconds"] is None

    # The next tick re-anchors and re-establishes the reference on its own, and
    # reports zero — it cannot be late against a schedule that just came into
    # existence, whatever the box was doing before the pause.
    clock.advance(INTERVAL)
    assert cadence.begin(INTERVAL) == 0.0


def test_a_changed_interval_reschedules_rather_than_banking_the_gap():
    """An operator raising tick_interval_seconds expects the schedule to follow.

    Not re-anchoring would report a tick due at 30s under the old cadence and
    found at 300s under the new one as 270s of lag it never incurred. The
    next tick settles onto the new cadence.
    """
    clock = FakeClock()
    cadence = TickCadence(clock=clock)

    _tick(cadence, clock, 1.0)           # one tick on the 30s cadence

    # Operator moves the cadence 30s -> 300s between ticks.
    assert cadence.begin(300) < 300.0, "a cadence change was banked as lag"

    _tick(cadence, clock, 1.0, interval=300)
    assert cadence.lag_seconds == 0.0


# ---------------------------------------------------------------------------
# the overrun signal, which needs no threshold
# ---------------------------------------------------------------------------

def test_overran_and_its_ratio_are_set_when_a_tick_exceeds_its_interval():
    clock = FakeClock()
    cadence = TickCadence(clock=clock)
    cadence.begin(INTERVAL)
    clock.advance(INTERVAL + 0.5)           # took 30.5s to do 30s of work
    cadence.end()
    assert cadence.overran is True
    assert cadence.overrun_ratio == pytest.approx(1.0167, abs=1e-3)


def test_a_tick_exactly_on_its_interval_does_not_count_as_overrunning():
    """`>`, not `>=`: a tick that uses exactly its budget has not overrun it.

    Rounding makes this reachable in practice — a 29999ms tick against a 30s
    interval is not late — and an inclusive comparison would flag the boundary
    case on every healthy tick.
    """
    clock = FakeClock()
    cadence = TickCadence(clock=clock)
    cadence.begin(INTERVAL)
    clock.advance(INTERVAL)
    cadence.end()
    assert cadence.overran is False
    assert cadence.overrun_ratio == pytest.approx(1.0)


def test_the_overrun_flags_reset_at_the_start_of_the_next_tick():
    """A per-tick flag read after the next `begin` is stale, not current.

    Cheap to pin and cheap to get wrong: `_log_tick` reads `overran` right after
    `end()`, so it is correct today, but leaving a stale True around invites the
    next caller to report an overrun for a tick that did not have one.
    """
    clock = FakeClock()
    cadence = TickCadence(clock=clock)
    cadence.begin(INTERVAL)
    clock.advance(120)
    cadence.end()
    assert cadence.overran is True

    clock.advance(INTERVAL)
    cadence.begin(INTERVAL)
    assert cadence.overran is False
    assert cadence.overrun_ratio == 0.0


# ---------------------------------------------------------------------------
# the contract with the ledger, and with the API
# ---------------------------------------------------------------------------

def test_the_ledger_reconstructs_the_same_mean_period_the_api_reports():
    """`mean_period_s = span / (n - 1)` is the windowed cost of one tick.

    This is the property that lets `/metrics` serve tick timing from
    `control.generation_stats` with no state of its own: only `recorded_at`'s
    first and last value and a count are needed. Pinned against a real cadence
    rather than asserted in a comment, because the two implementations drift the
    moment either is edited alone.

    `grocery/api/tests/test_tick_metrics.py` computes the same closed form from
    the route's own SQL and asserts it against the ledger.
    """
    clock = FakeClock()
    cadence = TickCadence(clock=clock)
    durations = [1.0, 2.0, 0.5, 4.0, 1.5]

    starts = []
    for work in durations:
        cadence.begin(INTERVAL)
        starts.append(clock.now)
        clock.advance(work)                 # the tick's own cost...
        cadence.end()
        # ...then the loop sleeps its interval, so one iteration really is
        # `work + interval` and the mean period must reflect that.
        clock.advance(INTERVAL)

    span = starts[-1] - starts[0]
    mean_period = span / (len(starts) - 1)

    # `recorded_at` is DEFAULT NOW() written by the ledger INSERT, which happens
    # at the END of the tick — so first..last spans n-1 whole iterations, and the
    # last tick's own cost is NOT in the window. The API's
    # `mean_overrun_ms = (mean_period - interval) * 1000` therefore estimates the
    # mean cost of the n-1 COMPLETED iterations, and must be documented that way;
    # getting it wrong would make the endpoint silently report a stale tick's cost
    # as if it were the current one's.
    mean_overrun_ms = (mean_period - INTERVAL) * 1000
    assert mean_period == pytest.approx(
        INTERVAL + sum(durations[:-1]) / (len(durations) - 1))
    assert mean_overrun_ms == pytest.approx(
        sum(durations[:-1]) / (len(durations) - 1) * 1000)


def test_the_mean_cost_matches_the_ewma_the_deadline_uses():
    """A window of identical ticks must converge the EWMA onto that cost.

    Without this the deadline could drift off the tick cost it is supposed to
    represent, and the lag would quietly become meaningless again.
    """
    clock = FakeClock()
    cadence = _replay(clock, TickCadence(clock=clock), 3.0, 200)
    assert cadence.snapshot()["cost_ewma_seconds"] == pytest.approx(3.0, abs=0.01)


def test_end_without_begin_raises_rather_than_reporting_a_zero_duration():
    """`wall_clock_ms` is written to the ledger from `end()`.

    A silent 0 there is not a missing number, it is a false measurement: the
    ledger's whole purpose is to say what a tick cost, and 0ms reads as "free".
    """
    cadence = TickCadence(clock=FakeClock())
    with pytest.raises(RuntimeError, match="never timed"):
        cadence.end()


def test_a_non_positive_interval_is_refused_with_an_explanation():
    """An interval of 0 would make every deadline meaningless.

    Reachable: `tick_interval_seconds` is settable from the API between 5 and
    3600, and a state row written by hand can hold anything.
    """
    cadence = TickCadence(clock=FakeClock())
    for bad in (0, -1, None):
        with pytest.raises(ValueError, match="positive"):
            cadence.begin(bad)


# ---------------------------------------------------------------------------
# the threshold
# ---------------------------------------------------------------------------

def test_lagging_reports_against_the_configured_threshold():
    clock = FakeClock()
    cadence = TickCadence(clock=clock)
    cadence.lag_seconds = 45.0
    assert cadence.lagging(30) is True
    assert cadence.lagging(600) is False


def test_the_threshold_is_inclusive_at_the_boundary():
    """`>=`, not `>`: a lag sitting exactly on the threshold has met it."""
    cadence = TickCadence(clock=FakeClock())
    cadence.lag_seconds = 120.0
    assert cadence.lagging(120.0) is True
    assert cadence.lagging(120.001) is False


def test_a_zero_threshold_alerts_on_any_lag_rather_than_erroring():
    """`tick_lag_alert_seconds: 0` is a legitimate "tell me everything" config."""
    cadence = TickCadence(clock=FakeClock())
    cadence.lag_seconds = 0.0
    assert cadence.lagging(0.0) is True


def test_the_ewma_alpha_is_a_fraction_so_the_mean_converges():
    """0 < alpha <= 1, or the EWMA either never follows a change or ignores it."""
    assert 0.0 < COST_EWMA_ALPHA <= 1.0


def test_the_default_threshold_is_the_documented_120_seconds():
    """config.py, config.yaml and this default must be the same number.

    The two other files are checked by the config tests; this pins the fallback
    that covers a config.yaml with no `observability:` block at all — i.e. every
    install generated before this card.
    """
    from grocery.generator import config as gc

    assert DEFAULT_ALERT_LAG_SECONDS == 120.0
    assert gc.ObservabilityConfig().tick_lag_alert_seconds == 120.0