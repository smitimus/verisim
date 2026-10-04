"""
Tick-level observability for the grocery generator (t_196d8da2).

`control.generation_stats` records `wall_clock_ms` per tick, so *what a tick
cost* has been on disk since the ledger existed — but nothing ever said whether
that cost was ACCEPTABLE. The failure mode is the one
`grocery/test-cycles-final-report.md` describes as a risk without ever measuring
it: a tick that overruns its interval does not merely take longer, it pushes
every later tick back.

## The scheduling fact this module is built on, and why it is surprising

The realtime loop is

    run_tick(...)                          # costs `work` seconds
    time.sleep(state['tick_interval_seconds'])

and the sleep is UNCONDITIONAL — the tick's own cost is not subtracted from it.
One iteration is therefore `interval + work`, not `interval`. Measured by
replaying the loop's own wall clock over one simulated day (2880 ticks at the
default 30s cadence):

| tick cost | real time for one simulated day | steady-state lag vs a 30s schedule |
|-----------|-----------------------------------|-----------------------------------|
| 0.05s     | 24h 2.4min                       | 2.4 min                          |
| 0.5s      | 24h 24min                        | 24 min                           |
| 5s        | 25h 40min                        | 4 hours                          |
| 45s       | 41h                              | 36 hours                         |

So **absolute lag against a fixed-rate schedule grows without bound on a
perfectly healthy box** — any tick that does real work adds its cost, every day,
forever. A metric like that cannot carry an absolute alert threshold: the number
that means "trouble" on a fast machine is normal on a slow one, so a fixed
threshold either fires forever or never. It is also not what an operator is
asking about, which is "is the generator keeping up *for this box*".

## What this measures instead

Two numbers, deliberately of different kinds, because they answer different
questions and only one of them can be expressed in absolute seconds.

**1. Deadline lag — bounded by construction, steady state near zero.** A tick is
due no earlier than `interval` after the previous tick *plus what this
generator's ticks have actually been costing*. That reference is an
exponentially-weighted mean of recent tick costs (`COST_EWMA_ALPHA`), so the
schedule adapts to the box instead of drifting away from it, while still
registering a step change — a tick that suddenly costs 120s pushes the deadline
out, and the following ticks read as late until the mean catches up. Lag here is
`now - due`, floored at 0 for the operator-facing value: being ahead of a
schedule the generator set for itself is not a problem.

**2. Overrun — the un-normalisable signal, and the card's primary ask.** A single
tick that cost more than its interval. It is a ratio (`cost / interval`), needs
no threshold tuning, and cannot be made to look healthy by being slow
everywhere: that is the difference between "this box is slow" and "this tick went
wrong". The hard case — a tick taking 45s against a 30s cadence, forever — trips
this on every tick, while its deadline lag sits at zero because the deadline
learned the 45s.

`TickCadence` owns those two and the tick ordinal. It does not decide what is
alarming (the threshold is config, `observability.tick_lag_alert_seconds`), does
not touch the database, and logs nothing — `main.py` owns the single instance
and the single log line per tick, so there is exactly one place to look for how
a tick is reported.

## The contract with the API

The API serves `/metrics` from `control.generation_stats` with no state of its
own, which it can do because the other two facts here are pure functions of the
ledger: over a window of n ticks,

    mean_period_s      = span / (n - 1)
    mean_overrun_ms    = (mean_period_s - interval) * 1000

Both need only `recorded_at` and `wall_clock_ms` — the first and last values and
a count. They are the windowed average of the same quantity this module tracks
per tick, and `grocery/api/tests/test_tick_metrics.py` pins the two against each
other so they cannot drift.

Stdlib only: the generator image stays slim.
"""
import time
from typing import Optional


# How fast the deadline reference follows a change in this generator's own tick
# cost. 0.05 ≈ a 20-tick memory: fast enough that a step change (a tick that
# suddenly costs 120s) shows up as lag within a handful of ticks, slow enough
# that one expensive tick does not redefine the schedule for the rest of the day.
COST_EWMA_ALPHA = 0.05


class TickCadence:
    """The generator's account of when a tick was due, how late it was, and
    whether it overran on its own.

    `clock` is injected so the recurrence can be tested without sleeping. It
    must be monotonic — the caller passes `time.monotonic`; a wall clock would
    make a backwards NTP step read as the generator sprinting ahead.
    """

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._due_at: Optional[float] = None
        self._started_at: Optional[float] = None
        self._interval_seconds = 0
        # The deadline reference: an EWMA of recent tick costs, in seconds.
        # None until this generator has a sample, because there is nothing to
        # reference before the first tick finishes.
        self._cost_ewma: Optional[float] = None
        # Cumulative, for the log line only. A restart resets it, deliberately:
        # it is a per-process counter, not a durable one, and the ledger is
        # where a durable count lives.
        self.ticks = 0
        self.lateness_seconds = 0.0
        self.lag_seconds = 0.0
        self.duration_ms = 0
        self.overran = False
        self.overrun_ratio = 0.0

    # -- state ---------------------------------------------------------------

    def reset(self):
        """Forget the schedule. Next `begin` anchors a new one.

        Called whenever the generator stops writing realtime data — stopped,
        paused, or mid-backfill. Two reasons, and the second matters more:

        * a generator that is not generating owes realtime nothing, so it must
          not greet the morning reporting an overnight stop as "lag";
        * the cost EWMA is stale after a stop. A generator that was writing
          120s ticks before a pause and 50ms ticks after it would otherwise be
          measured against a deadline that no longer describes its box.

        Losing the first tick's cost sample after a reset is the price of that
        honesty: the first tick back re-establishes the reference on its own,
        and that tick's own overrun is still reported.
        """
        self._due_at = None
        self._started_at = None
        self._interval_seconds = 0
        self._cost_ewma = None
        self.lateness_seconds = 0.0
        self.lag_seconds = 0.0
        self.overran = False
        self.overrun_ratio = 0.0

    # -- the tick ------------------------------------------------------------

    def begin(self, interval_seconds: int):
        """Mark the start of a tick and read its lateness. Returns the lateness.

        `interval_seconds` is the cadence this tick is measured against — the
        same number the main loop sleeps and the volume law divides by, so
        "overran" means what an operator thinks it means. The DEADLINE this tick
        is late against is `interval + cost_ewma`, set by the previous tick's
        `end`; see the module docstring for why it is not `interval` alone.
        """
        if interval_seconds is None or interval_seconds <= 0:
            raise ValueError(
                f"interval_seconds must be a positive number, got {interval_seconds!r}. "
                "Refusing to anchor a schedule with no interval: every subsequent "
                "lateness would be meaningless rather than merely wrong."
            )
        now = self._clock()
        self._interval_seconds = int(interval_seconds)

        if self._due_at is None:
            # First tick after a reset anchors the schedule. It cannot be late
            # against a schedule that did not exist yet.
            self._due_at = now
            self.lateness_seconds = 0.0
        else:
            # The deadline is the interval PLUS what this generator's own ticks
            # have been costing — not the interval alone. The sleep in the loop
            # is unconditional, so the honest period is `interval + cost`, and a
            # schedule that ignored `cost` would report a healthy box as
            # minutes-to-hours behind by the end of every day.
            #
            # Signed and NOT floored: the deadline is a schedule, so a tick that
            # runs fast pushes lateness below zero and repays earlier debt.
            self.lateness_seconds = now - self._due_at

        self.lag_seconds = max(0.0, self.lateness_seconds)
        self._started_at = now
        self.overran = False
        self.overrun_ratio = 0.0
        self.ticks += 1
        return self.lateness_seconds

    def end(self) -> int:
        """Mark the end of the tick; return its duration in whole milliseconds.

        Raises if `begin` was not called: `wall_clock_ms` is written to the
        ledger from this value, and a silent 0 there is a decorative number that
        reads like a measurement — the exact failure mode of a metric that was
        never really measured.
        """
        if self._started_at is None:
            raise RuntimeError(
                "TickCadence.end() without a matching begin() — the tick was never "
                "timed, so its duration would be reported as 0ms."
            )
        self.duration_ms = int(round((self._clock() - self._started_at) * 1000))
        # Overrun is against the INTERVAL, not the deadline: it answers "did this
        # one tick cost more than the cadence allows", which is a fact about the
        # tick rather than about this generator's recent average.
        self.overran = self.duration_ms > self._interval_seconds * 1000
        self.overrun_ratio = self.duration_ms / float(self._interval_seconds * 1000)

        cost = self.duration_ms / 1000.0
        if self._cost_ewma is None:
            self._cost_ewma = cost
        else:
            self._cost_ewma += COST_EWMA_ALPHA * (cost - self._cost_ewma)

        # The next tick is due this far on, and the EWMA that goes into that is
        # the one including THIS tick's cost — the deadline should describe the
        # tick that is coming, not the one that ran.
        self._due_at = self._started_at + self._interval_seconds + self._cost_ewma
        return self.duration_ms

    # -- reporting -----------------------------------------------------------

    def prefix(self, sim_dt) -> str:
        """`[tick N][YYYY-MM-DD HH:MM:SS]` — sim_dt, never wall clock.

        The simulated stamp is the one that identifies the tick to whoever is
        reading the log: in realtime it equals `now()`, but on a backfill hour it
        is the historical hour being written, and a prefix carrying the wall
        clock would make a backfilled hour indistinguishable from a live one.
        """
        return f"[tick {self.ticks}][{sim_dt:%Y-%m-%d %H:%M:%S}]"

    def lagging(self, threshold_seconds: float) -> bool:
        """True when the schedule debt is at or past `threshold_seconds`.

        A threshold of 0 (or a negative one) means "alert on any lag at all",
        which is a legitimate configuration and not a mistake worth guarding.
        """
        return self.lag_seconds >= threshold_seconds

    def snapshot(self) -> dict:
        """The cadence's state, for a debug log or a test assertion."""
        return {
            "ticks": self.ticks,
            "interval_seconds": self._interval_seconds,
            "lateness_seconds": self.lateness_seconds,
            "lag_seconds": self.lag_seconds,
            "duration_ms": self.duration_ms,
            "overran": self.overran,
            "overrun_ratio": self.overrun_ratio,
            "cost_ewma_seconds": self._cost_ewma,
        }


# The default the operator gets if `observability.tick_lag_alert_seconds` is
# absent from config.yaml. Two minutes is four default-cadence ticks, and — since
# the deadline already absorbs this box's own tick cost — a lag of that size is a
# genuine deviation from the generator's established pace rather than a
# restatement of it. The dataclass default in config.py is the same number and
# the shipped config.yaml sets it explicitly, so this only covers a config with
# the whole observability block missing.
DEFAULT_ALERT_LAG_SECONDS = 120.0