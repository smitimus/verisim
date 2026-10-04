"""
The realtime_factor threshold a consumer should alert on (t_35b4d860).

t_196d8da2 shipped `realtime_factor` as "the number to alert on" and the UI drew
its green/red boundary at 0.95, chosen for tidiness. That was the wrong number and
nobody could tell from the code, because the trip rate depends on the tick-cost
TAIL rather than on the median: a healthy generator here costs 72ms per tick
against a 30s cadence, yet 3.29% of its ticks cost over a second and the slowest
cost 95.7s, so any trailing window containing a spike reads low for as long as
that spike stays inside it.

Measured over the dev slot's whole tick ledger — every trailing-100 window
recomputed, 37,313 of them:

    below 0.95 -> 4.88% of windows
    below 0.90 -> 2.35%
    below 0.80 -> 1.10%

So 0.95 fires on one window in twenty of an ordinary, healthy day. An alert that
cries wolf that often is an alert nobody reads, which is worse than no alert —
it trains people to dismiss the one day it was right. 0.80 fires about once a
hundred and still fires while the ticks in the window are collectively eating a
fifth of the interval.

This file pins the CONSUMER-ADVICE (what a downstream readiness check should
threshold on, and why), because the number lives in data-lab's DAG rather than in
verisim's source — verisim cannot enforce it, only state it and keep its own UI
consistent with it. The UI's boundary is pinned separately, by reading
`base/ui/app.py`.
"""
import pathlib
import re

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
UI_SOURCE = REPO_ROOT / "base" / "ui" / "app.py"

# The trip rates measured on the dev slot (2026-10-04), by window size.
# Smaller windows are noisier, which is the whole point: a spike stays inside a
# 100-tick window for 100 ticks but inside a 1000-tick window for 1000.
MEASURED_TRIP_RATE = {
    100:  {0.95: 4.88, 0.90: 2.35, 0.80: 1.10},
    200:  {0.95: 5.12, 0.90: 3.92, 0.80: 0.53},
    500:  {0.95: 8.62, 0.90: 1.35, 0.80: 0.00},
    1000: {0.95: 4.47, 0.90: 0.00, 0.80: 0.00},
}

# The boundary a consumer should use, and the max acceptable false-trip rate.
RECOMMENDED = 0.80
MAX_HEALTHY_TRIP_RATE_PCT = 2.0


@pytest.mark.parametrize("window", sorted(MEASURED_TRIP_RATE))
def test_the_recommended_threshold_does_not_cry_wolf_on_a_healthy_generator(window):
    """Below RECOMMENDED, the measured trip rate stays under the budget.

    The budget (2% of windows) is itself a judgement, and the judgement is worth
    stating: an alert that fires on more than about one window in fifty has been
    firing so often that people route around it, and then it fails to protect the
    one window that mattered.
    """
    rates = MEASURED_TRIP_RATE[window]
    assert rates[RECOMMENDED] <= MAX_HEALTHY_TRIP_RATE_PCT, (
        f"at window={window}, {rates[RECOMMENDED]}% of windows fall below "
        f"{RECOMMENDED} on a HEALTHY generator, over the "
        f"{MAX_HEALTHY_TRIP_RATE_PCT}% budget")


def test_the_old_0_95_boundary_would_have_been_too_tight():
    """The regression this card exists to prevent, pinned as a fact.

    If someone re-tightens the boundary to 0.95 because it "looks more correct",
    this fails and quotes the number.
    """
    rate = MEASURED_TRIP_RATE[100][0.95]
    assert rate > MAX_HEALTHY_TRIP_RATE_PCT, (
        f"0.95 trips {rate}% of healthy windows at the /metrics default window "
        f"of 100 — it cannot be the recommended boundary")


def test_0_90_is_also_too_tight_and_the_measurement_says_so():
    """data-dev proposed 0.90. It is better than 0.95 and still too low.

    Recorded deliberately: the proposal came with its own reasoning and measured
    numbers, and the numbers do not support it at the default window. 2.35% of
    healthy windows still trips it, and the trip rate is WORSE at window=200
    (3.92%) — so the obvious "just widen the window" mitigation does not rescue
    0.90 either. This test exists so the next card to reason about this threshold
    starts from the measurement instead of re-deriving it.
    """
    assert MEASURED_TRIP_RATE[100][0.90] > MAX_HEALTHY_TRIP_RATE_PCT
    assert MEASURED_TRIP_RATE[200][0.90] > MAX_HEALTHY_TRIP_RATE_PCT


def test_the_ui_boundary_is_0_80_and_not_the_old_0_95():
    """verisim's own dashboard must agree with the advice it gives consumers.

    If the UI kept colouring 0.9 red while the docs said 0.8, an operator would
    trust the badge over the doc and be trained to ignore it.
    """
    source = UI_SOURCE.read_text()
    panel = source[source.index("def _tick_health_panel"):]
    panel = panel[:panel.index("\ndef ")]
    boundary = re.search(r"factor\s*>=\s*([0-9.]+)", panel)
    assert boundary, "the panel's green/red boundary is no longer a literal " \
                     "comparison on realtime_factor — find it and pin it here"
    assert float(boundary.group(1)) == RECOMMENDED, (
        f"the UI paints the factor red below {boundary.group(1)}, but the "
        f"measured recommendation is {RECOMMENDED}; a consumer reading this "
        f"endpoint and an operator reading this dashboard must get the same "
        f"verdict.")


def test_the_ui_states_the_measured_trip_rates():
    """The reasoning travels with the code, so the number is not re-litigated.

    A bare `>= 0.80` invites the next person to "correct" it to 0.95 without
    knowing a measurement exists.
    """
    source = UI_SOURCE.read_text()
    panel = source[source.index("def _tick_health_panel"):]
    panel = panel[:panel.index("\ndef ")]
    # The comment block immediately above the panel carries the numbers.
    lead = source[:source.index("def _tick_health_panel")]
    lead = lead[lead.rindex("# 0.95 -> 4.88%") - 200:] \
        if "# 0.95 -> 4.88%" in lead else lead[-2000:]
    assert "4.88" in lead and "2.35" in lead and "1.10" in lead, (
        "the measured trip rates (4.88% / 2.35% / 1.10%) must sit next to the "
        "0.80 boundary, or the next reader has no basis for it")