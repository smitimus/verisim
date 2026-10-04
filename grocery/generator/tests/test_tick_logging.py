"""
The alert threshold is a real, configurable knob (t_196d8da2).

`main.alert_threshold` is the one place the generator decides whether a lag
matters, so it gets its own tests rather than riding along in the cadence suite:
these are about config resolution and log behaviour, which the fake-clock tests
deliberately stub out.

What is pinned here:

* the threshold is read from `observability.tick_lag_alert_seconds` in
  config.yaml, and hot-reload reaches it;
* a config with NO `observability:` block still works — every install generated
  before this card is in that state, and a boot that raised would be a
  regression discovered by an upgrade rather than by a test;
* the shipped config.yaml and the dataclass default agree, so the documented
  number is the effective one;
* `_log_tick` emits the `[tick N][sim_dt]` prefix, raises the level to WARNING
  when the threshold is crossed, and distinguishes a per-tick overrun.
"""
import logging

import pytest
import yaml

from grocery.generator import config as gc
from grocery.generator import main as gm
from grocery.generator.observability import TickCadence


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = float(start)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += float(seconds)


def _cfg(**observability):
    cfg = gc.Config()
    if observability:
        for key, value in observability.items():
            setattr(cfg.observability, key, value)
    return cfg


def _sim_dt():
    import datetime
    return datetime.datetime(2026, 10, 4, 14, 30, 0)


COUNTS = dict(pos=42, online=7, tc=3, orders=2, stockouts=1)


# ---------------------------------------------------------------------------
# threshold resolution
# ---------------------------------------------------------------------------

def test_the_threshold_comes_from_config():
    assert gm.alert_threshold(_cfg(tick_lag_alert_seconds=45)) == 45.0


def test_a_config_without_the_block_falls_back_to_the_documented_default():
    """Every install generated before this card has no `observability:` block.

    A boot that raised here would be a regression an operator discovers on
    upgrade, not a test failure.
    """
    cfg = gc.Config()
    assert gm.alert_threshold(cfg) == 120.0

    # And the degraded shapes: a block present but empty, or the attribute gone.
    bare = gc.Config()
    bare.observability = None
    assert gm.alert_threshold(bare) == 120.0

    class _NoAttr:
        pass

    assert gm.alert_threshold(_NoAttr()) == 120.0


def test_a_nonsense_threshold_falls_back_rather_than_silently_disabling_the_alert():
    """`float("soon")` must not become 0.0 — that would alert on every tick.

    Falling back to the documented default keeps a typo loud but harmless;
    coercing it to 0 would turn a typo into an alert that fires continuously,
    which is the kind of thing that gets switched off and never fixed.
    """
    for bad in ('soon', None, [1]):
        cfg = _cfg()
        cfg.observability.tick_lag_alert_seconds = bad
        assert gm.alert_threshold(cfg) == 120.0


def test_a_negative_threshold_is_refused_in_favour_of_the_default():
    """A negative threshold is `lag >= negative`, i.e. always true — never what
    an operator meant, and it would fire on every single tick."""
    assert gm.alert_threshold(_cfg(tick_lag_alert_seconds=-5)) == 120.0


def test_a_zero_threshold_is_honoured_as_deliberate():
    """`tick_lag_alert_seconds: 0` means "tell me about any lag" and is a
    legitimate configuration — unlike a negative one, it is not a superset that
    trivially always matches."""
    assert gm.alert_threshold(_cfg(tick_lag_alert_seconds=0)) == 0.0


def test_the_threshold_reloads_from_config_yaml(tmp_path, monkeypatch):
    """config.yaml is re-read every tick, so the threshold must be too."""
    path = tmp_path / 'config.yaml'
    path.write_text('observability:\n  tick_lag_alert_seconds: 7.5\n')
    monkeypatch.setenv('CONF_PATH', str(path))

    cfg = gc.load_config()
    assert cfg.observability.tick_lag_alert_seconds == 7.5
    assert gm.alert_threshold(cfg) == 7.5

    # ...and a hot reload picks up a change without a restart.
    path.write_text('observability:\n  tick_lag_alert_seconds: 300\n')
    cfg = gc.reload_config(cfg)
    assert gm.alert_threshold(cfg) == 300.0


def test_the_shipped_config_and_the_dataclass_default_agree():
    """The number in the docs, the dataclass and config.yaml must be one number.

    A generator whose effective threshold depends on which of the three an
    operator happened to read is a generator nobody can tune.
    """
    for path in ('config.yaml', 'standalone/config.yaml'):
        full = gm.os.path.join(
            gm.os.path.dirname(gm.__file__), '..', path)
        full = gm.os.path.normpath(full)
        if not gm.os.path.exists(full):
            pytest.skip(f'{full} not found')
        with open(full) as f:
            data = yaml.safe_load(f)
        shipped = data.get('observability', {}).get('tick_lag_alert_seconds')
        assert shipped == gc.ObservabilityConfig().tick_lag_alert_seconds, (
            f'{path} sets {shipped} but the dataclass default is '
            f'{gc.ObservabilityConfig().tick_lag_alert_seconds}')


# ---------------------------------------------------------------------------
# the log line
# ---------------------------------------------------------------------------

def _log(caplog, level=logging.INFO, lag_seconds=0.0, overran=False,
         interval=30, duration_ms=500):
    """Drive `_log_tick` with a cadence stubbed to a known state."""
    cadence = TickCadence(clock=FakeClock())
    cadence.ticks = 7
    cadence.lag_seconds = lag_seconds
    cadence.lateness_seconds = lag_seconds
    cadence.overran = overran
    cadence.duration_ms = duration_ms
    cadence._interval_seconds = interval

    with caplog.at_level(level, logger='grocery-generator'):
        gm._log_tick(_cfg(), cadence, _sim_dt(), duration_ms,
                     counts=COUNTS, scenario_tag='weekend')
    return caplog.records


def test_the_log_line_carries_the_tick_prefix_and_the_simulated_stamp(caplog):
    records = _log(caplog)
    info = [r for r in records if r.levelno == logging.INFO]
    assert info, "no INFO line was emitted for a healthy tick"
    assert info[0].getMessage().startswith("[tick 7][2026-10-04 14:30:00]")


def test_a_healthy_tick_stays_at_info_and_does_not_warn(caplog):
    records = _log(caplog, lag_seconds=5.0)
    assert not [r for r in records if r.levelno >= logging.WARNING], (
        "a tick well inside the threshold raised a warning: "
        f"{[r.getMessage() for r in records]}")


def test_a_lagging_tick_is_reported_at_warning_with_the_lag_spelled_out(caplog):
    records = _log(caplog, lag_seconds=300.0)

    warn = [r for r in records if r.levelno == logging.WARNING]
    assert warn, "a 300s lag did not produce a warning"
    message = warn[0].getMessage()
    assert "BEHIND REALTIME" in message
    assert "300.0s" in message                     # the actual lag
    assert "120.0s" in message                     # and the threshold it crossed
    assert message.startswith("[tick 7][2026-10-04 14:30:00]")


def test_the_warning_threshold_is_the_configured_one(caplog):
    """A lag over a RAISED threshold must warn; one under it must not.

    Without this the threshold could be decorative — read, logged in the message,
    and never actually compared.
    """
    cfg = _cfg(tick_lag_alert_seconds=1.0)
    cadence = TickCadence(clock=FakeClock())
    cadence.lag_seconds = 30.0

    with caplog.at_level(logging.INFO, logger='grocery-generator'):
        gm._log_tick(cfg, cadence, _sim_dt(), 500, counts=COUNTS, scenario_tag='normal')
    assert [r for r in caplog.records if r.levelno == logging.WARNING]

    caplog.clear()
    cadence.lag_seconds = 0.5
    with caplog.at_level(logging.INFO, logger='grocery-generator'):
        gm._log_tick(cfg, cadence, _sim_dt(), 500, counts=COUNTS, scenario_tag='normal')
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


def test_a_single_overrun_tick_is_its_own_diagnosis(caplog):
    """An overrun and a lag are different problems and must read differently.

    "this tick cost 4x its interval" is one blip; "the generator is 300s behind"
    is a trend. Collapsing them loses the distinction that tells an operator
    whether to look at the last tick or at the box.
    """
    records = _log(caplog, lag_seconds=0.0, overran=True, duration_ms=120_000)

    warn = [r for r in records if r.levelno == logging.WARNING]
    assert warn, "a 4x overrun did not warn"
    assert any("OVERRAN" in r.getMessage() for r in warn)
    # An overrun alone must not claim the generator is behind: the deadline has
    # absorbed it, and saying otherwise is a false alarm on a healthy box.
    assert not any("BEHIND REALTIME" in r.getMessage() for r in warn)


def test_a_lagging_and_overrunning_tick_reports_both(caplog):
    records = _log(caplog, lag_seconds=300.0, overran=True, duration_ms=120_000)
    messages = [r.getMessage() for r in records if r.levelno == logging.WARNING]
    assert any("BEHIND REALTIME" in m for m in messages)
    assert any("OVERRAN" in m for m in messages)


def test_every_tick_emits_exactly_one_info_line(caplog):
    """One line per tick is the point of the prefix — a tick you can grep.

    Two INFO lines would make "[tick N]" ambiguous, and an operator grepping for
    one tick would have to work out which of the lines they were meant to read.
    """
    records = _log(caplog, lag_seconds=300.0, overran=True)
    info = [r for r in records if r.levelno == logging.INFO]
    assert len(info) == 1


def test_the_info_line_carries_the_counts_the_operator_already_relied_on(caplog):
    """The old log line had counts in it; the new one must still have them.

    The card adds a prefix and a lag, it does not replace the payload — anything
    that grepped for "Stockouts:" or "Scenario:" keeps working.
    """
    records = _log(caplog)
    message = records[0].getMessage()
    assert "POS: 42" in message
    assert "Online: 7" in message
    assert "TC: 3" in message
    assert "Orders: 2" in message
    assert "Stockouts: 1" in message
    assert "Scenario: weekend" in message
    assert "500ms" in message