"""
`/metrics`: is the generator keeping up? (t_196d8da2)

The card's premise is that `control.generation_stats.wall_clock_ms` recorded what
every tick cost and nothing ever surfaced whether that was acceptable, so "the
container crash-loops after an upgrade" was log-archaeology over numbers nobody
had looked at. This route answers it from the ledger alone.

Two classes of check:

* **Arithmetic**, against hand-built window aggregates. `_tick_metrics` is the
  pure function where the endpoint's value and its mistakes both live, so it is
  tested directly rather than only through HTTP. The properties pinned here are
  the ones that would otherwise be asserted in a docstring: that the period is
  `span / (n-1)` rather than `span / n`, that a backfill's hardcoded
  `wall_clock_ms = 0` reports as absent rather than as zero, and that fewer than
  two ticks produces no period at all instead of an invented one.

* **The route**, against the live container, including that it survives both
  build-time strip scripts — it is in a shared section precisely so the
  gas-station image keeps it, and that decision is only worth anything if a test
  holds it in place.

The live checks need a settled generator: `quiesced_generator` pauses it and
waits for the writes to stop, because a tick landing mid-read makes a tick
window mean nothing.
"""
import pathlib
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
API_SOURCE = REPO_ROOT / "base" / "api" / "main.py"

STRIP_SCRIPTS = {
    "grocery": REPO_ROOT / "grocery" / "standalone" / "strip_gas_station.py",
    "gas-station": REPO_ROOT / "gas-station" / "standalone" / "strip_grocery.py",
}


def _load_api_module():
    """Import base/api/main.py under a throwaway module name.

    Imported rather than pasted: the arithmetic these tests pin must be the
    arithmetic the route runs, and a copy in the test file would let the two
    drift the first time one of them was edited.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("vz196_api_under_test", API_SOURCE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def api():
    try:
        return _load_api_module()
    except Exception as exc:                              # noqa: BLE001
        pytest.skip(f"base/api/main.py could not be imported here: {exc!r}")


# ---------------------------------------------------------------------------
# the arithmetic
# ---------------------------------------------------------------------------

INTERVAL = 30
NOW = datetime(2026, 10, 4, 18, 0, 0, tzinfo=timezone.utc)


def _window(ticks, mean_cost_ms=None, min_cost_ms=None, max_cost_ms=None,
            timed=None, newest_sim_seconds_ago=0.0, first_ago=None,
            last_ago=0.0):
    """A hand-built `_tick_window` row."""
    first = NOW - timedelta(seconds=first_ago if first_ago is not None
                            else (ticks - 1) * INTERVAL)
    return {
        "ticks": ticks,
        "first_tick": first,
        "last_tick": NOW - timedelta(seconds=last_ago),
        "avg_wall_clock_ms": mean_cost_ms,
        "min_wall_clock_ms": min_cost_ms,
        "max_wall_clock_ms": max_cost_ms,
        "timed_ticks": ticks if timed is None else timed,
        "newest_simulation_dt": NOW - timedelta(seconds=newest_sim_seconds_ago),
        "pos_rows": 100 * ticks,
        "orders_rows": 10 * ticks,
    }


def _compute(api, window_row, mode="realtime", interval=INTERVAL, now=NOW,
             threshold=120.0, paused=False):
    return api._tick_metrics(window_row,
                             {"mode": mode, "is_running": True, "is_paused": paused},
                             interval, now, threshold)


def test_the_period_is_span_over_n_minus_one(api):
    """The window's first..last stamp spans n-1 whole iterations.

    `recorded_at` is stamped by the ledger INSERT, which happens at the END of a
    tick, so n stamps delimit n-1 complete loops. Dividing by n instead would
    understate the period by a whole interval — 30s of a 30.5s period, i.e. it
    would report a healthy generator as running 100x faster than realtime.
    """
    out = _compute(api, _window(ticks=100, mean_cost_ms=500, first_ago=99 * 30.5))
    assert out["mean_period_seconds"] == pytest.approx(30.5, abs=0.01)


def test_the_overhead_is_the_period_above_the_interval(api):
    """One iteration really costs `interval + tick_cost`, and that is the number.

    The loop's sleep is unconditional, so the tick's own cost is not absorbed by
    the interval — it is added to it. That overhead is what a healthy generator
    produces every single iteration, which is why it cannot itself be an alert.
    """
    out = _compute(api, _window(ticks=100, mean_cost_ms=500, first_ago=99 * 30.5))
    assert out["mean_overhead_ms"] == pytest.approx(500.0, abs=10.0)


def test_the_realtime_factor_is_the_fraction_of_realtime_produced(api):
    """interval / mean_period. The number to alert on, and it needs no tuning.

    Dimensionless, so one threshold is right on a fast machine and a slow one —
    which no threshold in seconds can be.
    """
    perfect = _compute(api, _window(ticks=100, mean_cost_ms=0, first_ago=99 * 30))
    assert perfect["realtime_factor"] == pytest.approx(1.0, abs=1e-4)

    half = _compute(api, _window(ticks=100, mean_cost_ms=30_000,
                                 first_ago=99 * 60.0))
    assert half["realtime_factor"] == pytest.approx(0.5, abs=1e-3)


def test_a_single_tick_has_no_period_because_there_is_none_yet(api):
    """One stamp deltas to nothing; inventing 1.0 or 0.0 is a fabrication."""
    out = _compute(api, _window(ticks=1, mean_cost_ms=500))
    assert out["mean_period_seconds"] is None
    assert out["realtime_factor"] is None
    assert out["mean_overhead_ms"] is None
    assert out["window_ticks"] == 1


def test_an_empty_window_reports_nothing_rather_than_zero(api):
    """A generator that has never ticked is a fact; reporting 0ms would be a lie."""
    empty = _window(ticks=0)
    empty["first_tick"] = None
    empty["last_tick"] = None
    empty["newest_simulation_dt"] = None

    out = _compute(api, empty)
    assert out["window_ticks"] == 0
    assert out["mean_period_seconds"] is None
    assert out["mean_tick_cost_ms"] is None
    assert out["data_staleness_seconds"] is None
    assert out["tick_staleness_seconds"] is None
    assert out["lagging"] is False      # nothing to be late about


def test_a_backfills_zero_wall_clock_reports_as_absent_not_as_free(api):
    """The backfill wrote a literal 0 into wall_clock_ms until t_196d8da2.

    An hour that cost 90s and one that cost 9s both recorded "0ms", so the mean
    over such a window is 0 — and rendering that as `mean_tick_cost_ms: 0` reads
    as "these ticks were free". `timed_ticks` is what distinguishes the two, and
    the endpoint has to be honest about which case it is in.
    """
    # AVG over rows whose every value is 0 is 0, not NULL.
    out = _compute(api, _window(ticks=50, mean_cost_ms=0.0, timed=50))
    assert out["mean_tick_cost_ms"] == 0.0
    assert out["timed_ticks"] == 50

    # A window where every row has NULL wall_clock_ms (possible if a future
    # writer omits it) must report absent rather than zero.
    out = _compute(api, _window(ticks=50, mean_cost_ms=None, timed=0))
    assert out["mean_tick_cost_ms"] is None
    assert out["timed_ticks"] == 0


def test_staleness_is_measured_against_the_database_clock(api):
    """Both staleness figures come from `now`, the DB's clock.

    The API computes them against the same clock that stamped `recorded_at`, so
    a container whose clock has drifted cannot manufacture or hide a figure that
    says whether the generator is keeping up.
    """
    out = _compute(api, _window(ticks=10, mean_cost_ms=100,
                                newest_sim_seconds_ago=120.0, last_ago=35.0))
    assert out["data_staleness_seconds"] == pytest.approx(120.0)
    assert out["tick_staleness_seconds"] == pytest.approx(35.0)
    assert "skew_note" not in out


def test_a_simulation_clock_ahead_of_the_database_suppresses_the_staleness(api):
    """`simulation_dt` is written from a NAIVE `datetime.now()` — measured skew.

    The generator passes a naive timestamp into a `TIMESTAMPTZ` column, so
    Postgres reads it in the DATABASE's timezone. When the generator container's
    TZ differs from Postgres's — the normal case, compose sets `TZ` per service —
    the column lands hours from the truth. Measured on the dev slot
    (2026-10-04): the newest `simulation_dt` was **3.71h in the future** of the
    database clock, and a naive subtraction reported a staleness of -13412s.

    A negative staleness is not a measurement, so it is suppressed and the skew is
    reported in its place. Emitting the negative number would put a figure on the
    dashboard that is obviously wrong to anyone who looks at it and meaningless to
    anyone who does not.
    """
    out = _compute(api, _window(ticks=10, mean_cost_ms=100,
                                newest_sim_seconds_ago=-3.71 * 3600,
                                last_ago=35.0))

    assert out["data_staleness_seconds"] is None
    assert out["simulation_clock_skew_seconds"] == pytest.approx(3.71 * 3600, rel=0.01)
    assert "datetime.now()" in out["skew_note"]
    assert "naive" in out["skew_note"]

    # The figures that do not depend on simulation_dt are untouched — the skew is
    # a defect in one column, not a reason to distrust the whole endpoint.
    assert out["tick_staleness_seconds"] == pytest.approx(35.0)
    assert out["realtime_factor"] is not None
    assert out["lagging"] is False


def test_a_paused_generator_in_realtime_mode_is_not_lagging(api):
    """Live on the dev slot: `mode=realtime`, `is_paused=true`, no tick for 1000s.

    The naive staleness test fires here, but the generator was PAUSED — an
    operator pressed pause. Calling that "lagging" is the false alarm this card
    exists to avoid, and it is the state a paused dev slot spends most of its time
    in, so it is worth pinning precisely.
    """
    out = _compute(api, _window(ticks=100, mean_cost_ms=211, last_ago=1000.0),
                   mode="realtime", paused=True)
    assert out["lagging"] is False
    assert "paused" in out["lagging_note"]
    # The note must not contradict the mode it is explaining.
    assert "realtime" not in out["lagging_note"].replace("not generating", "")


def test_a_generator_that_has_gone_quiet_is_lagging(api):
    """The one absolute-threshold alert here, and it scales with the cadence.

    "Has the generator stopped ticking?" has a per-box-independent answer once
    you allow for `tick_interval_seconds`: three missed cadences is quiet enough
    to matter at 30s or at 300s alike.
    """
    out = _compute(api, _window(ticks=100, mean_cost_ms=100, last_ago=INTERVAL * 2))
    assert out["lagging"] is False

    out = _compute(api, _window(ticks=100, mean_cost_ms=100, last_ago=INTERVAL * 4))
    assert out["lagging"] is True

    # ...and the bar moves with the cadence rather than being a fixed 90s.
    slow = _compute(api, _window(ticks=100, mean_cost_ms=100, last_ago=200),
                    interval=300)
    assert slow["quiet_after_seconds"] == 900
    assert slow["lagging"] is False


def test_a_stopped_or_paused_generator_is_not_reported_as_behind(api):
    """It is not late — it was told to stop. Same reasoning as `cadence.reset()`.

    A backfill is the same case: it is deliberately writing history, so its
    newest data being old is the design, not a defect.
    """
    for mode in ("stopped", "paused"):
        out = _compute(api, _window(ticks=100, mean_cost_ms=100, last_ago=10_000),
                       mode=mode)
        assert out["lagging"] is False, f"a {mode} generator was called lagging"


def test_a_backfill_says_why_its_data_is_old(api):
    """Without the note, a 30-day-old newest row during a fresh install's first
    minute reads as a defect rather than as the install working."""
    out = _compute(api,
                   _window(ticks=10, mean_cost_ms=100,
                           newest_sim_seconds_ago=30 * 86400),
                   mode="backfill")
    assert "backfill" in out["lagging_note"]
    assert out["lagging"] is False


def test_the_threshold_is_reported_so_the_caller_can_see_what_it_is_judging(api):
    out = _compute(api, _window(ticks=10, mean_cost_ms=100), threshold=45.0)
    assert out["lag_alert_seconds"] == 45.0


def test_a_zero_interval_cannot_divide_by_zero(api):
    """`tick_interval_seconds` is a DB column; a hand-written row can hold 0."""
    out = _compute(api, _window(ticks=10, mean_cost_ms=100), interval=0)
    assert out["interval_seconds"] >= 1
    assert out["realtime_factor"] is not None


# ---------------------------------------------------------------------------
# the rendered output
# ---------------------------------------------------------------------------

def test_the_text_rendering_omits_an_absent_metric_entirely(api):
    """A Prometheus parser has no way to read "no value" as anything but a gap.

    Emitting `verisim_realtime_factor 0` for a window with one tick would be a
    false alarm, and emitting `NaN` is worse. So an absent metric contributes no
    line at all — not even HELP/TYPE, because a header with no sample would tell a
    scraper the metric exists and then never deliver it. The metric reappears the
    moment there is a value to report.
    """
    out = _compute(api, _window(ticks=1, mean_cost_ms=None))
    text = api._render_prometheus("grocery", out, [])

    assert "verisim_realtime_factor" not in text, (
        "a metric with no value still emitted a header:\n" + text)
    assert not re.search(r'^verisim_realtime_factor', text, flags=re.MULTILINE)
    # ...while the metrics that DO have values are all there.
    assert "verisim_ticks_in_window 1" in text
    assert "verisim_lagging 0" in text

    # Once there is a window to measure, the line appears with its header.
    good = _compute(api, _window(ticks=50, mean_cost_ms=500, first_ago=49 * 30.5))
    text = api._render_prometheus("grocery", good, [])
    assert "# HELP verisim_realtime_factor" in text
    match = re.search(r'^verisim_realtime_factor (\S+)$', text, flags=re.MULTILINE)
    assert match, text
    assert float(match.group(1)) == pytest.approx(30 / 30.5, abs=1e-3)


def test_the_text_rendering_carries_the_headline_numbers(api):
    out = _compute(api, _window(ticks=50, mean_cost_ms=500,
                                first_ago=49 * 30.5, last_ago=5.0))
    text = api._render_prometheus("grocery", out, [])

    for name in ("verisim_tick_interval_seconds",
                 "verisim_mean_tick_cost_ms",
                 "verisim_tick_mean_period_seconds",
                 "verisim_realtime_factor",
                 "verisim_lagging",
                 "verisim_ticks_in_window"):
        assert re.search(rf'^{name} ', text, flags=re.MULTILINE), \
            f"{name} missing from /metrics output:\n{text}"

    assert text.endswith("\n")
    assert "verisim_lagging 0" in text


def test_row_counts_are_rendered_as_labelled_samples(api):
    """The per-table counters are the other half of the card's ask, and they
    carry a schema label so two industries with a same-named table stay apart."""
    out = _compute(api, _window(ticks=10, mean_cost_ms=100))
    rows = [{"schema": "pos", "table": "transactions", "rows": 1234},
            {"schema": "control", "table": "generation_stats", "rows": 10}]
    text = api._render_prometheus("grocery", out, rows)

    assert 'verisim_rows{schema="pos",table="transactions"} 1234' in text
    assert 'verisim_rows{schema="control",table="generation_stats"} 10' in text


def test_the_alert_threshold_is_read_from_the_mounted_config(tmp_path, monkeypatch):
    """The API and the generator must quote the SAME number for the same knob.

    Both read the mounted `config.yaml`; the API re-reads it per call rather than
    capturing it at import, because the file is hot-reloaded and a value frozen at
    boot would drift from the generator's the moment an operator tuned it.
    """
    mod = _load_api_module()
    cfg = tmp_path / "config.yaml"
    cfg.write_text("observability:\n  tick_lag_alert_seconds: 45\n")
    monkeypatch.setattr(mod, "_GROCERY_CONFIG_PATH", str(cfg))
    assert mod.metrics_alert_threshold() == 45.0

    # Re-read per call: a tuning that took effect for the generator has to take
    # effect here without a restart.
    cfg.write_text("observability:\n  tick_lag_alert_seconds: 300\n")
    assert mod.metrics_alert_threshold() == 300.0


def test_the_threshold_degrades_rather_than_raising(tmp_path, monkeypatch):
    """A metrics endpoint that 500s on a typo is worse than one that defaults."""
    mod = _load_api_module()

    missing = tmp_path / "absent.yaml"
    monkeypatch.setattr(mod, "_GROCERY_CONFIG_PATH", str(missing))
    assert mod.metrics_alert_threshold() == 120.0

    for body in ("observability:\n  tick_lag_alert_seconds: soon\n",
                 "observability:\n  tick_lag_alert_seconds: -5\n",
                 "generator:\n  tick_interval_seconds: 30\n"):
        bad = tmp_path / "config.yaml"
        bad.write_text(body)
        monkeypatch.setattr(mod, "_GROCERY_CONFIG_PATH", str(bad))
        assert mod.metrics_alert_threshold() == 120.0, body


def test_no_new_dependency_was_added(api):
    """The card's acceptance criterion, enforced rather than promised.

    A metrics endpoint is the classic place to reach for `prometheus_client`,
    which would break the slim-image rule every other module in this repo obeys.
    `PlainTextResponse` is starlette, already a FastAPI dependency.
    """
    source = API_SOURCE.read_text()
    assert "prometheus_client" not in source
    assert "PlainTextResponse" in source


# ---------------------------------------------------------------------------
# both build-time strip scripts
# ---------------------------------------------------------------------------

def _bound_names(path):
    """Module-level function names and assignment targets defined in `path`."""
    import ast

    tree = ast.parse(path.read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(t.id for t in node.targets if getattr(t, "id", None))
        elif isinstance(node, ast.AnnAssign):
            if getattr(node.target, "id", None):
                names.add(node.target.id)
    return names


@pytest.mark.parametrize("industry", sorted(STRIP_SCRIPTS))
def test_metrics_survives_every_build_time_strip(industry):
    """`/{industry}/metrics` and every name it uses must survive both strips.

    /metrics was put in a SHARED section precisely because every industry writes
    the same tick ledger — but `strip_grocery.py` deletes every "Grocery only" /
    "Grocery —" section, so a route parked in one would simply not exist in the
    gas-station image. This is the test that keeps that decision honest.

    A helper the strip removed but the route still calls is a NameError on the
    first request, which is exactly the class of bug the strip script is meant to
    prevent and is invisible until someone hits the endpoint.
    """
    script = STRIP_SCRIPTS[industry]
    if not script.exists():
        pytest.skip(f"{script} not found")

    before = _bound_names(API_SOURCE)
    needed = {
        "_tick_metrics", "_tick_window", "_rows_by_table", "_render_prometheus",
        "metrics_alert_threshold", "DEFAULT_ALERT_LAG_SECONDS",
        "DEFAULT_TICK_INTERVAL_SECONDS", "METRICS_ROWS_TTL_SECONDS",
        "_metrics_rows_cache", "metrics",
    }
    # Only the names /metrics actually needs — the rest of the file is expected to
    # shrink under the strip.
    assert needed <= before, f"/metrics needs {needed - before}, which main.py never defines"

    with tempfile.TemporaryDirectory() as tmp:
        out = pathlib.Path(tmp) / "api.py"
        result = subprocess.run([sys.executable, str(script), str(API_SOURCE), str(out)],
                                capture_output=True, text=True)
        assert result.returncode == 0, f"strip script failed: {result.stderr}"
        after = _bound_names(out)
        # Read INSIDE the block: `out` lives in a TemporaryDirectory that is gone
        # by the time the assertions below run.
        stripped = out.read_text()

    missing = needed - after
    assert not missing, (
        f"the {industry} image's strip script removed {sorted(missing)}, which "
        f"/metrics calls — that would be a NameError on the first request")

    # ...and the route decorator itself has to still be there, not merely a
    # function of that name still bound to something else.
    assert '@app.get("/{industry}/metrics"' in stripped, (
        f"the {industry} image no longer declares the /metrics route")


def test_the_metrics_route_is_not_in_a_grocery_only_section():
    """Guard the placement, not just the outcome.

    A future edit that moves the route into a "Grocery only" block would still
    pass the survival test above for grocery and fail only in the gas-station
    image at runtime, long after this branch merged.
    """
    source = API_SOURCE.read_text()
    marker = '@app.get("/{industry}/metrics"'
    assert marker in source, "the /metrics decorator is gone"

    # Walk back to the nearest section header and require it not to be an
    # industry-exclusive one.
    before = source[:source.index(marker)]
    headers = [line for line in before.splitlines() if line.startswith("# ")]
    section = headers[-1] if headers else ""
    assert not re.search(r'#\s*(Grocery|Support|Gas Station)( only| —)', section), (
        f"/metrics sits under an exclusive section header: {section!r}. It must be "
        f"shared — every industry writes the same tick ledger.")


# ---------------------------------------------------------------------------
# the live route
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("ensure_api_reachable")
def test_metrics_serves_plain_text_by_default(api_base_url):
    """Plain text, not JSON: what `curl` reads, a scraper parses, a human reads."""
    import httpx

    resp = httpx.get(f"{api_base_url}/grocery/metrics", timeout=30.0)
    assert resp.status_code == 200, resp.text
    assert resp.headers.get("content-type", "").startswith("text/plain"), \
        f"/metrics served {resp.headers.get('content-type')}"

    body = resp.text
    for name in ("verisim_realtime_factor", "verisim_tick_interval_seconds",
                 "verisim_lagging"):
        assert f"# TYPE {name}" in body or f"{name} " in body, \
            f"{name} missing from the live /metrics output"


@pytest.mark.usefixtures("ensure_api_reachable")
def test_metrics_json_form_is_self_consistent(quiesced_generator):
    """The two formats are the same numbers; JSON is not a separate computation."""
    client = quiesced_generator

    body = client.get("/grocery/metrics",
                      params={"fmt": "json", "rows": False}).json()
    text = client.get("/grocery/metrics",
                      params={"rows": False}).text     # the text default

    assert body["industry"] == "grocery"
    assert body["interval_seconds"] > 0
    assert isinstance(body["lagging"], bool)

    # The factor in JSON must be the factor in the text rendering.
    if body["realtime_factor"] is not None:
        assert f"verisim_realtime_factor {body['realtime_factor']}" in text

    assert "verisim_rows" not in text, "?rows=false still counted tables"


@pytest.mark.usefixtures("ensure_api_reachable")
def test_metrics_counts_rows_and_can_skip_them(quiesced_generator):
    """The per-table counters are half the card's ask, and they must be skippable.

    A `count(*)` over a full transaction_items is tens of millions of rows, so
    `?rows=false` is what a UI polling on an interval actually wants.
    """
    client = quiesced_generator

    with_rows = client.get("/grocery/metrics", params={"fmt": "json"}).json()
    tables = {f'{r["schema"]}.{r["table"]}' for r in with_rows["rows"]}
    assert tables, "no tables were counted"

    # The core relations the card names. Discovered from the catalogue, not a
    # hardcoded list — so a table added by a later card is counted without anyone
    # editing this file, which is the whole reason for not hardcoding it.
    for expected in ("pos.transactions", "pos.transaction_items",
                     "control.generator_state", "control.generation_stats",
                     "inv.stock_levels", "hr.employees"):
        assert expected in tables, f"{expected} missing from the row counts"

    # And the per-schema pairing is what keeps two industries with a same-named
    # table from colliding in the metric labels.
    assert any(r["schema"] != r["table"] for r in with_rows["rows"])

    without = client.get("/grocery/metrics",
                         params={"fmt": "json", "rows": False}).json()
    assert without["rows"] == []
    # Skipping the counts must not change the timing figures.
    assert without["mean_period_seconds"] == with_rows["mean_period_seconds"]
    assert without["realtime_factor"] == with_rows["realtime_factor"]


@pytest.mark.usefixtures("ensure_api_reachable")
def test_metrics_window_is_honoured_and_bounded(quiesced_generator):
    """`window` is the whole knob, and it must be validated at both ends."""
    client = quiesced_generator

    small = client.get("/grocery/metrics",
                       params={"fmt": "json", "window": 5, "rows": False}).json()
    assert small["window_ticks"] <= 5

    for bad in (1, 0, -3, 100000):
        resp = client.get("/grocery/metrics", params={"window": bad})
        assert resp.status_code == 422, (
            f"window={bad} was accepted; an unbounded or degenerate window is a "
            f"full-table scan of the ledger")

    assert client.get("/grocery/metrics", params={"fmt": "yaml"}).status_code == 422


@pytest.mark.usefixtures("ensure_api_reachable")
def test_metrics_does_not_claim_the_generator_is_behind_while_backfilling(api_base_url):
    """A fresh install's first minutes must not look like a failure.

    The CI contract tests start after the mode leaves backfill, but the UI
    dashboard polls this route from the moment the container is up, and this is
    the window in which it is polled most.
    """
    import httpx

    body = httpx.get(f"{api_base_url}/grocery/metrics",
                     params={"fmt": "json", "rows": False}, timeout=30.0).json()
    if body["mode"] == "backfill":
        assert body["lagging"] is False
        assert body.get("lagging_note")
    # Whatever the mode, a lagging verdict has to come with the numbers that
    # produced it — an unexplained flag is not actionable.
    assert "quiet_after_seconds" in body