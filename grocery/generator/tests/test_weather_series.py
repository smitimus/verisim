"""
Synthetic weather series tests (t_2ab1fb0a).

The card's done-when is "weather.daily per location (seasonality + random
fronts, seeded for backfill reproducibility); demand/attendance modifiers
wired; test covers correlation sign". This file covers each of those, plus the
two properties that make the series safe to backfill with:

  1. PURITY — same (store, date, config) gives the same row on every process,
     regardless of module-level `random` state. A series that redrew per call
     would hand one date different weather depending on whether the backfill
     (one tick per simulated hour) or realtime (2880 ticks a day) wrote it, and
     a re-seeded backfill would not reproduce the series it replaced — the same
     class of defect as t_94bbf1ce (POS 30x) and t_eb31c99f (online ~120x).

  2. AGREEMENT WITH THE MANUAL SCENARIO — a day at full severity reproduces
     `scenarios.severe_weather.volume_multiplier` and `attendance_modifier`
     exactly, because those are the constants the loss terms are derived from.
     If someone retunes the scenario and the automatic series quietly disagreed,
     the two weather paths would no longer mean the same thing.

  3. THE SIGN of every correlation, on a long enough series for the assertion
     to be about the law and not about one draw: hotter -> more demand, stormier
     -> less, storm-tomorrow -> a pre-buy bump, and each store sitting under its
     region's front (cross-store correlation, not N independent series).

DB-free throughout, like the rest of the suite: the persistence paths
(`ensure_day`, `day_effect`, `ensure_table`) are covered against a live DB by
`tests/test_cross_schema_integrity.py` and the CI integration job, not here.
"""
import json
import math
import os
import re
from datetime import date, datetime, timedelta

import pytest

from grocery.generator.config import Config, WeatherConfig
from grocery.generator.models import weather
from grocery.generator.scenarios import scenario_engine
from grocery.generator.scenarios.scenario_engine import (
    ScenarioContext,
    get_scenario_context,
)

STORE = 'aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa'
OTHER_STORE = 'bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb'


def _cfg(**weather_overrides) -> Config:
    """A config with the hourly/dow shaping flattened, so a volume multiplier
    is readable as 24.0 x the scenario/weather factors alone."""
    cfg = Config()
    cfg.volumes.hourly_weights = [1.0 for _ in range(24)]
    cfg.volumes.day_of_week_multipliers = {
        'monday': 1.0, 'tuesday': 1.0, 'wednesday': 1.0,
        'thursday': 1.0, 'friday': 1.0, 'saturday': 1.0, 'sunday': 1.0,
    }
    for key, value in weather_overrides.items():
        assert hasattr(cfg.weather, key), f'no weather config knob {key!r}'
        setattr(cfg.weather, key, value)
    return cfg


def _correlation(xs, ys) -> float:
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    if var_x <= 0 or var_y <= 0:
        return 0.0
    return cov / math.sqrt(var_x * var_y)


def _series(location_id: str, start: date, days: int, cfg: Config,
            latitude: float = 39.8, region_key: str = 'TX') -> list:
    """`days` consecutive rows for one store, starting at `start`."""
    return [
        weather.daily_weather(location_id, start + timedelta(days=offset), cfg,
                              latitude=latitude, region_key=region_key)
        for offset in range(days)
    ]


# ---------------------------------------------------------------------------
# 1. The series exists, is well-formed, and is deterministic
# ---------------------------------------------------------------------------

def test_row_shape_and_ranges():
    row = weather.daily_weather(STORE, date(2026, 7, 6), _cfg())
    assert row is not None
    assert set(row) == {
        'location_id', 'weather_date', 'temp_high_f', 'temp_low_f',
        'precipitation_in', 'cloud_cover_pct', 'severity_index', 'is_severe',
        'condition_code', 'demand_modifier', 'attendance_modifier', 'seed_key',
    }
    assert row['location_id'] == STORE
    assert row['weather_date'] == date(2026, 7, 6)
    # Every value the table's CHECK constraints and the demand law rely on.
    assert 0.0 <= row['severity_index'] <= 1.0
    assert row['precipitation_in'] >= 0.0
    assert 0.0 <= row['cloud_cover_pct'] <= 100.0
    assert row['temp_low_f'] <= row['temp_high_f']
    assert row['condition_code'] in weather.CONDITIONS
    assert row['demand_modifier'] > 0.0
    assert 0.0 < row['attendance_modifier'] <= 1.0
    assert row['seed_key'] == weather.location_seed(STORE, date(2026, 7, 6))


def test_daily_weather_is_pure_across_calls():
    """Same inputs -> the same row. The whole reproducibility guarantee."""
    cfg = _cfg()
    when = date(2026, 11, 14)
    first = weather.daily_weather(STORE, when, cfg, latitude=44.9, region_key='MN')
    for _ in range(5):
        assert weather.daily_weather(STORE, when, cfg, latitude=44.9,
                                     region_key='MN') == first


def test_daily_weather_ignores_module_random_state():
    """Purity must not be an accident of an unseeded global.

    A series that drew from the module-level `random` would look deterministic
    inside one process until anything else consumed a draw, which is exactly the
    "different value depending on who wrote it" failure in its quietest form.
    """
    import random as random_module

    cfg = _cfg()
    when = date(2026, 2, 3)
    random_module.seed(1234)
    first = weather.daily_weather(STORE, when, cfg, latitude=41.9, region_key='OH')
    for seed in (99, 7, 20260101):
        random_module.seed(seed)
        assert weather.daily_weather(STORE, when, cfg, latitude=41.9,
                                     region_key='OH') == first


def test_daily_weather_is_pure_across_a_fresh_process():
    """A second process proves the seed is DERIVED, not inherited from this
    process's RNG stream — the failure mode `random.seed()` inside one process
    cannot detect, and the one that would make a backfill unreproducible across
    the CI runner and the customer's install.

    PYTHONPATH is threaded through from this process so the child resolves the
    repo and the installed deps (pyyaml/psycopg2) exactly as the parent did,
    whatever the environment looks like.
    """
    import subprocess
    import sys
    import textwrap

    program = textwrap.dedent("""
        import json
        from datetime import date
        from grocery.generator.config import Config
        from grocery.generator.models import weather
        cfg = Config()
        row = weather.daily_weather(
            'aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa', date(2026, 7, 6), cfg,
            latitude=33.7, region_key='GA')
        print(json.dumps({k: str(v) for k, v in sorted(row.items())}))
    """)
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join(p for p in sys.path if p)
    out = subprocess.run([sys.executable, '-c', program], check=True,
                         capture_output=True, text=True, env=env,
                         cwd=os.getcwd())
    row = weather.daily_weather(STORE, date(2026, 7, 6), _cfg(),
                                latitude=33.7, region_key='GA')
    expected = json.dumps({k: str(v) for k, v in sorted(row.items())})
    assert out.stdout.strip() == expected, (
        'the same (store, date, config) produced different weather in another '
        'process — the series is not reproducible')


def test_disabled_series_returns_nothing():
    """`weather.enabled: false` must reach the pre-t_2ab1fb0a behaviour
    everywhere, not just at the call sites that happen to check the flag."""
    cfg = _cfg(enabled=False)
    assert weather.daily_weather(STORE, date(2026, 7, 6), cfg) is None
    assert weather.fronts_for_year('TX', 2026, cfg) == []
    assert weather.severity_on('TX', date(2026, 7, 6), cfg) == 0.0


# ---------------------------------------------------------------------------
# 2. Seasonality
# ---------------------------------------------------------------------------

def test_seasonal_temperature_peaks_in_midsummer():
    """The cosine peaks at SEASON_PEAK_DOY (mid-July) for a northern store.

    Asserted on the daily LOW, not the high: `high = mean + range/2`, so a
    January day at 45N has a perfectly respectable-looking high and only its
    low shows how cold the season is.
    """
    cfg = _cfg(daily_anomaly_std_f=0.0, local_severity_jitter=0.0,
               precip_base_chance=0.0)
    northern = _series(STORE, date(2026, 1, 1), 365, cfg, latitude=45.0)
    coldest = min(northern, key=lambda row: row['temp_low_f'])
    peak_doy = coldest['weather_date'].timetuple().tm_yday
    assert 1 <= peak_doy <= 40, (
        f'coldest day was doy {peak_doy} — the cosine peaks in mid-July, so '
        f'the coldest must fall near the start of the year')

    lows = [row['temp_low_f'] for row in northern]
    assert min(lows) < 10.0, (
        f'a 45N store should have a genuinely cold January (min low '
        f'{min(lows)}F)')
    assert max(lows) > 55.0, f'and a genuinely warm July (max low {max(lows)}F)'


def test_southern_hemisphere_season_is_inverted():
    """Latitude below the equator flips the phase — a Sydney store's summer is
    its own January, not the northern hemisphere's July."""
    cfg = _cfg(daily_anomaly_std_f=0.0, local_severity_jitter=0.0,
               precip_base_chance=0.0)
    southern = _series(STORE, date(2026, 1, 1), 365, cfg, latitude=-33.9)
    warmest = max(southern, key=lambda row: row['temp_low_f'])
    doy = warmest['weather_date'].timetuple().tm_yday
    assert doy <= 45 or doy >= 320, (
        f'a -33.9 store should be warmest around New Year, not July (doy {doy})')


def test_high_latitude_swings_further_than_low():
    cfg = _cfg(daily_anomaly_std_f=0.0, local_severity_jitter=0.0,
               precip_base_chance=0.0)
    def swing(latitude: float) -> float:
        rows = _series(STORE, date(2026, 1, 1), 365, cfg, latitude=latitude)
        highs = [row['temp_high_f'] for row in rows]
        return max(highs) - min(highs)

    assert swing(48.0) > swing(26.0), (
        'a 48N store must swing further through the year than a 26N one')


def test_annual_mean_falls_with_latitude():
    """The swing growing with latitude is only half the climate; the MEAN must
    fall too. Without this a 45N store's coldest day read 28F — "cold" that is
    indistinguishable from a mild one, so the covariate carried no signal for
    exactly the stores where weather matters most.
    """
    cfg = _cfg(daily_anomaly_std_f=0.0, local_severity_jitter=0.0,
               precip_base_chance=0.0)

    def annual_mean(latitude: float) -> float:
        rows = _series(STORE, date(2026, 1, 1), 365, cfg, latitude=latitude)
        return sum(row['temp_low_f'] for row in rows) / len(rows)

    miami, houston, chicago, minneapolis, fairbanks = (
        annual_mean(lat) for lat in (25.8, 29.7, 41.9, 44.9, 64.8))
    assert miami > houston > chicago > minneapolis > fairbanks, (
        'the annual mean must fall monotonically with latitude '
        f'({miami:.0f} / {houston:.0f} / {chicago:.0f} / {minneapolis:.0f} / '
        f'{fairbanks:.0f})')
    # The real US ordering for annual mean LOW is roughly 68/57/40/36/11, so
    # the model is deliberately cooler and flatter than reality: it treats every
    # store as dry-continental, with no maritime moderation for Miami. What it
    # must get right is the ORDER and the rough magnitude, which is what makes a
    # store's temperature a usable covariate rather than an arbitrary number.
    assert 40.0 < miami < 60.0, f'Miami mean {miami:.1f}F'
    assert 30.0 < minneapolis < 45.0, f'Minneapolis mean {minneapolis:.1f}F'
    assert fairbanks < 35.0, f'Fairbanks mean {fairbanks:.1f}F should be freezing'

    # And the gradient is linear in |latitude|, as documented.
    two_degrees_apart = annual_mean(40.0) - annual_mean(42.0)
    assert two_degrees_apart == pytest.approx(
        cfg.weather.latitude_temp_gradient_f * 2.0, abs=0.05)


def test_summer_highs_cross_comfort_so_the_heat_term_is_live():
    """A temperature band that never reached `comfort_temp_f` would make the
    heat->beverage half of the demand law DEAD CODE — every day reading exactly
    1.0 from the heat term, silently.

    This is the assertion that would have caught it: an earlier calibration had
    a US peak of 71F against a 78F comfort threshold, so no store ever saw a
    heat wave.
    """
    cfg = _cfg()
    for latitude in (26.0, 33.7, 41.9, 44.9):
        summer = [row for row in
                  _series(STORE, date(2026, 1, 1), 365, cfg, latitude=latitude)
                  if row['weather_date'].month in (6, 7, 8)]
        peak = max(row['temp_high_f'] for row in summer)
        assert peak > cfg.weather.comfort_temp_f, (
            f'a {latitude}N store peaked at {peak}F, below the '
            f'{cfg.weather.comfort_temp_f}F comfort threshold — the heat term '
            f'can never fire and heat->beverage is dead code')


def test_january_lows_reach_below_freezing_up_north():
    """The mirror of the test above, for the cold end. A band whose coldest
    night never approached `snow_temp_f` (34F) would make snow a condition the
    series could never produce at a northern store."""
    cfg = _cfg()
    winter = [row for row in
              _series(STORE, date(2026, 1, 1), 120, cfg, latitude=44.9)
              if row['weather_date'].month in (1, 2)]
    coldest = min(row['temp_low_f'] for row in winter)
    assert coldest < cfg.weather.snow_temp_f, (
        f'a 44.9N store only reached {coldest}F, above the '
        f'{cfg.weather.snow_temp_f}F snow threshold')


def test_daily_anomaly_is_seeded_but_not_zero():
    """Synoptic noise exists (consecutive days are not a smooth cosine) yet it
    comes from the location/date seed, not from a global."""
    cfg = _cfg()
    rows = _series(STORE, date(2026, 3, 1), 40, cfg, latitude=39.8)
    anomalies = [
        row['temp_high_f'] - (sum(
            r['temp_high_f'] for r in rows) / len(rows))
        for row in rows
    ]
    assert max(anomalies) - min(anomalies) > 10.0, (
        'no day-to-day variation: the series is a pure smooth curve')
    assert weather.daily_weather(STORE, date(2026, 3, 1), cfg) == rows[0]


# ---------------------------------------------------------------------------
# 3. Fronts — random, seeded, regional, seasonal
# ---------------------------------------------------------------------------

def test_fronts_are_generated_and_bounded():
    cfg = _cfg()
    fronts = weather.fronts_for_year('TX', 2026, cfg)
    assert len(fronts) == cfg.weather.fronts_per_year
    for front in fronts:
        assert cfg.weather.front_length_min_days <= front['length'] <= \
            cfg.weather.front_length_max_days
        assert 0.0 <= front['peak'] <= 1.0


def test_fronts_are_seeded_not_redrawn():
    cfg = _cfg()
    first = weather.fronts_for_year('TX', 2026, cfg)
    for _ in range(4):
        assert weather.fronts_for_year('TX', 2026, cfg) == first
    # A different region gets its own storms — a front is not universal.
    assert weather.fronts_for_year('MN', 2026, cfg) != first
    # Nor is another year.
    assert weather.fronts_for_year('TX', 2027, cfg) != first


def test_storms_are_biased_toward_winter():
    """`winter_severity_boost` exists because storm seasons are real: a year
    whose front peaks were uniformly distributed would be a wrong covariate, not
    a merely bland one."""
    cfg = _cfg(winter_severity_boost=0.8)
    cold_months, warm_months = [], []
    for region in ('TX', 'MN', 'OH', 'IL', 'PA', 'NY', 'NC', 'GA'):
        for day_offset in range(365):
            when = date(2026, 1, 1) + timedelta(days=day_offset)
            severity = weather.severity_on(region, when, cfg)
            (cold_months if when.month in (12, 1, 2) else
             warm_months if when.month in (6, 7, 8) else []).append(severity)
    assert (sum(cold_months) / len(cold_months)) > \
        (sum(warm_months) / len(warm_months)), (
        'front severity must be higher in Dec-Feb than Jun-Aug')


def test_fronts_are_shared_across_a_region_and_local_across_stores():
    """The two levels of the model. A front does not stop at a property line,
    so two stores in one state must see the SAME storm; they must still differ
    in how hard it lands (that is the local jitter, and without it the series is
    three identical columns)."""
    cfg = _cfg()
    a = _series(STORE, date(2026, 1, 1), 365, cfg, latitude=31.0, region_key='TX')
    b = _series(OTHER_STORE, date(2026, 1, 1), 365, cfg, latitude=29.8, region_key='TX')
    c = _series(STORE, date(2026, 1, 1), 365, cfg, latitude=31.0, region_key='MN')

    # Region-level severity is identical for two stores in one region.
    for left, right in zip(a, b):
        assert weather.severity_on('TX', left['weather_date'], cfg) == \
            weather.severity_on('TX', right['weather_date'], cfg)
    # …and differs between regions, which is what makes the series interesting.
    tx_sev = [weather.severity_on('TX', r['weather_date'], cfg) for r in a]
    mn_sev = [weather.severity_on('MN', r['weather_date'], cfg) for r in c]
    assert tx_sev != mn_sev
    # The two TX stores stay correlated on the realised severity…
    assert _correlation([r['severity_index'] for r in a],
                        [r['severity_index'] for r in b]) > 0.75
    # …but are not identical, so the local jitter is really doing something.
    assert any(x['severity_index'] != y['severity_index'] for x, y in zip(a, b))


def test_severe_days_exist_but_are_a_minority():
    """A series with no severe days tests nothing; a series where every day is
    severe is not weather, it is a permanent scenario."""
    cfg = _cfg()
    rows = _series(STORE, date(2026, 1, 1), 365, cfg)
    share = sum(1 for r in rows if r['is_severe']) / len(rows)
    assert 0.005 <= share <= 0.35, (
        f'{share:.1%} of days flagged severe — the flag has lost its meaning '
        f'if it is off both ends')


def test_pre_buy_signal_exists_the_day_before_a_front():
    """`front_edge_fraction` exists so severity(tomorrow) is readable BEFORE the
    storm lands; the pre-buy term in the demand law is unreadable without it.
    Asserted structurally on `modifiers_for`, not by hunting for a front: the
    law must hold for any (today, tomorrow) pair."""
    cfg = _cfg()
    demand, _ = weather.modifiers_for(0.0, 1.0, 60.0, cfg)
    assert demand == pytest.approx(1.0 + cfg.weather.pre_buy_gain)


# ---------------------------------------------------------------------------
# 4. The demand law — correlation signs (the card's explicit requirement)
# ---------------------------------------------------------------------------

def test_heat_raises_demand():
    cfg = _cfg()
    # Below comfort_temp_f the heat term is exactly zero — that is the point of
    # a comfort threshold, so the baseline is pinned to it rather than to 60F.
    cool, _ = weather.modifiers_for(0.0, 0.0, cfg.weather.comfort_temp_f, cfg)
    hot, _ = weather.modifiers_for(0.0, 0.0, 100.0, cfg)
    assert cool == pytest.approx(1.0), 'no heat term at or below comfort'
    assert hot > cool, 'heat must move demand UP (beverages)'
    # Linear in the degrees ABOVE COMFORT, as documented — so the gain is
    # per degree of real heat, not per degree of absolute temperature.
    degrees = 100.0 - cfg.weather.comfort_temp_f
    assert hot - cool == pytest.approx(cfg.weather.heat_demand_gain * degrees)


def test_severity_lowers_demand_today():
    cfg = _cfg()
    calm, _ = weather.modifiers_for(0.0, 0.0, 60.0, cfg)
    storm, _ = weather.modifiers_for(1.0, 0.0, 60.0, cfg)
    assert storm < calm, 'a storm on the day must cost footfall'
    assert storm == pytest.approx(cfg.scenarios.weather_volume_multiplier)


def test_severity_tomorrow_raises_demand_today():
    cfg = _cfg()
    quiet, _ = weather.modifiers_for(0.0, 0.0, 60.0, cfg)
    pre_buy, _ = weather.modifiers_for(0.0, 0.8, 60.0, cfg)
    assert pre_buy > quiet, 'the storm-shop must buy BEFORE the storm'


def test_net_sign_of_a_mild_front_is_negative():
    """A front that only partly arrives still costs footfall net of the
    pre-buy, at these gains. Pinning it stops a future retune from quietly
    turning every light shower into a boom."""
    cfg = _cfg()
    demand, _ = weather.modifiers_for(0.5, 0.0, 60.0, cfg)
    assert demand < 1.0


def test_attendance_falls_with_severity_and_never_rises():
    cfg = _cfg()
    previous = 1.0
    for severity in (0.0, 0.2, 0.4, 0.6, 0.8, 1.0):
        _, attendance = weather.modifiers_for(severity, 0.0, 60.0, cfg)
        assert 0.0 < attendance <= 1.0
        assert attendance <= previous, 'attendance must be monotone in severity'
        previous = attendance


def test_demand_modifier_is_clamped_both_ways():
    """Without the ceiling a 130F day multiplies the shop by ~2.3 and the volume
    law stops meaning anything.

    The FLOOR is deliberately unreachable at the configured gains: the worst day
    the law can produce is a full-severity storm costing
    `1 - weather_volume_multiplier` = 0.3, which lands at 0.7 — comfortably above
    the 0.4 floor. It is a safety net for a future retune of the scenario
    constants, not a binding clamp, and this test says so rather than pretending
    it binds.
    """
    cfg = _cfg()
    hot, _ = weather.modifiers_for(0.0, 0.0, 130.0, cfg)
    assert hot == pytest.approx(cfg.weather.demand_modifier_ceiling), (
        'the ceiling must bind on extreme heat')

    worst_possible = cfg.scenarios.weather_volume_multiplier
    assert worst_possible > cfg.weather.demand_modifier_floor, (
        'the floor must sit below anything the severity term can produce, or it '
        'silently erases the storm signal')
    coldest_storm, _ = weather.modifiers_for(1.0, 0.0, -40.0, cfg)
    assert coldest_storm == pytest.approx(worst_possible), (
        'severity and temperature cannot push below the scenario constant')

    # The floor DOES bind if a retune makes the storm loss deeper than it.
    cfg.scenarios.weather_volume_multiplier = 0.2
    floored, _ = weather.modifiers_for(1.0, 0.0, -40.0, cfg)
    assert floored == pytest.approx(cfg.weather.demand_modifier_floor)


def test_sign_correlations_over_a_full_year():
    """The card asks for a test on the correlation SIGN. On the realised series
    rather than on `modifiers_for`, because a law that holds pairwise but
    produces a series with the wrong correlation is still a broken covariate.

    Measured over 12 stores x 365 days so the assertions are about the model,
    not about one store's draw.
    """
    cfg = _cfg()
    highs, severities, demands, todays_severity = [], [], [], []
    for index in range(12):
        location = f'{index:08x}-0000-4000-8000-000000000000'
        region = ('TX', 'MN', 'FL', 'OH', 'IL', 'PA', 'NY', 'NC', 'GA',
                  'TN', 'IN', 'WI')[index]
        latitude = 26.0 + index * 1.6
        for row in _series(location, date(2026, 1, 1), 365, cfg,
                           latitude=latitude, region_key=region):
            highs.append(row['temp_high_f'])
            severities.append(row['severity_index'])
            demands.append(row['demand_modifier'])
            todays_severity.append(
                weather.severity_on(region, row['weather_date'], cfg))

    assert _correlation(highs, demands) > 0.0, (
        'hotter days must carry more demand (heat -> beverages)')
    assert _correlation(severities, demands) < 0.0, (
        'stormier days must carry less demand')
    # The pre-buy term: today's severity must not explain away the relationship
    # with TOMORROW's, or the covariate would carry no forward-looking signal.
    assert _correlation(todays_severity, demands) < 0.0


def test_pre_buy_correlates_positively_with_tomorrows_severity():
    """The pre-buy term is the only reason the series is worth forecasting:
    yesterday's weather says something about today's basket."""
    cfg = _cfg()
    todays_severity, demands, tomorrows_severity = [], [], []
    for index in range(8):
        location = f'{index:08x}-0000-4000-8000-000000000000'
        region = ('TX', 'MN', 'FL', 'OH', 'IL', 'PA', 'NY', 'NC')[index]
        for row in _series(location, date(2026, 1, 1), 365, cfg,
                           latitude=30.0 + index, region_key=region):
            todays_severity.append(row['severity_index'])
            demands.append(row['demand_modifier'])
            tomorrows_severity.append(
                weather.severity_on(region,
                                    row['weather_date'] + timedelta(days=1), cfg))

    assert _correlation(tomorrows_severity, demands) > 0.0, (
        "tomorrow's severity must raise TODAY's demand — the storm-shop")
    # And today's own severity must dominate tomorrow's in magnitude, or the
    # sign would be right for the wrong reason.
    assert abs(_correlation(todays_severity, demands)) > \
        abs(_correlation(tomorrows_severity, demands))


# ---------------------------------------------------------------------------
# 5. Agreement with the manual `severe_weather` scenario
# ---------------------------------------------------------------------------

def test_full_severity_reproduces_the_scenario_constants():
    """THE anti-drift assertion. The two loss terms are derived from
    `scenarios.severe_weather`, so a day at severity 1.0 must land exactly on
    the manual scenario's volume and attendance multipliers. Retuning the
    scenario retunes the series with it, by construction."""
    cfg = _cfg()
    demand, attendance = weather.modifiers_for(1.0, 0.0, 60.0, cfg)
    assert demand == pytest.approx(cfg.scenarios.weather_volume_multiplier)
    assert attendance == pytest.approx(cfg.scenarios.weather_attendance_modifier)


def test_retuning_the_scenario_moves_the_series_with_it():
    """Not just "they agree today" — the derivation is live. A test that only
    compared the two constants would still pass if someone replaced the derived
    terms with hardcoded copies of today's numbers."""
    cfg = _cfg()
    cfg.scenarios.weather_volume_multiplier = 0.5
    cfg.scenarios.weather_attendance_modifier = 0.6
    demand, attendance = weather.modifiers_for(1.0, 0.0, 60.0, cfg)
    assert demand == pytest.approx(0.5)
    assert attendance == pytest.approx(0.6)


def test_a_series_day_lands_on_the_manual_scenario_when_severe():
    """End to end through `daily_weather`.

    `is_severe` is a THRESHOLD flag (severity >= 0.70), not "severity == 1.0",
    so a flagged day can carry attendance anywhere between the scenario
    constant (at severity 1.0) and barely-below-normal (at the threshold). The
    invariant that must hold on every flagged day is therefore
    `attendance == 1 - (1 - scenario) * severity` — the law itself — with the
    scenario constant as the floor, not as the value.
    """
    cfg = _cfg()
    rows = _series(STORE, date(2026, 1, 1), 365, cfg)
    severe = [row for row in rows if row['is_severe']]
    assert severe, 'no severe day in a year of series'
    for row in severe:
        expected = 1.0 - (1.0 - cfg.scenarios.weather_attendance_modifier) \
            * row['severity_index']
        assert row['attendance_modifier'] == pytest.approx(expected, abs=1e-4), (
            f'{row["weather_date"]}: severity {row["severity_index"]} gave '
            f'attendance {row["attendance_modifier"]}, law says {expected:.4f}')
        # A severe day must genuinely cost attendance — it can never be neutral.
        # It is NOT bounded above by the scenario constant: at the 0.70
        # threshold a flagged day still reads 0.8, because the constant is the
        # value at severity 1.0 and the law is linear below it.
        assert row['attendance_modifier'] < 1.0

    # The maximum-severity day in the year lands exactly on the scenario value.
    peak = max(severe, key=lambda row: row['severity_index'])
    if peak['severity_index'] >= 1.0:
        assert peak['attendance_modifier'] == pytest.approx(
            cfg.scenarios.weather_attendance_modifier)


# ---------------------------------------------------------------------------
# 6. Wiring into the scenario context (the hooks)
# ---------------------------------------------------------------------------

def _effect(**overrides) -> dict:
    effect = {
        'demand_modifier': 1.0,
        'attendance_modifier': 1.0,
        'severity_index': 0.0,
        'condition_code': 'clear',
        'is_severe_day': False,
    }
    effect.update(overrides)
    return effect


def test_no_weather_effect_leaves_the_context_exactly_as_before():
    """The pre-t_2ab1fb0a context is the regression target: a day with no rows
    (series off, or a data dir seeded before this card) must be byte-identical
    to what the old code produced."""
    cfg = _cfg()
    when = datetime(2026, 7, 6, 12, 0, 0)  # Monday
    without = get_scenario_context(['normal'], 1.0, when, cfg)
    for effect in (None, {}):
        assert get_scenario_context(['normal'], 1.0, when, cfg,
                                    weather_effect=effect) == without
    assert without.weather_volume_modifier == 1.0
    assert without.weather_condition == ''
    assert without.attendance_modifier == 1.0


def test_weather_effect_scales_volume_and_attendance():
    cfg = _cfg()
    when = datetime(2026, 7, 6, 12, 0, 0)
    plain = get_scenario_context(['normal'], 1.0, when, cfg)
    stormy = get_scenario_context(
        ['normal'], 1.0, when, cfg,
        weather_effect=_effect(demand_modifier=0.6, attendance_modifier=0.7,
                               severity_index=0.9, condition_code='severe_storm',
                               is_severe_day=True))

    # volume_multiplier is 24.0 flat under the flattened config, so the ratio is
    # the weather factor and nothing else.
    assert plain.volume_multiplier == pytest.approx(24.0)
    assert stormy.volume_multiplier == pytest.approx(24.0 * 0.6)
    assert stormy.weather_volume_modifier == pytest.approx(0.6)
    assert stormy.attendance_modifier == pytest.approx(0.7)
    assert stormy.weather_severity == pytest.approx(0.9)
    assert stormy.weather_condition == 'severe_storm'
    # The unmodified field must stay clean: a reader comparing a weather day
    # with a promotion day needs to tell them apart.
    assert stormy.coupon_multiplier == 1.0
    assert not stormy.active_promotions


def test_weather_effect_stacks_with_a_manual_scenario():
    cfg = _cfg()
    when = datetime(2026, 7, 6, 12, 0, 0)
    both = get_scenario_context(
        ['promotion'], 1.0, when, cfg,
        weather_effect=_effect(demand_modifier=0.8, attendance_modifier=0.9))
    promotion_only = get_scenario_context(['promotion'], 1.0, when, cfg)
    assert both.volume_multiplier == pytest.approx(
        promotion_only.volume_multiplier * 0.8)
    assert 'promotion' in both.scenario_tag


def test_manual_severe_weather_scenario_wins_over_a_milder_day():
    """`min`, not assignment: a day can only remove attendance, so a manual
    `severe_weather` already on the books (0.75) must survive an automatic day
    that is milder. Assignment would let a clear day silently restore full
    attendance while the scenario is switched on."""
    cfg = _cfg()
    when = datetime(2026, 7, 6, 12, 0, 0)
    ctx = get_scenario_context(
        ['severe_weather'], 1.0, when, cfg,
        weather_effect=_effect(demand_modifier=1.0, attendance_modifier=0.95))
    assert ctx.attendance_modifier == pytest.approx(
        cfg.scenarios.weather_attendance_modifier)


def test_weather_day_is_tagged_only_when_it_actually_cost_footfall():
    cfg = _cfg()
    when = datetime(2026, 7, 6, 12, 0, 0)

    mild = get_scenario_context(
        ['normal'], 1.0, when, cfg,
        weather_effect=_effect(demand_modifier=0.98, is_severe_day=True))
    assert mild.scenario_tag == 'normal', (
        'a 2% dip is not a weather day; tagging it would drown the tag column')

    real = get_scenario_context(
        ['normal'], 1.0, when, cfg,
        weather_effect=_effect(demand_modifier=0.7, is_severe_day=True))
    assert 'weather_severe' in real.scenario_tag

    heat = get_scenario_context(
        ['normal'], 1.0, when, cfg,
        weather_effect=_effect(demand_modifier=1.3, is_severe_day=False))
    assert heat.scenario_tag == 'normal', (
        'a heat wave raises demand but storms nothing; it belongs in '
        'weather.daily, not in the scenario tag')


def test_weather_tag_composes_with_a_holiday_tag():
    cfg = _cfg()
    # 2026-11-26 is Thanksgiving (4th Thursday).
    when = datetime(2026, 11, 26, 12, 0, 0)
    ctx = get_scenario_context(
        ['normal'], 1.0, when, cfg,
        weather_effect=_effect(demand_modifier=0.7, is_severe_day=True))
    assert 'thanksgiving_week' in ctx.scenario_tag
    assert 'weather_severe' in ctx.scenario_tag
    assert ctx.scenario_tag.count('+') == 1


def test_weather_effect_does_not_disturb_the_hour_or_dow_laws():
    """The hour weight and day-of-week multiplier must be unaffected by weather,
    or a regression in the shared volume law (t_94bbf1ce / t_eb31c99f) could hide
    behind a weather factor.

    Both sides use the SAME real (unflattened) config: the weather factor is
    common to both hours and must cancel out of the ratio, leaving exactly the
    ratio of the two hour weights.
    """
    cfg = Config()   # real hourly weights, real day-of-week multipliers
    effect = _effect(demand_modifier=0.8, attendance_modifier=0.9)
    noon = datetime(2026, 7, 6, 12, 0, 0)    # Monday
    midnight = datetime(2026, 7, 6, 0, 0, 0)

    noon_w = get_scenario_context(['normal'], 1.0, noon, cfg,
                                  weather_effect=effect)
    midnight_w = get_scenario_context(['normal'], 1.0, midnight, cfg,
                                      weather_effect=effect)
    assert noon_w.volume_multiplier / midnight_w.volume_multiplier == \
        pytest.approx(cfg.volumes.hourly_weights[12] /
                      cfg.volumes.hourly_weights[0])

    # And the weather factor itself is exactly the ratio against the same
    # instant with no weather — nothing else moved.
    plain = get_scenario_context(['normal'], 1.0, noon, cfg)
    assert noon_w.volume_multiplier / plain.volume_multiplier == \
        pytest.approx(0.8)


# ---------------------------------------------------------------------------
# 7. The schema contract
# ---------------------------------------------------------------------------

def _schema_sql() -> str:
    import os
    path = os.path.join(os.path.dirname(__file__), '..', 'schema.sql')
    with open(path, 'r', encoding='utf-8') as fh:
        return fh.read()


def test_ddl_matches_schema_sql():
    """`models.weather.DDL` is the copy that reaches an EXISTING data dir (a
    schema.sql change only hits a fresh bootstrap), so the two must declare the
    same thing. A drift here would mean a table created on upgrade has different
    constraints from the one a fresh install gets — and only one of them would
    match what the generator writes.
    """
    schema = _schema_sql()
    block = schema[schema.index('CREATE SCHEMA IF NOT EXISTS weather;'):]
    normalise = lambda text: re.sub(r'\s+', ' ', text).strip()  # noqa: E731
    # schema.sql declares without IF NOT EXISTS (it runs once, on a fresh DB);
    # the module's copy adds it so the upgrade path is idempotent.
    assert normalise(block).replace('IF NOT EXISTS ', '') == \
        normalise(weather.DDL).replace('IF NOT EXISTS ', '')


def test_config_yaml_documents_the_defaults_and_overrides_nothing():
    """`grocery/config.yaml` now carries a documented `weather:` block. It must
    DOCUMENT the defaults, not override them: a value there that differs from
    `WeatherConfig` silently changes the series for anyone who deploys that file
    while their unit tests (which use `Config()`) keep asserting the other one.
    That is the "two sources of truth for one knob" defect in a config file.
    """
    import yaml

    path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.yaml')
    with open(path, 'r', encoding='utf-8') as fh:
        data = yaml.safe_load(fh)

    assert 'weather' in data, 'grocery/config.yaml has no weather section'
    defaults = WeatherConfig()
    differing = []
    for key, value in data['weather'].items():
        assert hasattr(defaults, key), (
            f'config.yaml sets weather.{key}, which is not a WeatherConfig '
            f'field — it would be silently ignored by _apply_yaml')
        if value != getattr(defaults, key):
            differing.append((key, value, getattr(defaults, key)))
    assert not differing, (
        'config.yaml overrides the documented defaults: '
        + '; '.join(f'{k}: yaml={y!r} default={d!r}' for k, y, d in differing))


def test_every_column_the_generator_writes_is_declared():
    """The row dict `daily_weather` returns IS the INSERT tuple. A column added
    to one and not the other fails at runtime, on a customer's data dir, not in
    CI."""
    schema = _schema_sql()
    block = schema[schema.index('CREATE TABLE weather.daily'):]
    # Column lines only: the table also contains PRIMARY KEY and CONSTRAINT
    # clauses at the same indent, which are not columns.
    declared = set(re.findall(r'^\s{4}(?!PRIMARY\b|CONSTRAINT\b|CHECK\b|FOREIGN\b|REFERENCES\b)'
                              r'(\w+)\s+\w', block, flags=re.MULTILINE))
    row = weather.daily_weather(STORE, date(2026, 7, 6), _cfg())
    assert set(row) <= declared, (
        f'columns written but not declared: {set(row) - declared}')
    # created_at is the only column the table adds on its own.
    assert declared - set(row) == {'created_at'}


def test_table_has_the_integrity_constraints_the_law_relies_on():
    """The demand law guarantees `demand_modifier > 0` and
    `attendance_modifier <= 1` by clamping; the table's CHECKs are what make
    that a DATABASE invariant rather than a convention, and
    `pos.transaction_items`' own `quantity > 0` is the precedent for it."""
    schema = _schema_sql()
    block = schema[schema.index('CREATE TABLE weather.daily'):]
    for constraint in ('weather_temp_ordering',
                       'CHECK (demand_modifier > 0)',
                       'CHECK (attendance_modifier > 0',
                       'CHECK (precipitation_in >= 0)',
                       'CHECK (severity_index >= 0 AND severity_index <= 1)',
                       'PRIMARY KEY (location_id, weather_date)'):
        assert constraint in block, f'{constraint} missing from weather.daily'


def test_weather_table_is_indexed_for_the_two_reads_data_lab_will_make():
    schema = _schema_sql()
    for index in ('idx_weather_date', 'idx_weather_location'):
        assert f'CREATE INDEX {index}' in schema, (
            f'{index} missing — a forecasting join would be a sequential scan')