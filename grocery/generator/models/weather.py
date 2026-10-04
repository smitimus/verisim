"""
Synthetic weather — the external covariate the grocery sim had no source for
(t_2ab1fb0a).

THE GAP. Weather reached the simulation only when a human switched the
`severe_weather` scenario on by hand (`scenario_engine._apply_single_scenario`,
lines 209-211). The automatic calendar covers holidays alone. So grocery
demand — famously weather-correlated — had no continuous covariate at all, and
data-lab had nothing to regress a forecast on except a column of tags that was
either entirely `normal` or entirely `severe_weather`.

WHAT THIS GENERATES. A per-location daily series in `weather.daily`: a
seasonal temperature swing from latitude and day-of-year, seeded synoptic noise
for the daily anomaly, and weather FRONTS — multi-day severity events seeded
per region, so the whole state gets the same storm and each store sits under it
with its own local deviation. It stays synthetic, per the generator ADR
(stdlib + psycopg2 + pyyaml, `grocery/generator/AGENTS.md`).

DETERMINISM IS THE WHOLE POINT. `daily_weather()` is a PURE FUNCTION of
(location, date, config): the same inputs give the same row on every process,
every run, on every machine. That is the same requirement that made
`main.daily_volume_target()` a pure function of the date — the backfill replays
a day an hour at a time and realtime writes it 2880 times a day, so anything
that drew per call would hand the same date different weather depending on who
wrote it (t_94bbf1ce, t_eb31c99f). A weather series that re-rolled on read
would also make a backfill irreproducible, and the card asks for backfill
reproducibility by name. Nothing here touches the module-level `random`.

THE LAW, AND WHY THE TWO EXISTING NUMBERS DECIDE IT. A day's demand and
attendance respond to the day's severity, and the day BEFORE a front carries a
pre-buy bump (the storm-shop):

    demand    = 1 + heat_gain * max(0, high_f - comfort_f)
                   + pre_buy_gain * severity(tomorrow)
                   - (1 - weather_volume_multiplier)     * severity(today)
    attendance= 1 - (1 - weather_attendance_modifier)     * severity(today)

Both loss terms are DERIVED from the `severe_weather` scenario constants
already in config rather than newly invented, so the automatic path and the
manual scenario agree by construction: a full-severity day reproduces
`weather_volume_multiplier` (0.7) for volume and
`weather_attendance_modifier` (0.75) for attendance exactly. A second, divergent
set of "realistic" weather coefficients would have been the same
two-sources-of-truth defect in a new costume, and the two paths disagreeing is
precisely what a reviewer would not be able to see.

NOT WIRED HERE: `supply_disruption`. A severe storm plausibly strands a truck,
but that is the `severe_weather` scenario's own effect and the card asks only
for the demand and attendance hooks. Leaving it manual keeps one place that
decides delivery failures.

`ScenarioContext` then consumes the day's effect (`scenario_engine`), and this
module owns the persistence: `ensure_day()` upserts one row per store, and
`day_effect()` reads the persisted rows back so the modifier applied to a tick
can never disagree with the covariate a downstream analyst joins on.
"""
import calendar
import logging
import math
import random
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional

import psycopg2.extras

from config import Config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------
# Kept byte-identical (modulo `IF NOT EXISTS`) to the block in `schema.sql`;
# `tests/test_weather_series.py::test_ddl_matches_schema_sql` enforces that.
# The `IF NOT EXISTS` copy is what runs on an EXISTING data dir: a schema.sql
# change only reaches a fresh bootstrap, so without this an install generated
# before t_2ab1fb0a has no `weather` schema at all and every tick would raise
# on the read-back. Same trap as `elasticity.seed_elasticity_columns` and
# `base/api/main.py::_has_stockout_tables`.
DDL = """
CREATE SCHEMA IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.daily (
    location_id         UUID         NOT NULL REFERENCES hr.locations(location_id),
    weather_date        DATE         NOT NULL,
    temp_high_f         NUMERIC(5,1) NOT NULL,
    temp_low_f          NUMERIC(5,1) NOT NULL,
    precipitation_in    NUMERIC(5,2) NOT NULL CHECK (precipitation_in >= 0),
    cloud_cover_pct     NUMERIC(5,1) NOT NULL CHECK (cloud_cover_pct BETWEEN 0 AND 100),
    severity_index      NUMERIC(5,3) NOT NULL CHECK (severity_index >= 0 AND severity_index <= 1),
    is_severe           BOOLEAN      NOT NULL,
    condition_code      VARCHAR(20)  NOT NULL
                           CHECK (condition_code IN ('clear', 'cloudy', 'rain',
                                                     'snow', 'severe_storm')),
    demand_modifier     NUMERIC(7,4) NOT NULL CHECK (demand_modifier > 0),
    attendance_modifier NUMERIC(7,4) NOT NULL CHECK (attendance_modifier > 0
                                                     AND attendance_modifier <= 1),
    -- The seed that produced this row. Persisted so an analyst can re-derive
    -- the series (and prove it is reproducible) without the generator's code.
    seed_key            VARCHAR(120) NOT NULL,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (location_id, weather_date),
    -- A day's low cannot sit above its high, whatever the generator drew.
    CONSTRAINT weather_temp_ordering CHECK (temp_low_f <= temp_high_f)
);

CREATE INDEX IF NOT EXISTS idx_weather_date     ON weather.daily (weather_date);
CREATE INDEX IF NOT EXISTS idx_weather_location ON weather.daily (location_id, weather_date);
"""

CONDITIONS = ('clear', 'cloudy', 'rain', 'snow', 'severe_storm')

# Day-of-year at which the seasonal cosine peaks (mid-July, northern
# hemisphere). Invert latitude sign below the equator.
SEASON_PEAK_DOY = 195


# ---------------------------------------------------------------------------
# The seeded series — pure functions of (location, date, config)
# ---------------------------------------------------------------------------

def _days_in_year(year: int) -> int:
    return 366 if calendar.isleap(int(year)) else 365


def _seasonal_temperature(doy: int, latitude: float, cfg: Config) -> float:
    """The day's mean temperature from latitude and day-of-year, in °F.

    Two latitude terms, because climate has two:

      - the MEAN falls with latitude (a Minneapolis store is colder than a
        Houston one on the same date, every date), via
        `latitude_temp_gradient_f` per degree;
      - the seasonal SWING grows with it (`seasonal_amplitude_f` x |lat|/40),
        so the north has both a lower floor and a wider range.

    With the mean flat, the swing term alone left a 45N store bottoming out
    around 28F in January — a "cold" that is indistinguishable from a mild
    one, which is no covariate at all. `mean_temp_f` is the reference at the
    equator, so a US store lands where the real ones do.

    The cosine peaks in mid-July, and is sign-flipped below the equator so a
    southern store's warm season is its own summer.
    """
    lat = float(latitude)
    mean = (cfg.weather.mean_temp_f
            - cfg.weather.latitude_temp_gradient_f * abs(lat))
    gain = min(abs(lat) / 40.0, 1.25)
    amplitude = cfg.weather.seasonal_amplitude_f * gain
    hemisphere = -1.0 if lat < 0 else 1.0
    phase = 2.0 * math.pi * (int(doy) - SEASON_PEAK_DOY) / 365.25
    return mean + hemisphere * amplitude * math.cos(phase)


def front_seed(region_key: str, year: int) -> str:
    """The seed string that fixes a region's fronts for a year.

    Fronts are keyed on the REGION, not the store, because a weather front does
    not stop at a store's property line — every store in the state is under the
    same one. Keying per store would give each store an independent storm and
    destroy the cross-store correlation a forecaster actually has to model.
    """
    return 'verisim-weather-front-%s-%d' % (region_key, int(year))


def location_seed(location_id: str, weather_date: date) -> str:
    """The seed string fixing one location's local deviations on one date."""
    return 'verisim-weather-%s-%s' % (location_id, weather_date.isoformat())


def fronts_for_year(region_key: str, year: int, cfg: Config) -> List[Dict[str, float]]:
    """Every front in one region-year: day-of-year, length, peak severity.

    Deterministic in `(region_key, year, cfg)` — the same call on any machine
    returns the same storms, which is what makes a backfill reproducible. Peaks
    are biased toward the region's winter by `winter_severity_boost`: storms
    that knock out a region are cold-season events.
    """
    if not cfg.weather.enabled:
        return []
    span = _days_in_year(year)
    rng = random.Random(front_seed(region_key, year))
    count = max(0, int(cfg.weather.fronts_per_year))
    min_len = max(1, int(cfg.weather.front_length_min_days))
    max_len = max(min_len, int(cfg.weather.front_length_max_days))
    lo = max(0.0, float(cfg.weather.front_peak_min))
    hi = max(lo, float(cfg.weather.front_peak_max))

    fronts: List[Dict[str, float]] = []
    for _ in range(count):
        doy = rng.randrange(0, span)
        length = rng.randint(min_len, max_len)
        peak = rng.uniform(lo, hi)
        # Winter bias: a cosine peaking at mid-January, ±boost.
        winter_phase = 2.0 * math.pi * (doy - 15) / 365.25
        peak *= 1.0 + cfg.weather.winter_severity_boost * math.cos(winter_phase)
        fronts.append({'doy': doy, 'length': float(length), 'peak': min(peak, 1.0)})
    return fronts


def _front_severity(fronts: Iterable[Dict[str, float]], doy: int, cfg: Config) -> float:
    """The strongest front covering `doy`, 0..1.

    A front's shape never reaches zero on its own edge days (`edge_fraction`),
    so a shopper can see it coming: a day immediately before a front still sees
    a real `severity(tomorrow)` and earns a pre-buy bump, which is the signal
    the demand law reads.
    """
    edge = max(0.0, min(1.0, cfg.weather.front_edge_fraction))
    best = 0.0
    for front in fronts:
        length = int(front['length'])
        center = (length - 1) / 2.0
        offset = abs(int(doy) - int(front['doy']))
        if offset > center:
            continue
        shape = edge + (1.0 - edge) * (1.0 - offset / max(center, 1.0))
        best = max(best, float(front['peak']) * shape)
    return min(best, 1.0)


def _severity_series(region_key: str, year: int, cfg: Config) -> List[float]:
    """Severity for every day-of-year of one region-year. ``fronts_for_year``
    is the only thing that draws, so this is a deterministic reshape of it."""
    fronts = fronts_for_year(region_key, year, cfg)
    return [_front_severity(fronts, doy, cfg) for doy in range(_days_in_year(year))]


def severity_on(region_key: str, when: date, cfg: Config) -> float:
    """The region's severity on `when`, 0..1. Public for tests and callers that
    want the covariate without a store's local deviation."""
    series = _severity_series(region_key, when.year, cfg)
    return series[when.timetuple().tm_yday - 1]


def daily_weather(location_id: str, when: date, cfg: Config,
                  latitude: Optional[float] = None,
                  region_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """One location's weather on one date — a PURE FUNCTION of its arguments.

    Returns the exact row that would be written to `weather.daily`, or None when
    the series is disabled (`weather.enabled: false`).

    Pure means: two calls with the same arguments are byte-identical regardless
    of module-level `random` state, process, or machine. That is what lets the
    backfill and realtime agree on a day, and what makes a re-seeded backfill
    reproduce the series it replaces.
    """
    if not cfg.weather.enabled:
        return None

    lat = float(cfg.weather.default_latitude if latitude is None else latitude)
    region = str(region_key or location_id)
    doy = when.timetuple().tm_yday

    severity = severity_on(region, when, cfg)
    tomorrow = severity_on(region, when + timedelta(days=1), cfg)

    # Local deviation: the seeded daily anomaly in temperature and the local
    # wobble in how hard this particular front lands.
    local = random.Random(location_seed(location_id, when))
    base = _seasonal_temperature(doy, lat, cfg)
    anomaly = local.gauss(0.0, cfg.weather.daily_anomaly_std_f)
    high = base + cfg.weather.diurnal_range_f / 2.0 + anomaly
    low = base - cfg.weather.diurnal_range_f / 2.0 + anomaly
    local_severity = min(max(severity * local.uniform(
        1.0 - cfg.weather.local_severity_jitter,
        1.0 + cfg.weather.local_severity_jitter), 0.0), 1.0)

    precip_chance = min(1.0, cfg.weather.precip_base_chance
                        + cfg.weather.precip_severity_gain * local_severity)
    precip = (local.uniform(cfg.weather.precip_min_in, cfg.weather.precip_max_in)
              if local.random() < precip_chance else 0.0)
    cloud = min(100.0, max(0.0, local.gauss(
        35.0 + 55.0 * local_severity, cfg.weather.cloud_cover_std_pct)))

    is_severe = local_severity >= cfg.weather.severe_threshold
    if is_severe:
        condition = 'severe_storm'
    elif precip >= cfg.weather.precip_significant_in and high <= cfg.weather.snow_temp_f:
        condition = 'snow'
    elif precip >= cfg.weather.precip_significant_in:
        condition = 'rain'
    elif cloud >= cfg.weather.cloudy_threshold_pct:
        condition = 'cloudy'
    else:
        condition = 'clear'

    demand, attendance = modifiers_for(
        severity_today=local_severity,
        severity_tomorrow=tomorrow,
        temp_high_f=high,
        cfg=cfg,
    )

    return {
        'location_id': str(location_id),
        'weather_date': when,
        'temp_high_f': round(high, 1),
        'temp_low_f': round(min(low, high), 1),
        'precipitation_in': round(precip, 2),
        'cloud_cover_pct': round(cloud, 1),
        'severity_index': round(local_severity, 3),
        'is_severe': is_severe,
        'condition_code': condition,
        'demand_modifier': round(demand, 4),
        'attendance_modifier': round(attendance, 4),
        'seed_key': location_seed(location_id, when),
    }


def modifiers_for(severity_today: float, severity_tomorrow: float,
                  temp_high_f: float, cfg: Config) -> tuple:
    """The demand and attendance multipliers for a day — the law, in one place.

    The two loss terms come from the `severe_weather` scenario constants, so a
    day at full severity (1.0) yields exactly `weather_volume_multiplier` and
    `weather_attendance_modifier`: the automatic series and the manual scenario
    cannot disagree about what a total storm does to the shop.
    """
    severity_today = max(0.0, min(1.0, float(severity_today)))
    severity_tomorrow = max(0.0, min(1.0, float(severity_tomorrow)))
    heat = max(0.0, float(temp_high_f) - cfg.weather.comfort_temp_f)

    demand = (1.0
              + cfg.weather.heat_demand_gain * heat
              + cfg.weather.pre_buy_gain * severity_tomorrow
              - (1.0 - cfg.scenarios.weather_volume_multiplier) * severity_today)
    attendance = 1.0 - (1.0 - cfg.scenarios.weather_attendance_modifier) * severity_today

    demand = min(max(demand, cfg.weather.demand_modifier_floor),
                 cfg.weather.demand_modifier_ceiling)
    attendance = max(attendance, cfg.weather.attendance_modifier_floor)
    return demand, attendance


def heat_index(temp_high_f: float, cfg: Config) -> float:
    """Degrees above comfort — the covariate the heat term reads. Kept here so
    the generator and the scenario engine cannot compute it differently."""
    return max(0.0, float(temp_high_f) - cfg.weather.comfort_temp_f)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def ensure_table(conn) -> bool:
    """Create `weather.daily` if this data dir predates t_2ab1fb0a. Idempotent."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'weather' AND table_name = 'daily'
        """)
        if cur.fetchone()[0]:
            return False
        cur.execute(DDL)
    conn.commit()
    log.info("Created weather.daily (this data dir predates t_2ab1fb0a).")
    return True


def _store_catalogue(conn) -> List[Dict[str, Any]]:
    """Every active store with the latitude and state its weather needs."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT location_id::text AS location_id, state, latitude
            FROM hr.locations
            WHERE is_active = TRUE AND location_type = 'store'
            ORDER BY location_id
        """)
        return [dict(r) for r in cur.fetchall()]


def ensure_day(conn, cfg: Config, when: date) -> int:
    """Upsert one row per store for `when`. Idempotent; returns rows written.

    Called once per simulated day by both the backfill and the realtime path
    BEFORE that day's ticks are written, so the modifier a tick applies and the
    covariate an analyst joins on are the same row. Re-running it on an existing
    day rewrites the identical values (the series is pure), which is what makes
    a re-seeded or gap-filled backfill converge instead of drifting.
    """
    if not cfg.weather.enabled:
        return 0
    rows = []
    for store in _store_catalogue(conn):
        row = daily_weather(store['location_id'], when, cfg,
                            latitude=store.get('latitude'),
                            region_key=store.get('state'))
        if row:
            rows.append(row)
    if not rows:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO weather.daily (
                location_id, weather_date, temp_high_f, temp_low_f,
                precipitation_in, cloud_cover_pct, severity_index, is_severe,
                condition_code, demand_modifier, attendance_modifier, seed_key)
            VALUES %s
            ON CONFLICT (location_id, weather_date) DO UPDATE SET
                temp_high_f         = EXCLUDED.temp_high_f,
                temp_low_f          = EXCLUDED.temp_low_f,
                precipitation_in    = EXCLUDED.precipitation_in,
                cloud_cover_pct     = EXCLUDED.cloud_cover_pct,
                severity_index      = EXCLUDED.severity_index,
                is_severe           = EXCLUDED.is_severe,
                condition_code      = EXCLUDED.condition_code,
                demand_modifier     = EXCLUDED.demand_modifier,
                attendance_modifier = EXCLUDED.attendance_modifier,
                seed_key            = EXCLUDED.seed_key
        """, [
            (r['location_id'], r['weather_date'], r['temp_high_f'], r['temp_low_f'],
             r['precipitation_in'], r['cloud_cover_pct'], r['severity_index'],
             r['is_severe'], r['condition_code'], r['demand_modifier'],
             r['attendance_modifier'], r['seed_key'])
            for r in rows
        ])
    conn.commit()
    return len(rows)


def day_effect(conn, cfg: Config, when: date) -> Optional[Dict[str, Any]]:
    """The store-averaged weather effect for `when`, read back from the table.

    Reading the PERSISTED rows rather than recomputing is deliberate: the
    number that scales a tick's volume and the row a forecasting model joins on
    are then the same number by construction, and a stale or hand-edited row is
    visible to the tick instead of being silently overridden.

    Returns None when the date has no rows — an older data dir, or a day the
    series was disabled for — so the scenario context is left untouched rather
    than forced to a guessed covariate.
    """
    if not cfg.weather.enabled:
        return None
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT severity_index, demand_modifier, attendance_modifier,
                   condition_code, temp_high_f, is_severe
            FROM weather.daily WHERE weather_date = %s
        """, (when,))
        rows = [dict(r) for r in cur.fetchall()]
    if not rows:
        return None

    def _mean(field: str) -> float:
        return sum(float(r[field]) for r in rows) / len(rows)

    # The modal condition, so the day is tagged with what most stores saw.
    counts: Dict[str, int] = {}
    for row in rows:
        counts[row['condition_code']] = counts.get(row['condition_code'], 0) + 1
    condition = max(sorted(counts), key=lambda code: counts[code])

    return {
        'weather_date': when,
        'store_count': len(rows),
        'severity_index': round(_mean('severity_index'), 4),
        'demand_modifier': round(_mean('demand_modifier'), 4),
        'attendance_modifier': round(_mean('attendance_modifier'), 4),
        'mean_temp_high_f': round(_mean('temp_high_f'), 2),
        'condition_code': condition,
        # "Was a storm actually on the shop", as opposed to "was the aggregate
        # demand modifier below 1.0" — a heat wave raises demand, so a day can
        # move the modifier without a single store being severe. The scenario
        # tag needs the former.
        'is_severe_day': sum(1 for r in rows if r['is_severe']) > 0,
        'severe_store_count': sum(1 for r in rows if r['is_severe']),
    }