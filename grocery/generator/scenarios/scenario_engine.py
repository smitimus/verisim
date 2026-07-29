"""
Scenario engine — determines the active generation context for each tick.

ScenarioContext bundles:
  - volume_multiplier    : applied to base transaction counts
  - active_promotions    : list of (department_name, discount_pct)
  - coupon_multiplier    : scalar applied to coupon savings (double coupon events)
  - scenario_tag         : written to every transaction for downstream analysis

Multiple scenarios can be active simultaneously. Their effects are merged:
  - volume_multiplier : multiplicative across all active scenarios
  - active_promotions : union of all departments
  - coupon_multiplier : max across all active scenarios
  - scenario_tag      : joined with '+' (e.g. 'promotion+holiday_week')
"""
from dataclasses import dataclass, field
from typing import List, Tuple, Dict
from datetime import datetime, date

import psycopg2.extras

from config import Config


# ---------------------------------------------------------------------------
# Automatic seasonal calendar
# ---------------------------------------------------------------------------

def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    """Return the nth occurrence of weekday (0=Mon … 6=Sun) in a given month."""
    count = 0
    for day in range(1, 32):
        try:
            d = date(year, month, day)
        except ValueError:
            break
        if d.weekday() == weekday:
            count += 1
            if count == n:
                return d
    raise ValueError(f"No {n}th weekday {weekday} in {year}-{month:02d}")


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of weekday in a given month."""
    result = None
    for day in range(1, 32):
        try:
            d = date(year, month, day)
        except ValueError:
            break
        if d.weekday() == weekday:
            result = d
    return result


def _get_holiday_multiplier(sim_dt: datetime) -> Tuple[float, str]:
    """
    Returns (multiplier, tag) for automatic US grocery seasonal events.
    Stacks on top of whatever manual scenario is active.
    Returns (1.0, '') when no holiday applies.
    """
    d = sim_dt.date()
    y, m, day = d.year, d.month, d.day

    # Thanksgiving week (4th Thursday of November ± 3 days) — biggest grocery week
    try:
        thanksgiving = _nth_weekday_of_month(y, 11, 3, 4)
        if abs((d - thanksgiving).days) <= 3:
            return 2.0, 'thanksgiving_week'
    except ValueError:
        pass

    # Christmas Eve & Day (Dec 23–25)
    if m == 12 and day in (23, 24, 25):
        return 1.9, 'christmas'

    # Pre-Christmas shopping week (Dec 20–22)
    if m == 12 and 20 <= day <= 22:
        return 1.6, 'pre_christmas'

    # New Year's Eve (Dec 31)
    if m == 12 and day == 31:
        return 1.6, 'new_years_eve'

    # Super Bowl Sunday (1st Sunday of February)
    try:
        super_bowl = _nth_weekday_of_month(y, 2, 6, 1)
        if d == super_bowl:
            return 1.7, 'super_bowl'
    except ValueError:
        pass

    # Memorial Day weekend (last Monday of May ± 2 days)
    try:
        mem_day = _last_weekday_of_month(y, 5, 0)
        if mem_day and abs((d - mem_day).days) <= 2:
            return 1.4, 'memorial_day_weekend'
    except ValueError:
        pass

    # Independence Day (Jul 3–4)
    if m == 7 and day in (3, 4):
        return 1.5, 'independence_day'

    # Labor Day weekend (1st Monday of September ± 2 days)
    try:
        labor_day = _nth_weekday_of_month(y, 9, 0, 1)
        if labor_day and abs((d - labor_day).days) <= 2:
            return 1.4, 'labor_day_weekend'
    except ValueError:
        pass

    # Halloween (Oct 30–31) — candy season
    if m == 10 and day in (30, 31):
        return 1.3, 'halloween'

    return 1.0, ''


# ---------------------------------------------------------------------------
# DB helpers — fetch active scenarios for a given sim_dt
# ---------------------------------------------------------------------------

def get_active_scenario_names(conn, sim_dt: datetime) -> List[str]:
    """
    Return the combined list of manually activated scenarios plus any
    scenario_schedules that cover sim_dt's date.
    """
    sim_date = sim_dt.date()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT scenario_name FROM control.active_scenarios ORDER BY activated_at")
        manual = [r['scenario_name'] for r in cur.fetchall()]

        cur.execute(
            """
            SELECT DISTINCT scenario_name
            FROM control.scenario_schedules
            WHERE start_date <= %s AND end_date >= %s
            ORDER BY scenario_name
            """,
            (sim_date, sim_date),
        )
        scheduled = [r['scenario_name'] for r in cur.fetchall()]

    # Merge: scheduled first, then manual (manual overrides if duplicate)
    seen = set()
    result = []
    for name in scheduled + manual:
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result if result else ['normal']


# ---------------------------------------------------------------------------
# Main scenario context
# ---------------------------------------------------------------------------

@dataclass
class ScenarioContext:
    volume_multiplier: float = 1.0
    active_promotions: List[Tuple[str, float]] = field(default_factory=list)
    coupon_multiplier: float = 1.0
    scenario_tag: str = 'normal'

    # Cross-domain effect modifiers (verisim#12)
    price_modifier: float = 1.0              # scale price change magnitude (inflation=1.15)
    attendance_modifier: float = 1.0          # scale timeclock attendance rate (weather=0.75)
    supply_disruption: bool = False           # trigger ordering shortfalls + transport delays
    labor_multiplier: float = 1.0             # scale scheduled headcount (holiday=1.2)
    shrinkage_modifier: float = 1.0           # scale shrinkage event rate (supplier_disruption=1.3)
    loyalty_engagement_modifier: float = 1.0  # scale coupon/deal usage probability
    per_store_multipliers: Dict[str, float] = field(default_factory=dict)  # per-store volume overrides


def _apply_single_scenario(
    scenario_name: str,
    volume_multiplier_override: float,
    cfg: Config,
    ctx: ScenarioContext,
) -> None:
    """Merge a single scenario's effects into ctx in-place."""
    if scenario_name == 'promotion':
        ctx.volume_multiplier *= volume_multiplier_override
        ctx.labor_multiplier = max(ctx.labor_multiplier, cfg.scenarios.promotion_labor_multiplier)
        for dept in cfg.scenarios.promotion_departments:
            if not any(p[0] == dept for p in ctx.active_promotions):
                ctx.active_promotions.append((dept, cfg.scenarios.promotion_discount_pct))

    elif scenario_name == 'holiday_week':
        ctx.volume_multiplier *= volume_multiplier_override * cfg.scenarios.holiday_week_multiplier
        ctx.labor_multiplier = max(ctx.labor_multiplier, cfg.scenarios.holiday_labor_multiplier)
        ctx.coupon_multiplier = max(ctx.coupon_multiplier, 1.5)

    elif scenario_name == 'double_coupons':
        ctx.volume_multiplier *= volume_multiplier_override
        ctx.coupon_multiplier = max(ctx.coupon_multiplier, cfg.scenarios.double_coupon_multiplier)
        ctx.loyalty_engagement_modifier = max(ctx.loyalty_engagement_modifier, 1.5)

    elif scenario_name == 'weekend':
        ctx.volume_multiplier *= volume_multiplier_override * cfg.scenarios.weekend_multiplier
        ctx.labor_multiplier = max(ctx.labor_multiplier, cfg.scenarios.weekend_labor_multiplier)

    elif scenario_name == 'inflation_pressure':
        ctx.price_modifier = max(ctx.price_modifier, cfg.scenarios.inflation_price_modifier)
        ctx.loyalty_engagement_modifier = min(ctx.loyalty_engagement_modifier, cfg.scenarios.inflation_loyalty_modifier)

    elif scenario_name == 'severe_weather':
        ctx.volume_multiplier *= cfg.scenarios.weather_volume_multiplier
        ctx.attendance_modifier = min(ctx.attendance_modifier, cfg.scenarios.weather_attendance_modifier)
        ctx.supply_disruption = True

    elif scenario_name == 'supplier_disruption':
        ctx.supply_disruption = True
        ctx.shrinkage_modifier = max(ctx.shrinkage_modifier, cfg.scenarios.supply_disruption_shrinkage_modifier)

    elif scenario_name == 'regional_peak':
        ctx.per_store_multipliers = {**ctx.per_store_multipliers, **cfg.scenarios.regional_peak_stores}

    elif scenario_name == 'deep_discount':
        ctx.price_modifier = min(ctx.price_modifier, cfg.scenarios.deep_discount_price_modifier)
        ctx.loyalty_engagement_modifier = max(ctx.loyalty_engagement_modifier, 1.3)

    else:
        # 'normal', 'rush_hour', or unknown — apply override only
        ctx.volume_multiplier *= volume_multiplier_override


def get_scenario_context(
    scenario_names: List[str],
    volume_multiplier_override: float,
    simulation_dt: datetime,
    cfg: Config,
) -> ScenarioContext:
    """
    Build a ScenarioContext for this tick from a list of active scenario names.
    Scenarios are merged multiplicatively for volume; promotions are unioned.
    """
    ctx = ScenarioContext()

    # Filter out 'normal' before processing; add back if nothing else present
    active = [s for s in scenario_names if s != 'normal']
    if not active:
        active = ['normal']

    for name in active:
        _apply_single_scenario(name, volume_multiplier_override, cfg, ctx)

    # Build scenario tag from all non-normal scenarios
    non_normal = [s for s in active if s != 'normal']
    ctx.scenario_tag = '+'.join(non_normal) if non_normal else 'normal'

    # Automatic seasonal calendar — stacks on top of manual scenarios
    holiday_mult, holiday_tag = _get_holiday_multiplier(simulation_dt)
    if holiday_mult > 1.0:
        ctx.volume_multiplier *= holiday_mult
        if ctx.scenario_tag == 'normal':
            ctx.scenario_tag = holiday_tag
        else:
            ctx.scenario_tag = f'{ctx.scenario_tag}+{holiday_tag}'

    # Rush hour stacks on top
    if simulation_dt.hour in cfg.scenarios.rush_hour_hours:
        ctx.volume_multiplier *= cfg.scenarios.rush_hour_multiplier
        if ctx.scenario_tag == 'normal':
            ctx.scenario_tag = 'rush_hour'
        else:
            ctx.scenario_tag = f'{ctx.scenario_tag}+rush_hour'

    # Hourly weight normalisation
    hour_weight = cfg.volumes.hourly_weights[simulation_dt.hour]
    ctx.volume_multiplier *= (hour_weight * 24)

    # Day-of-week multiplier
    dow = simulation_dt.strftime('%A').lower()
    ctx.volume_multiplier *= cfg.volumes.day_of_week_multipliers.get(dow, 1.0)

    return ctx
