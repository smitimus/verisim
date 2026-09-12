"""
Scenario engine — determines the active generation context for each tick
in the customer-support industry.

Support scenarios are contact SURGES and sentiment events, not retail demand:
  - volume_multiplier  : scales tickets/calls/chats created this tick
  - call_stress        : scales queue waits + abandonment
  - talk_multiplier    : scales handle time
  - sentiment_shift    : 0..1, pushes surveys toward detractors and tickets
                         toward negative sentiment / escalations
  - scenario_tag       : written to every record

Automatic calendar (stacks on manual scenarios):
  - tax_week (Apr 1-15): billing surge
  - holiday_week (Dec 18-31): shipping/returns surge
  - back_to_school (Aug 20 - Sep 10): account/technical surge
"""
from dataclasses import dataclass, field
from typing import List, Tuple
from datetime import datetime, date

import psycopg2.extras

from config import Config


# ---------------------------------------------------------------------------
# DB helpers — fetch active scenario names for a given sim_dt
# ---------------------------------------------------------------------------

def get_active_scenario_names(conn, sim_dt: datetime) -> List[str]:
    sim_date = sim_dt.date()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT scenario_name FROM control.active_scenarios ORDER BY activated_at")
        manual = [r['scenario_name'] for r in cur.fetchall()]
        cur.execute("""
            SELECT DISTINCT scenario_name
            FROM control.scenario_schedules
            WHERE start_date <= %s AND end_date >= %s
            ORDER BY scenario_name
        """, (sim_date, sim_date))
        scheduled = [r['scenario_name'] for r in cur.fetchall()]
    seen, result = set(), []
    for name in scheduled + manual:
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result if result else ['normal']


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------

@dataclass
class ScenarioContext:
    volume_multiplier: float = 1.0
    call_stress: float = 1.0
    talk_multiplier: float = 1.0
    sentiment_shift: float = 0.0
    scenario_tag: str = 'normal'


def _apply_single_scenario(name: str, cfg: Config, ctx: ScenarioContext) -> None:
    s = cfg.scenarios
    if name == 'service_outage':
        ctx.volume_multiplier *= s.outage_volume_multiplier
        ctx.call_stress *= 1.8
        ctx.talk_multiplier *= 1.35
        ctx.sentiment_shift = max(ctx.sentiment_shift, s.outage_sentiment_shift)
    elif name == 'weather_outage':
        ctx.volume_multiplier *= s.weather_outage_multiplier
        ctx.call_stress *= 1.5
        ctx.sentiment_shift = max(ctx.sentiment_shift, s.outage_sentiment_shift * 0.6)
    elif name == 'product_launch':
        ctx.volume_multiplier *= s.product_launch_multiplier
        ctx.talk_multiplier *= 1.15
        ctx.sentiment_shift = max(ctx.sentiment_shift, 0.10)
    elif name == 'marketing_blast':
        ctx.volume_multiplier *= s.marketing_blast_multiplier
        ctx.call_stress *= 1.2
    elif name == 'holiday_week':
        ctx.volume_multiplier *= s.holiday_multiplier
        ctx.call_stress *= 1.25
    elif name == 'weekend':
        ctx.volume_multiplier *= s.weekend_multiplier
    # 'normal', 'rush_hour' handled elsewhere (rush_hour is hourly-automatic)


def _calendar_multiplier(sim_dt: datetime) -> Tuple[float, str, float]:
    """Automatic contact-center seasonality -> (mult, tag, sentiment_shift)."""
    d = sim_dt.date()
    if d.month == 4 and d.day <= 15:
        return 1.5, 'tax_week', 0.05
    if d.month == 12 and d.day >= 18:
        return 1.6, 'holiday_week', 0.10
    if d.month == 1 and d.day <= 10:
        return 1.5, 'returns_wave', 0.08
    if d.month == 8 and d.day >= 20 or (d.month == 9 and d.day <= 10):
        return 1.25, 'back_to_school', 0.0
    return 1.0, '', 0.0


def get_scenario_context(scenario_names: List[str],
                         volume_multiplier_override: float,
                         simulation_dt: datetime,
                         cfg: Config) -> ScenarioContext:
    ctx = ScenarioContext()

    active = [s for s in scenario_names if s != 'normal']
    if not active:
        active = ['normal']
    for name in active:
        _apply_single_scenario(name, cfg, ctx)
    ctx.volume_multiplier *= volume_multiplier_override

    non_normal = [s for s in active if s != 'normal']
    ctx.scenario_tag = '+'.join(non_normal) if non_normal else 'normal'

    cal_mult, cal_tag, cal_sent = _calendar_multiplier(simulation_dt)
    if cal_mult > 1.0:
        ctx.volume_multiplier *= cal_mult
        ctx.sentiment_shift = max(ctx.sentiment_shift, cal_sent)
        ctx.scenario_tag = (cal_tag if ctx.scenario_tag == 'normal'
                            else f'{ctx.scenario_tag}+{cal_tag}')

    # Support peaks at business hours — stack rush multiplier
    if simulation_dt.hour in cfg.scenarios.rush_hour_hours:
        ctx.volume_multiplier *= cfg.scenarios.rush_hour_multiplier
        ctx.scenario_tag = ('rush_hour' if ctx.scenario_tag == 'normal'
                            else f'{ctx.scenario_tag}+rush_hour')

    # Hourly weight normalization (weights sum to 1 over the day)
    hour_weight = cfg.volumes.hourly_weights[simulation_dt.hour]
    ctx.volume_multiplier *= (hour_weight * 24)

    # Day-of-week multiplier
    dow = simulation_dt.strftime('%A').lower()
    ctx.volume_multiplier *= cfg.volumes.day_of_week_multipliers.get(dow, 1.0)

    return ctx
