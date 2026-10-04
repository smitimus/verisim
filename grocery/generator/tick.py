"""The realtime tick: one pass of the simulation clock.

`run_tick` writes one tick's worth of every domain, in the order the tick
lifecycle requires — POS transactions and their inventory depletion, timeclock
events, then the probabilistic events, then the supply chain and the daily
models once the simulated date rolls over. `get_ad_product_prices` and
`_weather_for_tick` are the two covariates it resolves first (the weekly-ad
price of record, and the day's weather).

Split out of `generator/main.py` (t_c2eca5dd); `main.py` re-exports every name.
"""
import logging
import os
import sys
import time
from datetime import date, timedelta
from typing import Dict, Optional

from models import hr, pos, timeclock, ordering, fulfillment, transport, inventory
from models import shrinkage, promotions, scheduling, returns, online, weather, customers
from scenarios.scenario_engine import get_scenario_context, get_active_scenario_names

# This module is loaded two ways: as `grocery.generator.tick` (relative imports
# resolve) and as flat `tick` when main.py runs as `python main.py` (they do not).
# See the long note in `main.py`.
if __package__:
    from .bootstrap import record_stats
    from .volume import compute_online_count, compute_pos_count, realtime_tick_seconds
else:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from bootstrap import record_stats
    from volume import compute_online_count, compute_pos_count, realtime_tick_seconds

log = logging.getLogger('grocery-generator')


# ---------------------------------------------------------------------------
# run_tick
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Main tick (realtime)
# ---------------------------------------------------------------------------

def get_ad_product_prices(conn, sim_date: date) -> Dict[str, float]:
    """product_id -> `promoted_price` for every item on this week's ad.

    The price OF RECORD for an ad item this week: what the shopper actually
    pays, which is what the demand curve must be keyed on. Before
    t_08deeddf this was never read during generation — `ensure_current_ad`
    returned the ad and nobody used it, so a 30%-off item sat at the same
    odds of reaching a basket as a full-price one and the whole weekly-ad
    signal in `mart_promotion_effectiveness` was decoration.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ai.product_id::text, ai.promoted_price
            FROM pricing.ad_items ai
            JOIN pricing.weekly_ads a ON a.ad_id = ai.ad_id
            WHERE a.start_date <= %s AND a.end_date >= %s
              AND ai.promoted_price IS NOT NULL
              AND ai.promoted_price > 0
        """, (sim_date, sim_date))
        return {r[0]: float(r[1]) for r in cur.fetchall()}

def _weather_for_tick(conn, cfg, sim_date: date, weather_cache: Dict[str, object]) -> Optional[dict]:
    """The day's weather effect, generated once and cached (t_2ab1fb0a).

    Realtime writes 2880 ticks a day at the default 30 s cadence, and
    `ensure_day` is a per-store upsert — running it every tick would be 2880
    redundant round trips. The day's row set does not change, so it is written
    once when the date changes and read back from the table for every tick after
    that, which is also what makes the modifier applied to a tick provably the
    same row a forecasting model would join on.

    `weather_cache` is a single mutable dict holding the cached date and effect
    rather than two arguments plus a return value, so the three call sites stay
    one line each. The DB read is cheap and deliberately NOT cached across a
    date change: a fresh day must be re-read after `ensure_day` writes it.
    """
    if not cfg.weather.enabled:
        return None
    if weather_cache.get('date') != sim_date:
        weather.ensure_day(conn, cfg, sim_date)
        weather_cache['date'] = sim_date
        weather_cache['effect'] = None  # force a read-back of the rows just written
    if weather_cache.get('effect') is None:
        weather_cache['effect'] = weather.day_effect(conn, cfg, sim_date)
    return weather_cache.get('effect')



def run_tick(conn, cfg, state, sim_dt, locations, employees, departments,
             products, trucks, members, coupons, deals, weather_cache=None):
    scenario_names = get_active_scenario_names(conn, sim_dt)
    if weather_cache is None:
        weather_cache = {}
    scenario = get_scenario_context(
        scenario_names,
        float(state['volume_multiplier']),
        sim_dt,
        cfg,
        # The synthetic weather covariate, read back from weather.daily (None
        # when the series is off or the day has no rows, in which case the
        # context is left exactly as it was pre-t_2ab1fb0a).
        weather_effect=_weather_for_tick(conn, cfg, sim_dt.date(), weather_cache),
    )
    # Write merged tag back for API display
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE control.generator_state SET active_scenario = %s WHERE state_id = 1",
            (scenario.scenario_tag,)
        )

    tick_seconds = realtime_tick_seconds(cfg, state)
    sim_date = sim_dt.date()
    pos_count = compute_pos_count(cfg, scenario, tick_seconds, sim_date)
    online_count = compute_online_count(cfg, scenario, tick_seconds, sim_date)
    tick_start = time.monotonic()

    # This week's ad prices: the price of record for ad items, and therefore
    # the key of the demand curve for them (t_08deeddf). One read per tick,
    # shared by both channels — the same law must see the same prices.
    ad_prices = get_ad_product_prices(conn, sim_date)

    # Stock-aware capping (t_959cd040). ONE allowance for the whole tick, shared
    # by both channels: POS and online sell the same shelf, so they must draw
    # from one budget or each would be told it could have the last unit. Built
    # before either channel runs, and flushed once both are done — that single
    # flush is what writes inv.stockout_events (the lost sales), decrements
    # inv.stock_levels, and accumulates inv.sku_demand_daily. Without it,
    # depletion floors at zero and the sale books full-quantity revenue for
    # stock that is not there, and reorder_point / restock_threshold_pct never
    # affect anything.
    stock_allowance = inventory.StockAllowance.from_lines(conn, [
        {'location_id': loc['location_id'], 'product_id': product['product_id']}
        for loc in locations['stores']
        for product in products
    ])

    # POS transactions
    depletion = pos.generate_pos_transactions(
        conn, cfg, sim_dt, pos_count, scenario,
        locations['stores'], products, employees, members, coupons, deals,
        ad_prices=ad_prices,
        stock_allowance=stock_allowance,
    )

    # Loyalty signups this batch just wrote are dimensioned here, not at the
    # next restart: `backfill_customers` is the one code path that creates a
    # household, and its candidate set is "cards with a NULL customer_id", so
    # this is a no-op on every tick that signed nobody up. Without it, a card
    # created on day 30 of a run would carry no segment until someone rebooted
    # the generator — a dimension that fills in behind the mart's back.
    customers.backfill_customers(conn, cfg)

    # Timeclock events
    tc_count = timeclock.generate_events(conn, sim_dt, employees, locations)

    # Online orders (e-commerce pickup + delivery) — same shelves as POS, same
    # volume law, its own configured band (`online.orders_per_day`), and the
    # same stock allowance: an online order is filled from the same shelf a POS
    # sale just drew on.
    online_depletion = online.generate_online_orders(
        conn, cfg, sim_dt, online_count, scenario, locations['stores'], products,
        members, ad_prices=ad_prices, stock_allowance=stock_allowance)
    online.advance_online_lifecycle(conn, cfg, sim_dt)

    # Persist the tick's stock outcome: decrement, stockout rows, daily ledger.
    # The written transaction/order ids are passed so a stockout only points at
    # a sale that exists — a basket the shelf could not cover at all writes no
    # sale row, and its walk-away is recorded with no parent.
    stockouts = inventory.flush_sales(
        conn, stock_allowance, sim_dt, scenario.scenario_tag,
        fulfilled_parents=[d['transaction_id'] for d in depletion]
                          + [d['transaction_id'] for d in online_depletion])

    # Probabilistic events
    pos.maybe_update_product_prices(conn, cfg, products, scenario)
    hr.maybe_hire_employee(conn, cfg, locations)
    hr.maybe_terminate_employee(conn)

    # Supply chain + daily models (once per simulated day — check at midnight boundary)
    orders_count = 0
    if sim_dt.hour == 0:
        warehouse_employees = [e for e in employees if e['location_type'] == 'warehouse']
        drivers = [e for e in warehouse_employees if e['department'] == 'transport']
        managers = [e for e in employees if e['department'] == 'management']

        order_ids = ordering.check_and_create_orders(
            conn, locations['stores'], locations['warehouses'], managers, sim_dt,
            scenario, inventory_cfg=cfg.inventory)
        orders_count = len(order_ids)

        fulfilled = fulfillment.process_pending_orders(conn, warehouse_employees, sim_dt)

        if fulfilled and locations['warehouses'] and trucks:
            wh_loc_id = locations['warehouses'][0]['location_id']
            transport.dispatch_loads(conn, fulfilled, trucks, drivers, wh_loc_id, sim_dt, scenario)

        transport.receive_delivered_loads(conn, sim_dt, scenario)

        # Phase 2: perishable expiry dates + shrinkage
        shrinkage.set_expiry_dates(conn, sim_dt)
        shrinkage.generate_shrinkage_events(conn, sim_dt, locations['stores'], scenario)

        # Phase 3: weekly ad lifecycle
        promotions.expire_old_ads(conn, sim_dt.date())
        promotions.ensure_current_ad(conn, sim_dt.date(), products)

        # Phase 4: labor scheduling (generate next week) + resolve yesterday's actuals
        scheduling.resolve_schedule_actuals(conn, sim_dt.date(), scenario)
        scheduling.generate_weekly_schedule(conn, sim_dt.date(), locations, employees, scenario)
        # Yesterday's shifts, so they are resolved under YESTERDAY's weather —
        # `attendance_modifier` is a per-day covariate now (t_2ab1fb0a), and
        # scoring yesterday's attendance with today's storm would write the
        # call-outs on the wrong shift rows. Read yesterday's own effect rather
        # than reusing this tick's; a day with no rows (the series was off, or
        # the row predates the backfill) leaves the shift resolution exactly as
        # it was pre-t_2ab1fb0a.
        yesterday_ctx = scenario
        if cfg.weather.enabled:
            yesterday = sim_dt.date() - timedelta(days=1)
            yesterday_effect = weather.day_effect(conn, cfg, yesterday)
            if yesterday_effect:
                yesterday_ctx = get_scenario_context(
                    get_active_scenario_names(conn, sim_dt),
                    1.0, sim_dt, cfg,
                    weather_effect=yesterday_effect,
                )
        scheduling.resolve_schedule_actuals(conn, sim_dt.date(), yesterday_ctx)

        # Phase 5: coupon + combo deal lifecycle — deactivate expired, top up
        # active set so the API always serves a current batch (freshness
        # contract with data-lab: raw_pos.combo_deals went STALE when deals
        # expired 2026-08-09 and nothing re-seeded them). Then reconcile the
        # promo rows against the redemptions actually on disk: windows that
        # pass a transaction outside them and a uses_count nobody maintained
        # are both source bugs data-lab asserts against.
        history_days = getattr(cfg.generator, 'backfill_lookback_days', 30)
        pos.seed_coupons(conn, cfg, departments, products, history_days)
        pos.seed_combo_deals(conn, cfg, departments, products, history_days)
        pos.reconcile_promotions(conn)

        # Phase 6: customer returns & refunds for transactions aged 2-14 days
        # (reversals + stock reintegration)
        restock = returns.generate_returns(conn, cfg, sim_dt, scenario)
        if restock:
            returns.restock_returns(conn, restock)

    elapsed_ms = round((time.monotonic() - tick_start) * 1000)
    record_stats(conn, pos_count, tc_count, orders_count,
                 scenario.scenario_tag, sim_dt, elapsed_ms)
    log.info("Tick done %dms | POS: %d | Online: %d | TC: %d | Orders: %d | Stockouts: %d | Scenario: %s",
             elapsed_ms, pos_count, online_count, tc_count, orders_count,
             stockouts, scenario.scenario_tag)

