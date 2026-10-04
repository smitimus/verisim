"""Backfill: writing the past, day by simulated hour.

`auto_backfill_if_fresh` starts a 30-day backfill on an empty database,
`run_backfill` writes it, and `has_data_for_date` / `_ensure_realtime` are the
gap and hand-off helpers. Backfill is gap-aware and idempotent: it skips a date
that already has POS data, treats the current day as partial (hours 0 ->
current hour, then one final tick at `now()` so realtime picks up with no gap),
and resolves yesterday's shifts under yesterday's weather — the same shape the
realtime midnight path uses, which is why `_weather_for_tick` is shared with
`tick.py`.

Split out of `generator/main.py` (t_c2eca5dd); `main.py` re-exports every name.
"""
import logging
import os
import sys
from datetime import date, datetime, timedelta

from models import hr, pos, timeclock, ordering, fulfillment, transport, inventory
from models import shrinkage, promotions, scheduling, returns, online, weather, customers
from scenarios.scenario_engine import get_scenario_context, get_active_scenario_names

# Loaded both as `grocery.generator.backfill` and as flat `backfill` when main.py
# runs as a script. See the long note in `main.py`.
if __package__:
    from .bootstrap import record_stats
    from .volume import (
        SIM_HOUR_SECONDS,
        compute_online_count,
        compute_pos_count,
        realtime_tick_seconds,
    )
    from .tick import _weather_for_tick, get_ad_product_prices
else:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from bootstrap import record_stats
    from volume import (
        SIM_HOUR_SECONDS,
        compute_online_count,
        compute_pos_count,
        realtime_tick_seconds,
    )
    from tick import _weather_for_tick, get_ad_product_prices

log = logging.getLogger('grocery-generator')


# ---------------------------------------------------------------------------
# gap detection + auto-start
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Backfill helpers
# ---------------------------------------------------------------------------

def has_data_for_date(conn, check_date):
    """Return True if POS transactions exist for check_date."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pos.transactions WHERE transaction_dt::date = %s LIMIT 1",
            (check_date,)
        )
        return cur.fetchone() is not None


def auto_backfill_if_fresh(conn, cfg):
    """
    Called once after seed_all(). Handles two scenarios:

    1. Empty DB: configure a 30-day backfill (today-30 → today)
    2. DB has data with gaps in the last 30 days: detect gaps, configure
       a targeted backfill to fill only the missing days, then transition to
       realtime once complete.

    Does nothing if all 30 days are covered — just stays running (or
    transitions to realtime if not already configured).

    Idempotent and safe to call on every startup.
    """
    today = date.today()
    lookback_days = getattr(cfg.generator, 'backfill_lookback_days', 30)
    lookback = today - timedelta(days=lookback_days)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT min(transaction_dt::date), count(*) FROM pos.transactions"
        )
        row = cur.fetchone()
        if row[1] == 0:
            # Empty DB — full fresh backfill
            log.info("Fresh database — configuring 30-day backfill: %s → %s",
                     lookback, today)
            with conn.cursor() as cur2:
                cur2.execute("""
                    UPDATE control.generator_state SET
                        mode = 'backfill',
                        is_running = TRUE,
                        is_paused = FALSE,
                        backfill_start_date = %s,
                        backfill_end_date = %s,
                        backfill_current_date = %s,
                        started_at = NOW(), updated_at = NOW()
                    WHERE state_id = 1
                """, (lookback, today, lookback))
            conn.commit()
            return

        # Data exists — check for gaps in the last 30 days using max timestamps.
        effective_start = min(row[0] if row[0] else today, lookback)
        effective_end = max(today, row[1] and (today))

        # Get max transaction timestamp per day in the active range.
        # A "complete" day has data reaching the end of the last open hour.
        # The store is now 24/7 — last valid hour is 23.
        cur.execute("""
            SELECT transaction_dt::date AS d, 
                   max(transaction_dt)::timestamp WITHOUT TIME ZONE AS last_txn
            FROM pos.transactions
            WHERE transaction_dt::date BETWEEN %s AND %s
            GROUP BY 1
        """, (effective_start, effective_end))
        by_date = {r[0]: r[1] for r in cur.fetchall() if r}

        # Also get the full date series so we don't miss completely empty days
        cur.execute("""
            SELECT generate_series(%s::date, %s::date, '1 day'::interval)::date AS d
        """, (effective_start, effective_end))
        all_dates = [r[0] for r in cur.fetchall()]

        # A day is "covered" if its last_txn reaches the last open hour (22).
        # Any day with last_txn before that is incomplete — needs resumption.
        missing_dates = []
        for d in all_dates:
            if d not in by_date:
                missing_dates.append(d)
                continue
            last_txn = by_date[d]
            if last_txn.hour >= 23:
                continue  # complete
            missing_dates.append(d)

        if not missing_dates:
            # No gaps — ensure we're in realtime if currently stopped/paused
            already_configured = _ensure_realtime(conn)
            if already_configured:
                return
            with conn.cursor() as cur2:
                cur2.execute("UPDATE control.generator_state SET mode = 'realtime', "
                             "is_running = TRUE, backfill_start_date = NULL, "
                             "backfill_end_date = NULL, backfill_current_date = NULL "
                             "WHERE state_id = 1")
            conn.commit()
            return

        # Gaps found — configure a targeted backfill to fill only missing days
        missing_dates = sorted(missing_dates)
        log.info("Detected %d incomplete/missing day(s) in the last 30 days: %s through %s",
                 len(missing_dates), missing_dates[0], missing_dates[-1])

        with conn.cursor() as cur2:
            # Set backfill range to cover all gaps, even if data is non-contiguous
            cur2.execute("""
                UPDATE control.generator_state SET
                    mode = 'backfill',
                    is_running = TRUE,
                    is_paused = FALSE,
                    backfill_start_date = %s,
                    backfill_end_date = %s,
                    backfill_current_date = %s,
                    started_at = NOW(), updated_at = NOW()
                WHERE state_id = 1
            """, (min(missing_dates), max(missing_dates), min(missing_dates)))
        conn.commit()


def _ensure_realtime(conn):
    """Return True if a backfill was already in progress."""
    with conn.cursor() as cur:
        cur.execute("SELECT mode, is_running FROM control.generator_state WHERE state_id = 1")
        row = cur.fetchone()
        if row and row[0] == 'backfill' and row[1]:
            return True
    return False


# ---------------------------------------------------------------------------
# run_backfill
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Backfill mode
# ---------------------------------------------------------------------------

def run_backfill(conn, cfg, state, locations, employees, departments,
                 products, trucks, members, coupons, deals):
    start = state['backfill_start_date']
    end = state['backfill_end_date']
    current = state['backfill_current_date'] or start
    today = date.today()
    now_dt = datetime.now()

    log.info("Starting backfill %s → %s (resuming from %s)", start, end, current)

    warehouse_employees = [e for e in employees if e['location_type'] == 'warehouse']
    drivers = [e for e in warehouse_employees if e['department'] == 'transport']
    managers = [e for e in employees if e['department'] == 'management']

    cur_date = current
    while cur_date <= end:
        # Re-read now_dt each day so the partial-day cutoff stays current
        # for slow backfills that span midnight.
        now_dt = datetime.now()
        today = date.today()

        # The day's weather, written BEFORE any of its hours (t_2ab1fb0a).
        # One upsert per day rather than one per simulated hour, and a re-run
        # of a resumed day rewrites the identical values because the series is a
        # pure function of (store, date) — so a gap-filled backfill converges
        # instead of accumulating a second, different weather history.
        weather_cache = {}
        if cfg.weather.enabled:
            weather.ensure_day(conn, cfg, cur_date)

        # For past dates, check if this day is fully generated using max txn timestamp.
        # Resume from the hour AFTER the last recorded transaction instead of hour 0.
        skip_day = False
        start_hour = 0  # default: regenerate full day from hour 0

        if cur_date != today:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT max(transaction_dt)::timestamp FROM pos.transactions "
                    "WHERE transaction_dt::date = %s", (cur_date,)
                )
                row = cur.fetchone()
            last_txn_time = row[0]

            if last_txn_time is None:
                # No data at all — generate the full day (start_hour stays 0)
                pass
            elif last_txn_time.hour >= 23:
                # Fully generated — last open hour (22) has data
                log.info("Skipping %s — already complete (last txn %s)",
                         cur_date, last_txn_time)
                skip_day = True
            else:
                # Partial day — resume from the hour AFTER the last recorded txn
                resuming_from = last_txn_time.replace(minute=0, second=0) + timedelta(hours=1)
                start_hour = min(resuming_from.hour, 24)
                log.info("%s has partial data (last txn %s), regenerating from hour %d",
                         cur_date, last_txn_time, start_hour)

        if skip_day:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE control.generator_state
                    SET backfill_current_date = %s, updated_at = NOW()
                    WHERE state_id = 1
                """, (cur_date + timedelta(days=1),))
            conn.commit()
            cur_date += timedelta(days=1)
            continue

        # Partial day: today gets hours start_hour → current_hour; no end-of-day
        # events (they will run tonight via the realtime midnight handler).
        is_partial = (cur_date == today)
        end_hour = now_dt.hour if is_partial else 23

        log.info("Backfilling %s%s (hours %d–%d)",
                 cur_date, " [partial — up to current hour]" if is_partial else "", start_hour, end_hour)

        for hour in range(start_hour, end_hour + 1):
            # For the last hour of a partial day, use the exact current time so
            # the final backfill tick aligns with where realtime picks up.
            # All previous hours use the hour boundary (e.g. 13:00:00).
            if is_partial and hour == end_hour:
                sim_dt = datetime.now()
            else:
                sim_dt = datetime(cur_date.year, cur_date.month, cur_date.day, hour, 0, 0)
            scenario_names = get_active_scenario_names(conn, sim_dt)
            scenario = get_scenario_context(
                scenario_names,
                float(state['volume_multiplier']),
                sim_dt, cfg,
                # Read back from the rows `ensure_day` wrote for cur_date, so
                # a backfilled hour sees the same weather a realtime hour on
                # that date would — the whole reason the series is pure.
                weather_effect=_weather_for_tick(conn, cfg, cur_date, weather_cache),
            )
            # One tick per simulated hour here, so the whole hour's demand is
            # written at once — the same law realtime applies 120 times an hour,
            # against the same date-keyed daily budget, so both paths write the
            # same day at the same volume (t_94bbf1ce, t_eb31c99f).
            pos_count = compute_pos_count(cfg, scenario, SIM_HOUR_SECONDS, cur_date)
            online_count = compute_online_count(cfg, scenario, SIM_HOUR_SECONDS, cur_date)
            # The ad in force for the BACKFILLED date, not today's: a backfill
            # hour must see the price of record of the day it is writing, or
            # the ad signal lands on the wrong dates (t_08deeddf).
            ad_prices = get_ad_product_prices(conn, cur_date)

            # Same one-allowance-per-tick rule as the realtime path: the whole
            # simulated hour's POS + online demand is resolved against one
            # snapshot of the shelf, then flushed together (t_959cd040).
            stock_allowance = inventory.StockAllowance.from_lines(conn, [
                {'location_id': loc['location_id'],
                 'product_id': product['product_id']}
                for loc in locations['stores']
                for product in products
            ])

            depletion = pos.generate_pos_transactions(
                conn, cfg, sim_dt, pos_count, scenario,
                locations['stores'], products, employees, members, coupons, deals,
                ad_prices=ad_prices,
                stock_allowance=stock_allowance,
            )

            # Online orders for this hour (placed + lifecycle advanced in
            # time order so statuses converge realistically).
            online_depletion = online.generate_online_orders(
                conn, cfg, sim_dt, online_count, scenario, locations['stores'],
                products, members, ad_prices=ad_prices,
                stock_allowance=stock_allowance)
            online.advance_online_lifecycle(conn, cfg, sim_dt)

            # Decrement the shelf, write the lost sales, update the ledger.
            inventory.flush_sales(
                conn, stock_allowance, sim_dt, scenario.scenario_tag,
                fulfilled_parents=[d['transaction_id'] for d in depletion]
                                  + [d['transaction_id'] for d in online_depletion])

            # Partial day: generate timeclock events per-hour using the same
            # idempotent realtime logic (checks existing events before inserting).
            if is_partial:
                tc_count = timeclock.generate_events(conn, sim_dt, employees, locations)
            else:
                # A full day writes its whole shift schedule in one pass after the
                # hourly loop, so the per-hour timeclock count is only meaningful on a
                # partial day. Report 0 rather than a planned number it never wrote.
                tc_count = 0

            # The tick ledger (t_ac80c514). The backfill writes one row per simulated
            # hour, exactly as the realtime tick loop does, so a consumer asking "was the
            # generator up, and under what regime, when these sales were written?" has
            # an answer for every day the backfill produced — including the 30-day
            # window of a fresh install, which used to exist in the fact tables with no
            # telemetry at all. The holiday/rush-hour tag is `scenario.scenario_tag`,
            # the same context this hour's transactions were generated under, so the tag
            # cannot drift from the rows it describes.
            #
            # Counts are what landed (`len(depletion)`), not `pos_count`: a
            # stockout-capped hour (t_959cd040) writes fewer rows than it planned for.
            record_stats(conn, len(depletion), tc_count, len(online_depletion),
                         scenario.scenario_tag, sim_dt, 0, bump_state_clock=False)

        if not is_partial:
            # Full day: run all end-of-day events in one pass.
            timeclock.generate_day_events(conn, cur_date, employees)

            # Compute scenario context for end-of-day models using a
            # representative time (11pm of the current backfill day).
            eod_dt = datetime(cur_date.year, cur_date.month, cur_date.day, 23, 0)
            eod_scenario_names = get_active_scenario_names(conn, eod_dt)
            eod_scenario = get_scenario_context(
                eod_scenario_names,
                1.0,
                eod_dt, cfg,
                # The SAME day's weather the hourly loop above sampled, so
                # end-of-day models (scheduling, shrinkage, transport) see the
                # covariate the day's transactions were written under.
                weather_effect=_weather_for_tick(conn, cfg, cur_date, weather_cache),
            )

            order_ids = ordering.check_and_create_orders(
                conn, locations['stores'], locations['warehouses'], managers,
                datetime(cur_date.year, cur_date.month, cur_date.day, 22, 0),
                eod_scenario, inventory_cfg=cfg.inventory)
            fulfilled = fulfillment.process_pending_orders(conn, warehouse_employees,
                datetime(cur_date.year, cur_date.month, cur_date.day, 23, 0))
            if fulfilled and locations['warehouses'] and trucks:
                wh_loc_id = locations['warehouses'][0]['location_id']
                transport.dispatch_loads(conn, fulfilled, trucks, drivers, wh_loc_id,
                    datetime(cur_date.year, cur_date.month, cur_date.day, 23, 30),
                    eod_scenario)
            transport.receive_delivered_loads(conn,
                datetime(cur_date.year, cur_date.month, cur_date.day, 23, 59),
                eod_scenario)

            pos.maybe_update_product_prices(conn, cfg, products, eod_scenario)

            sim_day_end = datetime(cur_date.year, cur_date.month, cur_date.day, 23, 59)
            shrinkage.set_expiry_dates(conn, sim_day_end)
            shrinkage.generate_shrinkage_events(conn, sim_day_end, locations['stores'], eod_scenario)
            promotions.expire_old_ads(conn, cur_date)
            promotions.ensure_current_ad(conn, cur_date, products)
            scheduling.resolve_schedule_actuals(conn, cur_date, eod_scenario)
            scheduling.generate_weekly_schedule(conn, cur_date, locations, employees, eod_scenario)
            # Yesterday's shifts resolved under YESTERDAY's weather, exactly as
            # the realtime midnight path does (t_2ab1fb0a). Inside a backfilled
            # day the wrong day's storm here would be invisible in the day's own
            # transactions, and still put the call-outs on the wrong rows.
            prev_effect = (weather.day_effect(conn, cfg, cur_date - timedelta(days=1))
                           if cfg.weather.enabled else None)
            prev_ctx = get_scenario_context(
                eod_scenario_names, 1.0, eod_dt, cfg,
                weather_effect=prev_effect or weather_cache.get('effect'),
            )
            scheduling.resolve_schedule_actuals(conn, cur_date, prev_ctx)

            # Coupon + combo deal lifecycle (same as realtime Phase 5) —
            # a 30-day backfill must not leave deals expired at the end.
            history_days = getattr(cfg.generator, 'backfill_lookback_days', 30)
            pos.seed_coupons(conn, cfg, departments, products, history_days)
            pos.seed_combo_deals(conn, cfg, departments, products, history_days)
            pos.reconcile_promotions(conn)

            # Phase 6: returns for transactions aged 2-14 days relative to
            # this backfill day (same contract as realtime).
            restock = returns.generate_returns(conn, cfg,
                datetime(cur_date.year, cur_date.month, cur_date.day, 23, 30),
                eod_scenario)
            if restock:
                returns.restock_returns(conn, restock)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE control.generator_state
                SET backfill_current_date = %s, updated_at = NOW()
                WHERE state_id = 1
            """, (cur_date + timedelta(days=1),))
        conn.commit()
        cur_date += timedelta(days=1)

    # The last (partial) backfill day skips the day-end block above, so its
    # redemptions would stay uncounted until the next simulated midnight.
    # Reconcile once here so a freshly loaded database reads exactly correct.
    pos.reconcile_promotions(conn)

    # Dimension every loyalty card the backfill just wrote, in ONE pass over
    # the whole set — deliberately NOT per day. `plan_households` groups cards
    # by walking them in signup order and deciding, per card, whether it joins
    # the household already open; run per backfill day instead, the walk would
    # restart at each day's boundary and the second adult's card — signed up on
    # a later day than the first — would never be considered as sitting next to
    # its partner. A 30-day backfill would then produce ~30 single-card
    # households instead of the intended mix, and the segment mix would depend
    # on where the backfill happened to be cut. It is idempotent and cheap (it
    # no-ops the moment nothing has a NULL customer_id), so running it here as
    # well as per-tick in realtime is safe.
    customers.backfill_customers(conn, cfg)

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE control.generator_state
            SET mode = 'realtime', is_running = TRUE,
                backfill_start_date = NULL, backfill_end_date = NULL,
                backfill_current_date = NULL, updated_at = NOW()
            WHERE state_id = 1
        """)
    conn.commit()
    log.info("Backfill complete — transitioning to realtime.")

