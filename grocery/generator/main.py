"""
Grocery Data Generator — main entry point.

On startup:
  1. Ensures the 'grocery' database and schema exist (self-bootstrapping).
  2. Seeds all reference data (idempotent).
  3. Auto-starts a 30-day backfill when the database is empty, then
     transitions to realtime automatically — no manual configuration needed.
  4. Enters the generation loop (realtime / backfill / stopped).

Backfill behaviour:
  - Skips any date that already has POS transaction data (no duplicates).
  - When backfill_end_date is today, the last day is treated as a partial
    day: generates hours 0 → (current_hour - 1) at their hour boundaries,
    then one final tick at datetime.now() to align exactly with where
    realtime picks up — zero gap between backfill and realtime.
  - Full days (any date before today) use generate_day_events() for
    timeclock; the partial current day uses generate_events() per-hour
    so that open shifts are handled correctly by realtime thereafter.

Supply-chain pipeline (runs once per simulated day):
  POS depletion → low stock → store orders → fulfillment → truck dispatch → delivery receipts

Layout (t_c2eca5dd) — this module is the entry point and the re-export facade;
the work lives in four siblings:

  `generator/bootstrap.py`  database bootstrap, connection, control state
  `generator/volume.py`     the volume law (POS + online share it)
  `generator/seed.py`       reference-data seeding
  `generator/tick.py`       the realtime tick
  `generator/backfill.py`   backfill mode + gap helpers

Every public name is re-exported here, so `main.run_tick(...)`,
`from main import compute_pos_count`, and the tests importing
`grocery.generator.main.*` keep working unchanged.

**Sibling imports are conditional, and that is deliberate.** This file is BOTH
an importable module (`grocery.generator.main`, or flat `main` on the generator's
sys.path) and a script (`python main.py` — what `grocery/generator/Dockerfile`
and the standalone entrypoint actually run). A package-relative import
(`from .volume import ...`) raises "attempted relative import with no known
parent package" when the file is run as `__main__`, and an absolute
`from volume import ...` creates a second copy of the siblings when this tree
is imported as `grocery.generator.main` — which is how 25 POS tests broke
during the pos.py split. So: use the relative form when we are inside a real
package, and fall back to the flat sibling name when we are the script.

Siblings import each other the same way, via the `bootstrap`/`volume`/`tick`
modules themselves rather than reaching for each other's names directly.
"""
import logging
import os
import sys
import time
from datetime import datetime

import psycopg2
import psycopg2.extras

from config import load_config, reload_config
from models import hr, pos

# `__package__` is '' or None exactly when this file is the entry script.
_IN_PACKAGE = bool(__package__)

if _IN_PACKAGE:
    from .bootstrap import (  # noqa: F401
        SCHEMA_FILE,
        bootstrap_database,
        get_connection,
        read_state,
        record_stats,
        wait_for_db,
    )
    from .volume import (  # noqa: F401
        DEFAULT_TICK_INTERVAL_SECONDS,
        SIM_HOUR_SECONDS,
        _unbiased_count,
        compute_online_count,
        compute_pos_count,
        daily_online_target,
        daily_pos_target,
        daily_volume_target,
        online_count_expectation,
        per_tick_volume_expectation,
        pos_count_expectation,
        realtime_tick_seconds,
    )
    from .seed import seed_all  # noqa: F401
    from .backfill import (  # noqa: F401
        _ensure_realtime,
        auto_backfill_if_fresh,
        has_data_for_date,
        run_backfill,
    )
    from .tick import (  # noqa: F401
        _weather_for_tick,
        get_ad_product_prices,
        run_tick,
    )
else:
    # Running as `python main.py`: sibling modules live beside this file.
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from bootstrap import (  # noqa: F401
        SCHEMA_FILE,
        bootstrap_database,
        get_connection,
        read_state,
        record_stats,
        wait_for_db,
    )
    from volume import (  # noqa: F401
        DEFAULT_TICK_INTERVAL_SECONDS,
        SIM_HOUR_SECONDS,
        _unbiased_count,
        compute_online_count,
        compute_pos_count,
        daily_online_target,
        daily_pos_target,
        daily_volume_target,
        online_count_expectation,
        per_tick_volume_expectation,
        pos_count_expectation,
        realtime_tick_seconds,
    )
    from seed import seed_all  # noqa: F401
    from backfill import (  # noqa: F401
        _ensure_realtime,
        auto_backfill_if_fresh,
        has_data_for_date,
        run_backfill,
    )
    from tick import (  # noqa: F401
        _weather_for_tick,
        get_ad_product_prices,
        run_tick,
    )

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s — %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
)
log = logging.getLogger('grocery-generator')


def main():
    cfg = load_config()
    wait_for_db(cfg)
    bootstrap_database(cfg)

    conn = get_connection(cfg)
    psycopg2.extras.register_uuid()

    locations, employees, departments, products, trucks = seed_all(conn, cfg)

    # Auto-start a 30-day backfill on a fresh (empty) database.
    auto_backfill_if_fresh(conn, cfg)

    # Repair promo windows/counters against whatever history is already on
    # disk — an install generated before the windowing fix would otherwise
    # keep its out-of-window redemptions (card t_01b4fe4f). Idempotent.
    pos.reconcile_promotions(conn)

    log.info("Generator ready. Entering main loop.")

    REFRESH_EVERY = 20
    tick_count = 0
    members = pos.fetch_loyalty_members(conn)
    coupons = pos.fetch_active_coupons(conn)
    deals = pos.fetch_active_deals(conn)
    # Survives across ticks, not across a restart: the day's weather rows are
    # written once when the date changes and then read back from the table, so
    # a 30 s tick does not re-upsert 2880 times a day (t_2ab1fb0a). A restart
    # re-derives it from the same pure seed, so the values are unchanged.
    weather_cache = {}

    while True:
        try:
            cfg = reload_config(cfg)
            state = read_state(conn)

            if state['mode'] == 'stopped' or not state['is_running']:
                time.sleep(state['tick_interval_seconds'])
                continue

            if state['is_paused']:
                time.sleep(state['tick_interval_seconds'])
                continue

            if state['mode'] == 'backfill':
                members = pos.fetch_loyalty_members(conn)
                coupons = pos.fetch_active_coupons(conn)
                deals = pos.fetch_active_deals(conn)
                employees = hr.fetch_active_employees(conn)
                locations = hr.fetch_locations(conn)
                run_backfill(conn, cfg, state, locations, employees, departments,
                             products, trucks, members, coupons, deals)
                continue

            run_tick(conn, cfg, state, datetime.now(), locations, employees,
                     departments, products, trucks, members, coupons, deals,
                     weather_cache=weather_cache)

            tick_count += 1
            if tick_count % REFRESH_EVERY == 0:
                members = pos.fetch_loyalty_members(conn)
                coupons = pos.fetch_active_coupons(conn)
                deals = pos.fetch_active_deals(conn)
                employees = hr.fetch_active_employees(conn)
                locations = hr.fetch_locations(conn)

            time.sleep(state['tick_interval_seconds'])

        except psycopg2.OperationalError as e:
            log.error("DB connection lost: %s — reconnecting...", e)
            time.sleep(10)
            try:
                conn = get_connection(cfg)
            except Exception:
                pass
        except Exception as e:
            log.exception("Unexpected error in main loop: %s", e)
            time.sleep(10)


if __name__ == '__main__':
    main()

