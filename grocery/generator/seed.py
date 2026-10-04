"""Reference-data seeding — the one-time, idempotent fill.

`seed_all` builds every dimension the tick loop reads: locations and employees,
the product catalogue, price history, coupons and deals, loyalty members, trucks,
the customer dimension, and the elasticity columns. It returns the caches the
loop keeps for its lifetime.

Split out of `generator/main.py` (t_c2eca5dd); `main.py` re-exports every name.
"""
import logging
from datetime import date

from models import hr, pos, timeclock, ordering, fulfillment, transport, inventory
from models import shrinkage, promotions, scheduling, returns, online, weather, customers
from elasticity import seed_elasticity_columns

log = logging.getLogger('grocery-generator')


# ---------------------------------------------------------------------------
# seed_all
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def seed_all(conn, cfg):
    log.info("Running seed checks...")
    # First: make sure an OLD data dir has the elasticity columns. A
    # schema.sql change only reaches a *fresh* bootstrap (the same trap the
    # AGENTS.md documents for the PG16->PG18 rebuild and for `pos.returns`), so
    # an install generated before t_08deeddf keeps its old `pos.products` and
    # the demand curve has nothing to key on. Idempotent and additive.
    seed_elasticity_columns(conn, cfg)
    # Same trap, same remedy, for the customer dimension: a `schema.sql` change
    # only reaches a fresh bootstrap, so an install generated before this card
    # has no `pos.customers` and no `loyalty_members.customer_id`. Idempotent and
    # additive, and it must run BEFORE seed_loyalty_members so the seeded cards
    # are dimensioned in the same pass.
    customers.ensure_tables(conn)
    # Same trap, same remedy, for `weather.daily` (t_2ab1fb0a): a schema.sql
    # change only reaches a fresh bootstrap, so an install generated before this
    # card has no `weather` schema at all. Idempotent and additive.
    weather.ensure_table(conn)
    locations = hr.seed_locations(conn, cfg)
    employees = hr.seed_employees(conn, cfg, locations)
    departments = pos.seed_departments(conn, cfg)
    products = pos.seed_products(conn, cfg, departments)
    pos.seed_price_history(conn, cfg, products)
    inventory.seed_inventory(conn, cfg, products, locations['stores'])
    trucks = transport.seed_trucks(conn, truck_count=4)
    # Promotions are seeded with a window that reaches back over the backfill
    # horizon: the back-dated transactions reference them, so a window opening
    # "today" would leave every back-dated redemption outside it.
    history_days = getattr(cfg.generator, 'backfill_lookback_days', 30)
    pos.seed_named_coupons(conn, departments, history_days)
    pos.seed_coupons(conn, cfg, departments, products, history_days)
    pos.seed_combo_deals(conn, cfg, departments, products, history_days)
    pos.seed_loyalty_members(conn, cfg)
    # Every loyalty card gets a household. Idempotent: the candidate set is
    # "cards with a NULL customer_id" and the write sets it, so this also
    # picks up members that predate the dimension and nothing on a re-run.
    customers.backfill_customers(conn, cfg)
    # One-time: mark perishable products + assign shelf_life_days
    shrinkage.mark_perishable_products(conn)
    # Ensure a current weekly ad exists at startup
    promotions.ensure_current_ad(conn, date.today(), products)
    log.info("Seed complete: %d stores, %d warehouses, %d employees, %d products, %d trucks",
             len(locations['stores']), len(locations['warehouses']),
             len(employees), len(products), len(trucks))
    return locations, employees, departments, products, trucks

