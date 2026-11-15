"""
Fulfillment model — warehouse processes approved store orders.

Flow:
  1. process_pending_orders() — called after ordering, same simulated day:
     - Advances the rows an earlier pass left in 'received' or 'picking'
     - Creates fulfillment.orders for each approved store order and schedules
       its pick (see "The lifecycle" below)
     - Creates fulfillment.items (picks all requested qty, simulates occasional shorts)
     - Moves the store order to 'picking'
     - Returns the (fulfillment_id, store_order_id, store_location_id) tuples
       that made the day's truck — see `transport.dispatch_loads`

  2. transport.dispatch_loads() moves the ones on a load to 'shipped' and the
     store order with them; transport.receive_delivered_loads() moves them to
     'delivered' when the load arrives.

The lifecycle (t_5a408514)
---------------------------
`fulfillment.orders.status` used to be the literal 'packed' on every row, with
`started_at = completed_at = sim_dt`, so the six marts built on these columns
published `hours_to_fulfill` 0 on 6,849/6,849 orders, `fulfillment_status`
constant on 100 % of them, and `hours_to_pick_start` as low as -642 h.

Two separate defects produced that shape, and both are fixed here:

  * **No timeline.** The row was stamped with one instant. A pick now has a
    start and an end, and both are derived per row rather than clamped to one
    value: `received → picking → packed → shipped → delivered`.

  * **A wall clock in a business-clock column.** `created_at` defaulted to
    NOW(), which during a backfill is today's real time while `started_at` is
    a simulated day weeks in the past — so `started_at - created_at` (data-lab's
    `hours_to_pick_start`) was negative by the age of the backfill. `created_at`
    is now the instant the warehouse RECEIVED the order (`approved_dt`), which
    is what `mart_fulfillment_operations.order_received_at` already calls it.
    Nothing watermarks on this column: data-lab loads `fulfillment.orders` with
    a FULL refresh, and the API paginates on it with a primary-key tiebreak.

THE SCHEDULE IS DERIVED, NOT STORED
-----------------------------------
Two constraints shape the machine:

  * a simulated fact may not be stamped after `sim_dt` — that is a future date,
    and data-lab's `column_profile.py` fails those (`future_dates`,
    `future_tolerance_ratio` 0.001), so the plan cannot be written down as a
    future `started_at`;
  * a plan that lived only in memory would be lost on the restart the realtime
    loop has to survive.

So each row's schedule — how long the order waited on the dock, how long the
pick took, whether it is one of the queued orders that misses the day's truck —
is drawn from the row's own `fulfillment_id` (`_pick_schedule`). Every pass
reaches the same answer, with no column, no memory, and no migration.

TWO KINDS OF PASS
-----------------
`day_wide_pass=True` is the backfill: one call models a whole simulated day, so
the day's picks are compressed into the window between the order's approval and
the pass (20-85 % of it, drawn per row — a hard clamp is the original defect in
a different constant). `day_wide_pass=False` is realtime, where a pass is an
instant: a row created now waits for the pick start its schedule names, and a
later pass records the work. That is where the dwell between states is real —
`received` for as long as the queue says, `picking` while the pick runs.

`pending`/`picking`/`loaded`/`cancelled` are still valid statuses — rows written
by earlier images carry them, and the API contract must keep loading those.

Short-picks are the vendor's behaviour, not the warehouse's (t_57b1a1ab)
--------------------------------------------------------------------------
This used to be `random.random() < 0.05` on every line of every order: one flat
rate for every SKU, from every vendor, forever. A mart built on that could not
rank vendors — they were all identical by construction.

`vendor_cfg` is the `cfg.vendors` block. When it is supplied, the short rate
for a line is that PRODUCT's vendor rate (`inv.suppliers.short_ship_rate` via
`suppliers.short_probability_for`), so a vendor with a 15% rate really is
shorted three times as often as one at 5%. Supply disruption still stacks on
top, because a scenario is a departure from normal rather than a vendor's
character.

The parameter is optional so existing callers and tests are unaffected, and so
a pre-t_57b1a1ab data dir (no `inv.suppliers` at all) keeps the old behaviour
rather than failing to pick anything.
"""
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# The flat short rate used when no vendor config is available (the pre-
# t_57b1a1ab behaviour, and the fallback for a data dir with no inv.suppliers).
# Named so it is greppable rather than a bare 0.05 literal.
DEFAULT_SHORT_RATE = 0.05

# Units a short pick takes off the requested quantity.
SHORT_PICK_MIN_UNITS = 1
SHORT_PICK_MAX_UNITS = 5

# How much worse a short pick gets under scenario.supply_disruption.
SUPPLY_DISRUPTION_SHORT_MULTIPLIER = 2.0

# ---------------------------------------------------------------------------
# Pick/pack schedule (t_5a408514)
# ---------------------------------------------------------------------------
# Hours between the order reaching the warehouse and the pick starting, for an
# order the dock takes in its stride. The order waits for a picker, which is
# what makes `started_at` and `created_at` different instants.
PICK_START_HOURS = (0.5, 6.0)

# Share of orders the dock is too backed up to reach (or the truck leaves
# before): their pick start is the next shift, not this one. This is the only
# state that outlives a pass in a day-wide backfill — without it a fresh
# 30-day backfill would only ever hold 'shipped' and 'delivered'.
PICK_BACKLOG_RATE = 0.10
BACKLOG_START_HOURS = (10.0, 22.0)

# Line work: walking the aisle and scanning one line, plus pack/label at the end
# of the pick.
PICK_MINUTES_PER_LINE = 0.9
PICK_MIN_MINUTES = 6.0
PACK_MINUTES = (12.0, 45.0)

# The longest single pick the model will report. `hours_to_fulfill` is one
# warehouse shift, not a day and a half; the marts' bounds are documented
# against this ceiling.
MAX_PICK_MINUTES = 240.0

# Floor on the window a pass must offer before anything can be stamped.
MIN_PICK_WINDOW_MINUTES = 3.0

# A day-wide pass compresses the day's picks into its window. The share is
# drawn per row and bounded — squeezing everything to `window * 0.85` is the
# degenerate constant this card exists to remove.
MIN_PICK_SHARE_OF_WINDOW = 0.20
MAX_PICK_SHARE_OF_WINDOW = 0.85


def _pick_schedule(fulfillment_id: str, lines: int) -> Tuple[float, float, float, bool]:
    """
    (start_delay_min, duration_min, squeeze_share, queued) for one fulfillment.

    Drawn from the row's own id: a pass recomputes the same schedule on every
    call, on every restart, without storing a plan the schema has no column
    for. Drawn in one place and in one order, so the values cannot drift
    between the pass that schedules a pick and the pass that records it.
    """
    rng = random.Random("pick:" + str(fulfillment_id))
    queued = rng.random() < PICK_BACKLOG_RATE
    hours = BACKLOG_START_HOURS if queued else PICK_START_HOURS
    start_delay = rng.uniform(*hours) * 60.0
    duration = min(
        MAX_PICK_MINUTES,
        max(PICK_MIN_MINUTES,
            lines * PICK_MINUTES_PER_LINE + rng.uniform(*PACK_MINUTES)))
    share = rng.uniform(MIN_PICK_SHARE_OF_WINDOW, MAX_PICK_SHARE_OF_WINDOW)
    return start_delay, duration, share, queued


def _pick_state(fulfillment_id: str,
                approved_dt: datetime,
                sim_dt: datetime,
                lines: int,
                day_wide_pass: bool,
                started_at: Optional[datetime] = None) -> Tuple[str, Optional[datetime], Optional[datetime]]:
    """
    (status, started_at, completed_at) for this row as of `sim_dt`.

    'received' means the pick has not started; 'picking' means it started and
    has not finished (completed_at stays NULL); 'packed' means it is done and
    waiting for a truck. Every instant returned is inside
    [approved_dt, sim_dt] — see the module docstring for why that bound is not
    negotiable.

    `started_at` is passed for a row already 'picking', so a later pass does
    not redraw its start and stretch the pick.
    """
    start_delay, duration, share, queued = _pick_schedule(fulfillment_id, lines)
    window = (sim_dt - approved_dt).total_seconds() / 60.0
    if window < MIN_PICK_WINDOW_MINUTES:
        return ('received', None, None)

    if started_at is not None:
        start_delay = (started_at - approved_dt).total_seconds() / 60.0
    elif day_wide_pass and not queued:
        # A day-wide pass models the whole day at once. Where the schedule
        # would land the pick after the pass, the day's work is compressed into
        # the window instead — proportionally and per row, never as one clamp.
        if start_delay + duration > window:
            duration = max(1.0, window * share)
            start_delay = min(start_delay,
                              max(0.0, window - duration - 1.0))

    if start_delay >= window:
        return ('received', None, None)          # still on the dock
    started = approved_dt + timedelta(minutes=start_delay)
    if start_delay + duration > window:
        return ('picking', started, None)        # in the aisle as the pass runs
    return ('packed', started, started + timedelta(minutes=duration))


def process_pending_orders(
    conn,
    warehouse_employees: List[Dict],
    sim_dt: datetime,
    vendor_cfg=None,
    scenario=None,
    day_wide_pass: bool = False,
) -> List[Tuple[str, str, str]]:
    """
    Fulfil approved orders. Returns list of
    (fulfillment_id, store_order_id, store_location_id) for the ones that made
    this pass's truck — transport marks those 'shipped'.

    `day_wide_pass=True` for the backfill (one call models a whole simulated
    day), False for a realtime tick (one call models an instant).
    """
    # One vendor lookup per pass, not per line (t_57b1a1ab).
    vendor_by_product: Dict[str, Dict] = {}
    if vendor_cfg is not None:
        try:
            from models import suppliers as _suppliers
            vendor_by_product = _suppliers.vendor_by_product(conn)
        except Exception:
            log.warning("Vendor lookup unavailable — using the flat short rate "
                        "(pre-t_57b1a1ab data dir?)")
            vendor_by_product = {}

    # A disruption makes every vendor worse, on top of its own rate — a
    # scenario is a departure from normal, not a vendor's character.
    disruption = bool(scenario is not None
                      and getattr(scenario, 'supply_disruption', False))

    pickers = [e for e in warehouse_employees if e['department'] == 'warehouse']

    fulfilled = []
    with conn.cursor() as cur:
        # ── Packed rows that missed an earlier truck ───────────────────────
        # Not part of `approved` any more (the store order left 'approved' when
        # the fulfillment was created), so without this sweep they would never
        # reach a load, a receipt, or a 'delivered' status. Queried BEFORE the
        # advance below packs anything, or this pass would dispatch the rows it
        # has just started twice.
        cur.execute("""
            SELECT fo.fulfillment_id::text, fo.store_order_id::text,
                   so.store_location_id::text
            FROM fulfillment.orders fo
            JOIN ordering.store_orders so ON so.order_id = fo.store_order_id
            WHERE fo.status = 'packed'
              AND NOT EXISTS (SELECT 1 FROM transport.load_items li
                              WHERE li.fulfillment_id = fo.fulfillment_id)
        """)
        fulfilled.extend(cur.fetchall())

        # ── New fulfillments for approved orders ───────────────────────────
        cur.execute("""
            SELECT so.order_id::text, so.warehouse_location_id::text,
                   so.store_location_id::text,
                   COALESCE(so.approved_dt, so.order_dt)::timestamp
            FROM ordering.store_orders so
            WHERE so.status = 'approved'
            ORDER BY so.order_dt
        """)
        approved = cur.fetchall()

        for order_id, wh_loc_id, store_loc_id, approved_dt in approved:
            fulfillment_id = str(uuid4())
            assigned_to = random.choice(pickers)['employee_id'] if pickers else None

            # Get order items
            cur.execute("""
                SELECT product_id, quantity_approved
                FROM ordering.store_order_items
                WHERE order_id = %s::uuid AND quantity_approved IS NOT NULL
            """, (str(order_id),))
            items = cur.fetchall()

            # The warehouse has it, and the pick has not started yet. The
            # advance below moves it on if its schedule says the pick is due —
            # it is the same function that advances every open row, so a row
            # cannot be written in a state the machine would not produce.
            #
            # `created_at` is the warehouse's own clock: the instant the order
            # reached it, not the wall clock at INSERT (module docstring).
            cur.execute("""
                INSERT INTO fulfillment.orders
                    (fulfillment_id, store_order_id, warehouse_location_id,
                     assigned_to, status, created_at)
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, 'received', %s)
            """, (fulfillment_id, str(order_id), str(wh_loc_id),
                   assigned_to, approved_dt))

            # Create fulfillment items. The short rate is the vendor's own.
            item_records = []
            for prod_id, qty_req in items:
                if vendor_by_product:
                    from models import suppliers as _suppliers
                    vendor = vendor_by_product.get(str(prod_id))
                    rate = (_suppliers.short_probability_for(vendor, vendor_cfg)
                            if vendor else DEFAULT_SHORT_RATE)
                else:
                    rate = DEFAULT_SHORT_RATE
                if disruption:
                    rate = min(1.0, rate * SUPPLY_DISRUPTION_SHORT_MULTIPLIER)
                short = random.random() < rate
                qty_picked = (
                    max(0, qty_req - random.randint(SHORT_PICK_MIN_UNITS,
                                                    SHORT_PICK_MAX_UNITS))
                    if short else qty_req)
                pick_status = 'short' if short else 'picked'
                item_records.append((fulfillment_id, str(prod_id), qty_req,
                                     qty_picked, pick_status))

            if item_records:
                execute_values(cur, """
                    INSERT INTO fulfillment.items
                        (fulfillment_id, product_id, quantity_requested,
                         quantity_picked, pick_status)
                    VALUES %s
                """, item_records, template="(%s::uuid,%s::uuid,%s,%s,%s)")

            # The order leaves 'approved' the moment the warehouse has it. The
            # truck moves it to 'shipped' (transport.dispatch_loads), so a row
            # still in 'received' or 'picking' must not claim it was shipped.
            cur.execute("""
                UPDATE ordering.store_orders
                SET status = 'picking', updated_at = %s
                WHERE order_id = %s::uuid
            """, (sim_dt, str(order_id)))

        # ── Advance every open row to where its schedule says it is ────────
        # `approved_dt::timestamp` is deliberate: the column is TIMESTAMPTZ and
        # a naive `sim_dt` cannot be subtracted from an aware value. Postgres
        # converts in the session timezone, which is the same one a naive
        # datetime is read back in on INSERT, so the two stay comparable.
        cur.execute("""
            SELECT fo.fulfillment_id::text, fo.store_order_id::text,
                   so.store_location_id::text, fo.status,
                   fo.started_at::timestamp, so.approved_dt::timestamp,
                   (SELECT count(*) FROM fulfillment.items fi
                     WHERE fi.fulfillment_id = fo.fulfillment_id)
            FROM fulfillment.orders fo
            JOIN ordering.store_orders so ON so.order_id = fo.store_order_id
            WHERE fo.status IN ('received', 'picking')
              AND so.approved_dt IS NOT NULL
            ORDER BY so.approved_dt
        """)
        open_rows = cur.fetchall()

        for (fulfillment_id, order_id, store_loc_id, status,
             started_at, approved_dt, lines) in open_rows:
            new_status, new_started, new_completed = _pick_state(
                fulfillment_id, approved_dt, sim_dt, lines, day_wide_pass,
                started_at=started_at if status == 'picking' else None)
            if new_status == status and status == 'received':
                continue                        # nothing has happened yet
            cur.execute("""
                UPDATE fulfillment.orders
                   SET status = %s, started_at = %s, completed_at = %s
                 WHERE fulfillment_id = %s::uuid
            """, (new_status, new_started, new_completed, fulfillment_id))
            if new_status == 'packed':
                fulfilled.append((fulfillment_id, order_id, store_loc_id))

    conn.commit()
    log.info("Fulfilled %d orders", len(fulfilled))
    return fulfilled
