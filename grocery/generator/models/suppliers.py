"""
Vendors, and the shortage → credit-memo lifecycle (t_57b1a1ab).

WHAT WAS MISSING
----------------
`inv.products.supplier_name` was a free-text string and `lead_time_days` a
static integer; `fulfillment.items` recorded `pick_status = 'short'` and
`transport.receive_delivered_loads` silently EXCLUDED those lines from the
receipt:

    SELECT fi.product_id, fi.quantity_picked
    FROM transport.load_items li
    JOIN fulfillment.items fi ON fi.fulfillment_id = li.fulfillment_id
    WHERE li.load_id = %s::uuid AND fi.pick_status = 'picked'

So a vendor who could not fill a line left no trace: the store's shelf came up
short, nothing recorded it, nobody was blamed, and no money came back. The
ordering → fulfillment → transport → receipt chain was closed and stopped one
step short of the vendor relationship. This module is that step.

WHAT IT DOES NOW
----------------
1. `seed_suppliers` — first-class vendor rows with promised lead time + spread,
   a short-ship rate, credit terms, and (for the DSD vendors) a delivery
   schedule per store. `inv.products.supplier_id` is a real FK.
2. `draw_lead_time_days` — the realised lead time is drawn around the vendor's
   PROMISE, per product per event. This is what makes `lead_time_mean_days` and
   `lead_time_stddev_days` mean something: before, every product had one
   constant integer and there was no promise to be late against.
3. `short_probability_for` — a short is drawn around the VENDOR's rate, so a
   good vendor is genuinely better than a bad one.
4. `record_short_ships` — the receipt-time path: turn the short-picks that
   `receive_delivered_loads` already excluded into `inv.short_ship_events`.
5. `open_credit_memo` / `advance_credit_memos` — the claim and its lifecycle:
   open → submitted → paid | rejected, with expiry against the vendor's claim
   window and a settlement delay drawn from the vendor's payment terms.
6. `generate_dsd_deliveries` — DSD vendors deliver to the shelf on their own
   truck: `inv.dsd_deliveries` + items, stock on hand, and their own short-ship
   → credit path.

PLACEMENT IN THE TICK
---------------------
Short-ships are recorded inside `transport.receive_delivered_loads` (the moment
the load is found short, so the store and the load are both known). Credit-memo
advancement runs AFTER `returns.generate_returns` in the same daily block — the
phase where every other *follow-up* event already runs — so a memo is submitted
and resolved by simulated days, not by ticks. DSD deliveries run in the same
daily block, just before the credit pass, because a DSD line's short-ship is
detected at delivery and must be creditable from the same day.

WHY NOT A BOOLEAN
-----------------
A credit memo gets a table of its own rather than `short_ship_events.is_credited`
because the question a vendor-performance mart answers is not "was it credited"
but "how long did it take to be paid, and did it get paid at all". Those need a
lifecycle with its own timestamps and a terminal state, and a memo that never
resolves is a real finding rather than a missing value.
"""
import logging
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)

# The caller may pass a private `random.Random` (for a deterministic test) or
# nothing at all, in which case the module-level `random` is used so a test's
# `random.seed(...)` governs the draw. Both expose the surface used below, so
# the union is widened the same way `elasticity.RNG` is.
RNG = Any

# Where a shortfall is noticed. A warehouse line is found at the receiving dock;
# a DSD line is found on the shelf when the vendor's truck has been and gone.
DETECTED_RECEIVING = 'receiving'
DETECTED_DSD = 'dsd_delivery'

# Why the vendor did not fill the line. The mix is what decides whether a
# claim is worth filing and whether the vendor pays, so it is drawn per event
# rather than assumed.
# (reason, weight)
SHORT_REASONS = [
    ('warehouse_shortage', 0.40),
    ('out_of_stock', 0.25),
    ('weather_carrier_delay', 0.15),
    ('delivery_missed', 0.12),
    # Ours, not theirs: the goods were picked and then rejected at our dock.
    ('quality_reject', 0.08),
]

# How a vendor answers a claim. Keyed to the reason: our own quality rejection
# is rarely credited, the vendor's own stock-out usually is.
# (reason -> (pay probability, rejection reason or None))
REASON_PAY_PROFILE = {
    'warehouse_shortage': (0.92, None),
    'out_of_stock': (0.88, None),
    'weather_carrier_delay': (0.70, None),
    'delivery_missed': (0.55, 'Delivery window missed — claim disputed'),
    'quality_reject': (0.30, 'Goods rejected at our dock — claim denied'),
}

# Resolved by whom, for the paid / rejected pair.
RESOLVER_VENDOR = 'vendor'
RESOLVER_STORE = 'store'

# Row templates for the batched writes, named so a test can assert each one has
# exactly as many slots as its INSERT has columns and its row tuple has fields.
# (The stockout template in `inventory` shipped with 11 placeholders for 12
# columns and only failed on the first tick whose shelf ran short — found by the
# wipe+reseed probe, t_959cd040. Same class of bug, same guard.)
SHORT_SHIP_ROW_TEMPLATE = (
    "(%s::uuid,%s::uuid,%s::uuid,%s::uuid,%s::uuid,"
    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
)
# No credit-memo template: a memo is written by a single-row `cur.execute` with
# named columns and `RETURNING`, because the caller needs the new id to link
# the next lifecycle step to it. An execute_values template would be a constant
# nothing reads.
DSD_DELIVERY_ROW_TEMPLATE = (
    "(%s::uuid,%s::uuid,%s::uuid,%s::uuid,%s,%s,%s,%s,%s)"
)
DSD_ITEM_ROW_TEMPLATE = "(%s::uuid,%s::uuid,%s,%s,%s)"

# One slot per column of the INSERT each template feeds. These counts are the
# parity guard's other half (the tests assert slots == count AND that the row
# tuple `record_short_ships` builds has exactly `count` fields), because a
# template with one slot fewer than the row tuple has fields is invisible until
# a real short-ship fires — which is how the stockout template in `inventory`
# shipped with 11 placeholders for 12 columns and only failed on the first tick
# whose shelf ran short (t_959cd040). A short-ship is far rarer than a stockout,
# so a mismatch here would have stayed dark for weeks.
#
# Each count matches the INSERT it feeds:
#   short_ship_events  16 columns   dsd_deliveries  9 columns
#   dsd_delivery_items  5 columns
SHORT_SHIP_COLUMN_COUNT = 16
DSD_DELIVERY_COLUMN_COUNT = 9
DSD_ITEM_COLUMN_COUNT = 5

# A short-ship cannot fill more than the whole line, and a line below this is
# not worth the paperwork of a claim — the vendor's terms decide that, not us,
# so this is only the floor below which we stop generating a line at all.
MIN_SHORT_UNITS = 1.0


# ---------------------------------------------------------------------------
# The pure laws (no database — testable directly)
# ---------------------------------------------------------------------------

def draw_lead_time_days(vendor: Dict, rng: Optional[RNG] = None) -> int:
    """
    A realised lead time, in whole days, drawn around the vendor's PROMISE.

    `vendor` needs `lead_time_mean_days` and `lead_time_stddev_days`. The
    result is clamped to >= 0 because a gaussian can go negative and a negative
    lead time is not a thing.

    This is the function that makes the two columns worth having. The old model
    shipped `lead_time_days` as one static integer per product, so a
    vendor-performance mart had nothing to compare an actual delivery against —
    the "promise" and the "reality" were the same number by construction. Here
    they are two numbers with a distribution between them, which is the whole
    reason a fill-rate / on-time-delivery mart is possible.
    """
    rng = rng or random
    mean = float(vendor.get('lead_time_mean_days', 2.0) or 0.0)
    stddev = max(0.0, float(vendor.get('lead_time_stddev_days', 0.0) or 0.0))
    if stddev <= 0.0:
        return max(0, int(round(mean)))
    return max(0, int(round(rng.gauss(mean, stddev))))


def short_probability_for(vendor: Dict, cfg: Any,
                          detected_source: str = DETECTED_RECEIVING) -> float:
    """
    P(this line comes up short) for `vendor`, bounded to [0, 1].

    Drawn from the VENDOR's rate rather than a flat 5% for every line, so a
    vendor with `short_ship_rate: 0.15` really is worse than one at 0.04 and a
    fill-rate mart can tell them apart. A DSD line gets the configured bonus on
    top: a vendor truck that misses the window leaves the shelf empty that
    morning, which is the perishable version of the same problem.

    Bounded because a config with a nonsense rate must clamp rather than
    produce `random.random() < 3.0`, which would short 100% of lines without
    anyone noticing the typo.
    """
    rate = float(vendor.get('short_ship_rate') or 0.05)
    if detected_source == DETECTED_DSD:
        rate += float(getattr(cfg.vendors, 'dsd_short_ship_bonus', 0.0) or 0.0)
    return min(1.0, max(0.0, rate))


def draw_short_reason(rng: Optional[RNG] = None) -> str:
    """One reason from the weighted mix."""
    rng = rng or random
    reasons = [r for r, _ in SHORT_REASONS]
    weights = [w for _, w in SHORT_REASONS]
    return rng.choices(reasons, weights=weights, k=1)[0]


def claim_deadline_days(vendor: Dict) -> int:
    """The vendor's claim window, never below one day."""
    try:
        window = int(vendor.get('credit_window_days', 14) or 0)
    except (TypeError, ValueError):
        window = 14
    return max(1, window)


def settlement_days(vendor: Dict, rng: Optional[RNG] = None) -> int:
    """
    Days from a submitted claim to the vendor paying it.

    Drawn from the vendor's own terms (mean + stddev). The memo lifecycle's
    "days to pay" is then a real distribution whose shape a manager controls
    by editing one vendor's config, rather than a constant this module invented.
    """
    rng = rng or random
    mean = float(vendor.get('credit_settle_mean_days', 10) or 0.0)
    stddev = max(0.0, float(vendor.get('credit_settle_stddev_days', 4) or 0.0))
    if stddev <= 0.0:
        return max(0, int(round(mean)))
    return max(0, int(round(rng.gauss(mean, stddev))))


def short_quantity(requested: float, picked: float,
                   rng: Optional[random.Random] = None) -> float:
    """
    The shortfall on one line, to 3dp (the precision of every quantity column
    here). Never negative and never more than was asked for — the same
    arithmetic `inv.short_ship_events`' CHECK constraints enforce, done in
    Python so the caller can skip a line rather than fail an INSERT.
    """
    return max(0.0, round(float(requested) - float(picked), 3))


def dsd_delivery_weekdays(per_week: int, rng: Optional[RNG] = None
                          ) -> List[int]:
    """
    The weekdays a DSD vendor visits, chosen without replacement.

    `per_week` is clamped to 0..7 — 0 means "once a week" (a single weekday
    chosen at random), because a vendor that never delivers is a config error,
    not a behaviour worth modelling. Python's weekday is Monday=0, matching the
    `delivery_weekday` CHECK of 0..6.
    """
    rng = rng or random
    try:
        count = int(per_week)
    except (TypeError, ValueError):
        count = 4
    count = max(0, min(7, count))
    if count == 0:
        return [rng.randrange(0, 7)]
    return sorted(rng.sample(range(0, 7), count))


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

def seed_suppliers(conn, cfg: Config) -> List[Dict]:
    """
    Create `inv.suppliers` from `config.vendors`. Idempotent.

    The table is the source of truth once written: re-running with a changed
    config UPDATES the vendor's behaviour columns rather than adding a second
    copy, because a config change must not orphan the products already pointing
    at a supplier_id. Name and code are unique keys, so the update is keyed on
    whichever one the config carries.

    Returns the vendor rows (id + every behaviour column), which the seeding
    path needs to assign products and the inbound path needs to draw from — so
    no caller has to re-read them.
    """
    entries = [v for v in (cfg.vendors.vendors or []) if v and v.get('name')]
    if not entries:
        log.warning("No vendors configured — inv.suppliers will be empty and "
                    "inv.products.supplier_id stays NULL")
        return []

    rows = []
    for entry in entries:
        rows.append((
            str(entry['name']),
            str(entry.get('code') or _code_for(str(entry['name']))),
            str(entry.get('fulfillment_model') or 'warehouse'),
            float(entry.get('lead_time_mean_days') or 2.0),
            max(0.0, float(entry.get('lead_time_stddev_days') or 0.0)),
            min(1.0, max(0.0, float(entry.get('short_ship_rate') if
                                     entry.get('short_ship_rate') is not None
                                     else 0.05))),
            bool(entry.get('credit_eligible', True)),
            max(1, int(entry.get('credit_window_days') or 14)),
            max(0, int(entry.get('credit_settle_mean_days', 10) or 0)),
            max(0, int(entry.get('credit_settle_stddev_days', 4) or 0)),
        ))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO inv.suppliers
                (supplier_name, supplier_code, fulfillment_model,
                 lead_time_mean_days, lead_time_stddev_days, short_ship_rate,
                 credit_eligible, credit_window_days,
                 credit_settle_mean_days, credit_settle_stddev_days)
            VALUES %s
            ON CONFLICT (supplier_name) DO UPDATE SET
                supplier_code = EXCLUDED.supplier_code,
                fulfillment_model = EXCLUDED.fulfillment_model,
                lead_time_mean_days = EXCLUDED.lead_time_mean_days,
                lead_time_stddev_days = EXCLUDED.lead_time_stddev_days,
                short_ship_rate = EXCLUDED.short_ship_rate,
                credit_eligible = EXCLUDED.credit_eligible,
                credit_window_days = EXCLUDED.credit_window_days,
                credit_settle_mean_days = EXCLUDED.credit_settle_mean_days,
                credit_settle_stddev_days = EXCLUDED.credit_settle_stddev_days,
                is_active = TRUE,
                updated_at = NOW()
        """, rows, template=(
            "(%s,%s,%s,%s::numeric,%s::numeric,%s::numeric,%s,%s,%s,%s)"))
    conn.commit()

    vendors = fetch_suppliers(conn)
    log.info("Seeded %d vendors (%d DSD)", len(vendors),
             sum(1 for v in vendors if v['fulfillment_model'] == 'dsd'))
    return vendors


def _code_for(name: str) -> str:
    """A vendor code from its name when the config does not carry one."""
    letters = ''.join(ch for ch in name.upper() if ch.isalnum())
    return (letters[:8] or 'VEN').upper()


def fetch_suppliers(conn) -> List[Dict]:
    """
    Every active vendor, with every column the inbound path draws from.

    One read per seed / per daily pass — not one per line — so a 500-SKU
    backfill day is not 500 lookups.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT supplier_id::text, supplier_name, supplier_code,
                   fulfillment_model, lead_time_mean_days,
                   lead_time_stddev_days, short_ship_rate, credit_eligible,
                   credit_window_days, credit_settle_mean_days,
                   credit_settle_stddev_days
            FROM inv.suppliers
            WHERE is_active
            ORDER BY supplier_name
        """)
        columns = ['supplier_id', 'supplier_name', 'supplier_code',
                   'fulfillment_model', 'lead_time_mean_days',
                   'lead_time_stddev_days', 'short_ship_rate',
                   'credit_eligible', 'credit_window_days',
                   'credit_settle_mean_days', 'credit_settle_stddev_days']
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def seed_dsd_schedules(conn, cfg: Config, suppliers: List[Dict],
                       store_locations: List[Dict]) -> int:
    """
    Give every DSD vendor a delivery schedule at every store.

    Which weekdays is drawn once per (vendor, store) and then stored, so the
    schedule is stable across ticks and across a restart — a schedule that
    changed every boot would make "did the vendor deliver on schedule?"
    unanswerable, which is the only question a delivery schedule exists to
    answer. Idempotent on (supplier, location, weekday).
    """
    dsd = [s for s in suppliers if s['fulfillment_model'] == 'dsd']
    if not dsd or not store_locations:
        return 0

    records = []
    for vendor in dsd:
        for store in store_locations:
            for weekday in dsd_delivery_weekdays(
                    getattr(cfg.vendors, 'dsd_deliveries_per_week', 4)):
                start_hour = random.choice([5, 6, 7, 8])
                records.append((
                    vendor['supplier_id'],
                    store['location_id'],
                    weekday,
                    f'{start_hour:02d}:00:00',
                    f'{start_hour + 2:02d}:00:00',
                ))

    if not records:
        return 0

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO inv.supplier_delivery_schedules
                (supplier_id, location_id, delivery_weekday,
                 delivery_window_start, delivery_window_end)
            VALUES %s
            ON CONFLICT (supplier_id, location_id, delivery_weekday)
            DO NOTHING
        """, records, template="(%s::uuid,%s::uuid,%s,%s::time,%s::time)")
    conn.commit()
    log.info("Seeded %d DSD delivery schedules", len(records))
    return len(records)


def seed_supplier_assignments(conn, cfg: Config, vendors: List[Dict],
                              products: List[Dict]) -> int:
    """
    Point `inv.products` at a vendor, and set its lead time from that vendor.

    Assignment is by DEPARTMENT, not per product: a real catalogue is bought
    from a handful of vendors, each covering whole departments or the whole
    store. Random-per-product would give a warehouse-supplied department a
    dozen one-off DSD vendors and make every vendor's fill rate statistically
    meaningless.

    A DSD vendor serves a perishable department (Produce, Deli by default); a
    warehouse vendor serves the rest. Within that, the vendor is drawn per
    department so the split is stable across a backfill.

    `inv.products.lead_time_days` is left as the vendor's PROMISED time for that
    product — it is what the reorder lead-time math and the API's existing
    column both mean. The REALISED time lands per event in
    `inv.short_ship_events`, because one product has many arrivals and one
    integer cannot hold them all.
    """
    if not vendors or not products:
        return 0

    by_model: Dict[str, List[Dict]] = {'warehouse': [], 'dsd': []}
    for vendor in vendors:
        by_model.setdefault(vendor['fulfillment_model'], []).append(vendor)

    dsd_names = {str(d).strip().lower()
                 for d in (getattr(cfg.vendors, 'dsd_departments', None) or [])}

    # One vendor per department, drawn once. Keyed by department so the whole
    # department moves together and each vendor's volume is meaningful.
    dept_vendor: Dict[str, Dict] = {}
    for department in {str(p.get('department') or '') for p in products}:
        if not department:
            continue
        wants_dsd = department.strip().lower() in dsd_names
        pool = by_model.get('dsd' if wants_dsd else 'warehouse') or []
        if not pool:
            # No DSD vendor configured for a perishable department: fall back to
            # the warehouse pool rather than leaving those SKUs vendorless.
            pool = by_model.get('warehouse') or []
        if pool:
            dept_vendor[department] = random.choice(pool)

    assignments = []
    for product in products:
        vendor = dept_vendor.get(str(product.get('department') or ''))
        if not vendor:
            continue
        assignments.append((
            vendor['supplier_id'],
            vendor['supplier_name'],
            draw_lead_time_days(vendor),
            product['product_id'],
        ))

    if not assignments:
        return 0

    with conn.cursor() as cur:
        execute_values(cur, """
            UPDATE inv.products ip
            SET supplier_id = v.supplier_id,
                supplier_name = v.supplier_name,
                lead_time_days = v.lead_time_days,
                updated_at = NOW()
            FROM (VALUES %s) AS v(supplier_id, supplier_name, lead_time_days, product_id)
            WHERE ip.product_id = v.product_id::uuid
        """, assignments,
            template="(%s::uuid,%s,%s::int,%s::uuid)")

        # A data dir seeded before this table has inv.products rows but no
        # suppliers; backfill any that are still unassigned so a stock image
        # does not leave the vendor columns permanently NULL.
        cur.execute("""
            UPDATE inv.products ip
            SET supplier_name = COALESCE(ip.supplier_name, sup.supplier_name)
            FROM inv.suppliers sup
            WHERE ip.supplier_id IS NULL
              AND sup.fulfillment_model = 'warehouse'
            AND NOT EXISTS (
                SELECT 1 FROM inv.products other
                WHERE other.supplier_id = sup.supplier_id)
        """)
    conn.commit()
    log.info("Assigned %d products across %d vendors",
             len(assignments), len({a[0] for a in assignments}))
    return len(assignments)


# ---------------------------------------------------------------------------
# Short-ships (the receiving-dock path)
# ---------------------------------------------------------------------------

def record_short_ships(conn, cfg: Any, sim_dt: datetime,
                       store_location_id: str,
                       vendor_by_product: Dict[str, Dict],
                       short_rows: List[Dict], scenario=None) -> int:
    """
    Write `inv.short_ship_events` for the short lines of ONE delivered load.

    Called by `transport.receive_delivered_loads`, which has just excluded
    those lines from the receipt — so this is the exact moment the vendor's
    failure becomes visible, with the store and the load both known.

    `short_rows` is what the caller already SELECTed, one dict per short line:
        {'item_id', 'fulfillment_id', 'product_id',
         'quantity_requested', 'quantity_picked', 'unit_cost'}

    `fulfillment_id` is per ROW, not a single argument: one truck load groups
    several store orders by destination, so its short lines can belong to
    different `fulfillment.orders` — and the event has to point at the pick
    that was actually short, or the row cannot be reconciled against the
    fulfillment it came from.

    A line with no vendor on disk cannot be blamed on anyone, so it is skipped
    rather than written with a NULL supplier_id (the column is NOT NULL): the
    pre-t_57b1a1ab data dirs produce exactly that, and skipping is the honest
    answer — there is no vendor to credit.
    """
    if not short_rows:
        return 0

    tag = getattr(scenario, 'scenario_tag', None) if scenario else None
    records = []
    for row in short_rows:
        vendor = vendor_by_product.get(str(row['product_id']))
        if not vendor:
            continue
        requested = float(row['quantity_requested'])
        picked = float(row['quantity_picked'])
        short = short_quantity(requested, picked)
        if short < MIN_SHORT_UNITS:
            continue
        unit_cost = round(float(row['unit_cost']), 4)
        promised = max(0, int(round(float(vendor['lead_time_mean_days']))))
        records.append((
            str(row['item_id']),
            str(row['fulfillment_id']),
            vendor['supplier_id'],
            str(row['product_id']),
            str(store_location_id),
            DETECTED_RECEIVING,
            round(requested, 3),
            round(picked, 3),
            short,
            unit_cost,
            round(short * unit_cost, 2),
            promised,
            # The realised lead time of THIS arrival: the promise plus the
            # vendor's own spread. Recorded on the event rather than read back
            # off the product because one product arrives many times.
            draw_lead_time_days(vendor),
            bool(vendor['credit_eligible']),
            sim_dt,
            tag,
        ))

    if not records:
        return 0

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO inv.short_ship_events
                (fulfillment_item_id, fulfillment_id, supplier_id, product_id,
                 location_id, detected_source, quantity_requested,
                 quantity_picked, quantity_short, unit_cost, short_value,
                 promised_lead_time_days, realized_lead_time_days,
                 is_creditable, event_dt, scenario_tag)
            VALUES %s
        """, records, template=SHORT_SHIP_ROW_TEMPLATE)
    conn.commit()
    log.info("Short-ships: %d line(s) at store %s", len(records),
             store_location_id)
    return len(records)


# ---------------------------------------------------------------------------
# Credit memos
# ---------------------------------------------------------------------------

def open_credit_memo(conn, cfg: Config, short_ship_id: str,
                     vendor: Dict, sim_dt: datetime,
                     rng: Optional[RNG] = None) -> Optional[str]:
    """
    Decide whether a short-ship becomes a CLAIM, and file it.

    Returns the memo id, or None when no claim is filed — which is a legitimate
    outcome, not a failure: the vendor's terms do not credit it, or the store
    decided the paperwork was not worth it. Both are real procurement
    decisions, and a mart that only ever saw filed claims would rate every
    vendor as perfect.

    One memo per short-ship, enforced by the UNIQUE on `short_ship_id`, so this
    is safe to call again for the same event (the second call finds no row and
    returns None rather than double-crediting).
    """
    if not vendor.get('credit_eligible'):
        return None
    if random.random() >= float(getattr(cfg.vendors, 'credit_claim_rate', 0.7)):
        return None

    with conn.cursor() as cur:
        cur.execute("""
            SELECT se.short_ship_id::text, se.product_id::text, se.location_id::text,
                   se.quantity_short, se.unit_cost, se.short_value
            FROM inv.short_ship_events se
            WHERE se.short_ship_id = %s::uuid
              AND se.is_creditable
              AND NOT EXISTS (
                    SELECT 1 FROM inv.supplier_credit_memos m
                    WHERE m.short_ship_id = se.short_ship_id)
        """, (short_ship_id,))
        row = cur.fetchone()

    if not row:
        return None

    short_ship_id, product_id, location_id, quantity, unit_cost, short_value = row
    if float(quantity) <= 0:
        return None

    reason = draw_short_reason(rng)
    deadline = (sim_dt.date() if hasattr(sim_dt, 'date') else sim_dt) \
        + timedelta(days=claim_deadline_days(vendor))
    memo_number = 'CM-%s' % uuid4().hex[:10].upper()

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO inv.supplier_credit_memos
                (credit_memo_number, short_ship_id, supplier_id, product_id,
                 location_id, credit_quantity, unit_cost, credit_amount,
                 short_reason, memo_status, claim_deadline, scenario_tag)
            VALUES (%s, %s::uuid, %s::uuid, %s::uuid, %s::uuid, %s, %s, %s,
                    %s, 'open', %s::date, %s)
            RETURNING credit_memo_id::text
        """, (memo_number, short_ship_id, vendor['supplier_id'], product_id,
              location_id, round(float(quantity), 3), round(float(unit_cost), 4),
              round(float(short_value), 2), reason, deadline,
              getattr(rng, '_scenario_tag', None) if rng else None))
        memo_id = cur.fetchone()[0]
    conn.commit()
    log.info("Credit memo %s opened: %.2f on %s (%s)",
             memo_number, float(short_value), vendor['supplier_name'], reason)
    return memo_id


def advance_credit_memos(conn, cfg: Config, sim_dt: datetime,
                         scenario=None) -> Dict[str, int]:
    """
    One daily pass over the open claims: submit, chase, pay or dispute.

    Runs once per simulated day, in the same block as `returns`, because the
    memo lifecycle is a days-long process — advancing it per tick would resolve
    a 10-day settlement inside 10 ticks and make the "days to pay" column
    meaningless.

    The three transitions:
      open       -> submitted  inside the claim window (usually), else expired
      submitted  -> paid | rejected  once the vendor's settlement time has
                                    passed and the chase delay is up

    An `open` memo past its deadline EXPIRES rather than being submitted: the
    vendor's terms have closed the window, which is exactly the finding a
    claims-aging mart exists to surface, so it is recorded instead of being
    silently prevented from ever existing.
    """
    today = sim_dt.date() if hasattr(sim_dt, 'date') else sim_dt
    chase_after = max(0, int(getattr(cfg.vendors, 'credit_chase_after_days', 3)))
    tag = getattr(scenario, 'scenario_tag', None) if scenario else None
    counts = {'submitted': 0, 'paid': 0, 'rejected': 0, 'expired': 0}

    # 1) open -> submitted, or open -> expired
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.credit_memo_id::text, m.claim_deadline, m.created_at
            FROM inv.supplier_credit_memos m
            JOIN inv.suppliers s ON s.supplier_id = m.supplier_id
            WHERE m.memo_status = 'open'
              AND s.is_active
              AND m.claim_deadline <= %s::date
        """, (today,))
        expired = cur.fetchall()
        for memo_id, deadline, _created in expired:
            cur.execute("""
                UPDATE inv.supplier_credit_memos
                SET memo_status = 'expired', updated_at = NOW()
                WHERE credit_memo_id = %s::uuid AND memo_status = 'open'
            """, (memo_id,))
            counts['expired'] += cur.rowcount

        cur.execute("""
            SELECT m.credit_memo_id::text, m.claim_deadline
            FROM inv.supplier_credit_memos m
            JOIN inv.suppliers s ON s.supplier_id = m.supplier_id
            WHERE m.memo_status = 'open'
              AND s.is_active
              AND m.claim_deadline > %s::date
        """, (today,))
        # Only claim what the vendor's terms actually credit — and a claim is
        # filed the same day the short-ship was found, so these are all fresh.
        for memo_id, deadline in cur.fetchall():
            if random.random() >= float(getattr(cfg.vendors, 'credit_claim_rate', 0.7)):
                continue
            cur.execute("""
                UPDATE inv.supplier_credit_memos
                SET memo_status = 'submitted', submitted_dt = %s,
                    scenario_tag = COALESCE(scenario_tag, %s), updated_at = NOW()
                WHERE credit_memo_id = %s::uuid AND memo_status = 'open'
            """, (sim_dt, tag, memo_id))
            counts['submitted'] += cur.rowcount

    # 2) submitted -> paid | rejected. Only claims past the chase delay are
    #    resolved, so a vendor who answers in two days looks responsive.
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.credit_memo_id::text, m.short_reason, m.submitted_dt,
                   s.credit_settle_mean_days, s.credit_settle_stddev_days
            FROM inv.supplier_credit_memos m
            JOIN inv.suppliers s ON s.supplier_id = m.supplier_id
            WHERE m.memo_status = 'submitted'
              AND s.is_active
              AND m.submitted_dt <= %s
        """, (sim_dt - timedelta(days=chase_after),))
        pending = cur.fetchall()

    for memo_id, reason, submitted_dt, mean, stddev in pending:
        vendor = {'credit_settle_mean_days': mean,
                  'credit_settle_stddev_days': stddev}
        settle_at = submitted_dt + timedelta(days=settlement_days(vendor))
        if sim_dt < settle_at:
            continue  # still inside the vendor's payment terms

        pay_rate, rejection_reason = REASON_PAY_PROFILE.get(
            reason, REASON_PAY_PROFILE['warehouse_shortage'])
        if random.random() < pay_rate:
            cur.execute("""
                UPDATE inv.supplier_credit_memos
                SET memo_status = 'paid', resolved_dt = %s,
                    resolved_by = %s, updated_at = NOW()
                WHERE credit_memo_id = %s::uuid AND memo_status = 'submitted'
            """, (sim_dt, RESOLVER_VENDOR, memo_id))
            counts['paid'] += cur.rowcount
        else:
            cur.execute("""
                UPDATE inv.supplier_credit_memos
                SET memo_status = 'rejected', resolved_dt = %s,
                    resolved_by = %s, rejection_reason = %s, updated_at = NOW()
                WHERE credit_memo_id = %s::uuid AND memo_status = 'submitted'
            """, (sim_dt, RESOLVER_VENDOR, rejection_reason, memo_id))
            counts['rejected'] += cur.rowcount

    conn.commit()
    if any(counts.values()):
        log.info("Credit memos: %d submitted, %d paid, %d rejected, %d expired",
                 counts['submitted'], counts['paid'], counts['rejected'],
                 counts['expired'])
    return counts


# ---------------------------------------------------------------------------
# Direct Store Delivery
# ---------------------------------------------------------------------------

def generate_dsd_deliveries(conn, cfg: Config, sim_dt: datetime,
                            vendors: List[Dict], store_locations: List[Dict],
                            products: List[Dict], scenario=None
                            ) -> Dict[str, int]:
    """
    Run the DSD vendors' drops for one simulated day.

    A DSD vendor restocks the shelf directly on its own schedule
    (`inv.supplier_delivery_schedules`), so this is a DELIVERY — there is no
    pallet on one of our `transport.loads`, no receiving dock, and deliberately
    no `inv.receipts` row. Modelling it as a receipt would double-count the
    goods once on our truck and once on the vendor's.

    It also runs the DSD half of the short-ship path: a line the vendor did not
    bring is found on the shelf (detected_source = 'dsd_delivery'), credited
    against the vendor's terms on the spot, and restocked only for what
    actually arrived.

    Idempotent per (schedule, delivery_date) — the UNIQUE is not on the delivery
    table, so idempotence is enforced by a NOT EXISTS on the schedule's weekday,
    which is what a re-run of the same day re-derives.
    """
    today = sim_dt.date() if hasattr(sim_dt, 'date') else sim_dt
    weekday = today.weekday()
    tag = getattr(scenario, 'scenario_tag', None) if scenario else None
    counts = {'deliveries': 0, 'lines': 0, 'short_lines': 0}

    if not vendors or not store_locations or not products:
        return counts

    with conn.cursor() as cur:
        cur.execute("""
            SELECT ds.schedule_id::text, ds.supplier_id::text,
                   ds.location_id::text, s.fulfillment_model, s.supplier_name
            FROM inv.supplier_delivery_schedules ds
            JOIN inv.suppliers s ON s.supplier_id = ds.supplier_id
            WHERE ds.delivery_weekday = %s AND ds.is_active AND s.is_active
        """, (weekday,))
        drops = cur.fetchall()

    if not drops:
        return counts

    # The SKUs each vendor owns. One read for the whole pass.
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ip.product_id::text, ip.supplier_id::text, ip.unit_of_measure,
                   p.cost
            FROM inv.products ip
            JOIN pos.products p ON p.product_id = ip.product_id
            WHERE ip.supplier_id IS NOT NULL
        """)
        catalogue = cur.fetchall()
    by_supplier: Dict[str, List[Tuple[str, str, float]]] = {}
    for product_id, supplier_id, _uom, cost in catalogue:
        by_supplier.setdefault(str(supplier_id), []).append(
            (str(product_id), str(supplier_id), float(cost)))

    vendor_by_id = {str(v['supplier_id']): v for v in vendors}
    already = set()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.schedule_id::text
            FROM inv.dsd_deliveries d
            WHERE d.delivery_date = %s
        """, (today,))
        already = {row[0] for row in cur.fetchall()}

    for schedule_id, supplier_id, location_id, model, supplier_name in drops:
        if schedule_id in already:
            continue
        vendor = vendor_by_id.get(str(supplier_id))
        if not vendor:
            continue
        catalogue_lines = by_supplier.get(str(supplier_id))
        if not catalogue_lines:
            continue

        # A drop is a top-up, not a full catalogue: a handful of lines the
        # vendor actually brings on this run.
        wanted = min(len(catalogue_lines),
                     random.randint(2, 6))
        chosen = random.sample(catalogue_lines, wanted)

        delivery_id = str(uuid4())
        delivered_hour = random.choice([5, 6, 7, 8, 9])
        delivered_at = datetime(today.year, today.month, today.day,
                                delivered_hour, 0, 0)
        # A DSD truck that misses its window is short by the whole drop.
        missed = random.random() < float(
            getattr(cfg.vendors, 'dsd_short_ship_bonus', 0.0) or 0.0)

        delivery_items = []
        short_rows = []
        total_units = 0.0
        total_value = 0.0
        for product_id, supplier_id_ref, cost in chosen:
            unit_cost = round(cost, 4)
            if missed:
                quantity = 0.0
            else:
                quantity = float(random.randint(6, 48))
            line_total = round(quantity * unit_cost, 2)
            total_units += quantity
            total_value += line_total
            delivery_items.append((delivery_id, product_id, quantity,
                                   unit_cost, line_total))

            # What the vendor PROMISED on this run, against what it did. A zero
            # delivery is the whole drop missed, which is the extreme case of the
            # same arithmetic rather than a separate branch.
            promised = draw_lead_time_days(vendor)
            short = short_quantity(quantity, quantity if not missed else 0.0)
            if short >= MIN_SHORT_UNITS:
                short_rows.append({
                    'product_id': product_id,
                    'quantity_requested': round(quantity, 3),
                    'quantity_picked': 0.0,
                    'unit_cost': unit_cost,
                    'promised': promised,
                })

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO inv.dsd_deliveries
                    (dsd_delivery_id, schedule_id, supplier_id, location_id,
                     delivery_date, delivered_at, total_units, total_value,
                     line_count)
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, %s::date,
                        %s, %s, %s, %s)
            """, (delivery_id, schedule_id, supplier_id, location_id, today,
                  delivered_at, round(total_units, 3), round(total_value, 2),
                  len(delivery_items)))

            if delivery_items:
                execute_values(cur, """
                    INSERT INTO inv.dsd_delivery_items
                        (dsd_delivery_id, product_id, quantity_delivered,
                         unit_cost, line_total)
                    VALUES %s
                """, delivery_items, template=DSD_ITEM_ROW_TEMPLATE)

            # Stock the shelf for what actually arrived. A DSD line has no
            # load_id and no receipt, so this is where a DSD restock happens.
            if total_units > 0:
                execute_values(cur, """
                    UPDATE inv.stock_levels sl
                    SET quantity_on_hand = sl.quantity_on_hand + v.qty,
                        last_updated = NOW()
                    FROM (VALUES %s) AS v(product_id, qty)
                    WHERE sl.product_id = v.product_id::uuid
                      AND sl.location_id = %s::uuid
                """, [(item[1], item[2]) for item in delivery_items
                      if item[2] > 0],
                    template="(%s::uuid,%s::numeric)")

        counts['deliveries'] += 1
        counts['lines'] += len(delivery_items)

        # 2) The DSD short-ship path.
        for row in short_rows:
            short = short_quantity(row['quantity_requested'],
                                   row['quantity_picked'])
            if short < MIN_SHORT_UNITS:
                continue
            unit_cost = row['unit_cost']
            with conn.cursor() as cur2:
                cur2.execute("""
                    INSERT INTO inv.short_ship_events
                        (fulfillment_item_id, fulfillment_id, supplier_id,
                         product_id, location_id, detected_source,
                         quantity_requested, quantity_picked, quantity_short,
                         unit_cost, short_value, promised_lead_time_days,
                         realized_lead_time_days, is_creditable, event_dt,
                         scenario_tag)
                    SELECT %s::uuid, %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    RETURNING short_ship_id::text
                """, ('00000000-0000-0000-0000-%012d'
                       % random.randrange(0, 10 ** 12),
                       '00000000-0000-0000-0000-%012d'
                       % random.randrange(0, 10 ** 12),
                       supplier_id, row['product_id'], location_id,
                       DETECTED_DSD, row['quantity_requested'],
                       row['quantity_picked'], short, unit_cost,
                       round(short * unit_cost, 2), row['promised'],
                       draw_lead_time_days(vendor),
                       bool(vendor['credit_eligible']), delivered_at, tag))
                short_ship_id = cur2.fetchone()[0]
            conn.commit()

            memo_id = open_credit_memo(conn, cfg, short_ship_id, vendor,
                                       delivered_at)
            if memo_id:
                counts['short_lines'] += 1

    conn.commit()
    if counts['deliveries']:
        log.info("DSD: %d drop(s), %d line(s), %d short line(s) credited",
                 counts['deliveries'], counts['lines'], counts['short_lines'])
    return counts


# ---------------------------------------------------------------------------
# Lookups shared with the receipt path
# ---------------------------------------------------------------------------

def vendor_by_product(conn) -> Dict[str, Dict]:
    """
    `{product_id: vendor_row}` for every product that has one.

    Read ONCE per receiving pass and handed to `record_short_ships`, rather
    than looked up per short line: a load can carry dozens of lines and this is
    the query that would otherwise run once per short one.
    """
    vendors = fetch_suppliers(conn)
    by_id = {str(v['supplier_id']): v for v in vendors}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT product_id::text, supplier_id::text
            FROM inv.products
            WHERE supplier_id IS NOT NULL
        """)
        rows = cur.fetchall()
    return {
        product_id: by_id[supplier_id]
        for product_id, supplier_id in rows
        if supplier_id in by_id
    }


def credit_memo_summary(conn) -> List[Tuple[str, int, int]]:
    """
    Per supplier: (supplier_name, total credited, still open).

    The generator's own sanity check that the lifecycle produced something
    measurable — a mart can then be built on the same shape.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.supplier_name,
                   COUNT(*) FILTER (WHERE m.memo_status = 'paid'),
                   COUNT(*) FILTER (WHERE m.memo_status IN ('open','submitted'))
            FROM inv.supplier_credit_memos m
            JOIN inv.suppliers s ON s.supplier_id = m.supplier_id
            GROUP BY s.supplier_name
            ORDER BY s.supplier_name
        """)
        return cur.fetchall()