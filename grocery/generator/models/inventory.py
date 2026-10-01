"""
Inventory model — seeds stock levels, allocates stock to sales, records stockouts.

Restocking is handled by the ordering → fulfillment → transport → receipt
pipeline (transport.receive_delivered_loads). This module seeds initial stock,
resolves each sale line against what the shelf actually holds, and records what
was lost when a shelf came up short (t_959cd040).

Before t_959cd040 the write path was:

    UPDATE inv.stock_levels
    SET quantity_on_hand = GREATEST(0, quantity_on_hand - %s)

while the sale itself was written at the FULL requested quantity. Two things
were wrong at once, and fixing only one of them would be worse than either:

  * `GREATEST(0, ...)` silently absorbed the shortage — stock floored at zero
    and the sale kept its full quantity, so `pos.transaction_items` reported
    units that were never on the shelf. Measured on the dev DB: 249 of 1485
    store-SKU rows (16.8%) pinned at zero with ~600k transactions still selling
    against them over the following 7 days.
  * `reorder_point`, `reorder_qty` and `restock_threshold_pct` were decorative.
    A shortage could not be observed, so nothing could respond to one.

So the shortage is now the unit of work. `apply_sales` resolves a batch of sale
lines against the shelf, caps each line at what is on hand, writes the shortfall
to `inv.stockout_events` and the requested/fulfilled split to
`inv.sku_demand_daily`, and decrements. The POS and online models call it with
the lines they are about to write and cap their own line quantities from what it
returns, so the sale on disk is the sale the shelf could actually cover.
"""
import logging
from typing import Dict, Iterable, List, Optional, Tuple

from psycopg2.extras import execute_values

from config import Config

log = logging.getLogger(__name__)

# What each channel is called in inv.stockout_events.channel.
CHANNEL_POS = 'pos'
CHANNEL_ONLINE = 'online'

SkuKey = Tuple[str, str]  # (location_id, product_id)

# Row templates for the batched writes. Named so a test can assert each one has
# exactly as many placeholders as its INSERT has columns and its row tuple has
# fields — a mismatch is invisible until a real stockout fires, which is how the
# stockout template shipped with 11 placeholders for 12 columns (found by the
# wipe+reseed probe, t_959cd040).
STOCKOUT_ROW_TEMPLATE = (
    "(%s::uuid,%s::uuid,%s,%s::uuid,%s::uuid,"
    "%s,%s,%s,%s,%s,%s,%s)"
)
SKU_DEMAND_ROW_TEMPLATE = "(%s::uuid,%s::uuid,%s,%s,%s,%s,%s,%s)"
STOCK_LEVEL_DECREMENT_TEMPLATE = "(%s::uuid,%s::uuid,%s::numeric)"

# How many columns each of those templates writes, for the parity test.
STOCKOUT_COLUMN_COUNT = 12
SKU_DEMAND_COLUMN_COUNT = 8
STOCK_LEVEL_DECREMENT_COLUMN_COUNT = 3


def seed_inventory(conn, cfg: Config, products: List[Dict],
                   store_locations: List[Dict]) -> None:
    """
    Create inv.products and inv.stock_levels for all product/store combos.
    Only seeds stock at store locations (warehouse stock managed separately).
    Idempotent.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM inv.products")
        if cur.fetchone()[0] > 0:
            return

    log.info("Seeding inventory for %d products × %d stores...",
             len(products), len(store_locations))

    import random
    SUPPLIERS = [
        'UNFI', 'KeHE Distributors', 'McLane Company',
        'C&S Wholesale Grocers', 'Nash Finch', 'Supervalu'
    ]

    inv_prod_records = []
    for p in products:
        inv_prod_records.append((
            p['product_id'],
            random.randint(15, 40),    # reorder_point
            random.randint(50, 300),   # reorder_qty
            p.get('uom', 'each'),
            random.choice(SUPPLIERS),
            random.randint(1, 4),      # lead_time_days
        ))

    stock_records = []
    for p in products:
        for loc in store_locations:
            stock_records.append((
                p['product_id'],
                loc['location_id'],
                cfg.inventory.initial_stock_per_product,
                0,
            ))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO inv.products
                (product_id, reorder_point, reorder_qty, unit_of_measure,
                 supplier_name, lead_time_days)
            VALUES %s ON CONFLICT (product_id) DO NOTHING
        """, inv_prod_records, template="(%s::uuid,%s,%s,%s,%s,%s)")

        execute_values(cur, """
            INSERT INTO inv.stock_levels
                (product_id, location_id, quantity_on_hand, quantity_reserved)
            VALUES %s ON CONFLICT (product_id, location_id) DO NOTHING
        """, stock_records, template="(%s::uuid,%s::uuid,%s,%s)")

    conn.commit()
    log.info("Seeded inventory")


def refresh_perishable_expiry_on_receipt(conn, location_id: str,
                                          product_ids: List[str],
                                          received_date) -> None:
    """
    When new stock arrives via receipt, update expiry_date for restocked
    perishable items. The clock resets to received_date + shelf_life_days.
    Called by transport.receive_delivered_loads after updating stock levels.
    """
    if not product_ids:
        return
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE inv.stock_levels sl
            SET    expiry_date  = (%s::date + p.shelf_life_days),
                   last_updated = NOW()
            FROM   pos.products p
            WHERE  sl.product_id   = p.product_id
              AND  p.is_perishable = TRUE
              AND  p.shelf_life_days IS NOT NULL
              AND  sl.location_id  = %s::uuid
              AND  sl.product_id   = ANY(%s::uuid[])
        """, (received_date, location_id, product_ids))
    conn.commit()


# ---------------------------------------------------------------------------
# Stock-aware allocation (t_959cd040)
# ---------------------------------------------------------------------------
# POS and online both sell the SAME shelf in the same tick, so the allowance has
# to be shared: if each channel asked the database independently, in-store
# shoppers and online orders would each be told they could have the last unit.
# A tick therefore opens one `StockAllowance` covering both channels, hands the
# lines it is about to write to `take()`, and the returned quantities are what
# the sale rows carry.
#
# The write path has three steps, in this order, and the order matters:
#
#   1. build the lines (price, quantity) — the demand, before any stock check
#   2. `allowance.take(...)` — cap each line at what the shelf can cover
#   3. write the sale rows at the capped quantities, and let the allowance
#      commit the decrement + stockout rows + daily demand ledger
#
# Step 2 must precede step 3: revenue has to be booked on what the shopper
# actually got, or the tax/discount/loyalty-point arithmetic all describes a
# basket that never happened.


class StockAllowance:
    """
    The stock a single tick is allowed to sell, per store-SKU.

    Created once per tick with the on-hand snapshot; `take()` is called by each
    channel for every line it wants to write and returns the quantity that line
    may actually sell, reserving it so a later line in the same tick cannot be
    handed the same unit.
    """

    def __init__(self, on_hand_by_sku: Dict[SkuKey, float]):
        self._remaining = {key: float(qty) for key, qty in on_hand_by_sku.items()}
        self._reserved: Dict[SkuKey, float] = {}
        # Every line asked for, and what it was granted, in call order. main
        # flushes this once per tick: the stockout rows and the daily demand
        # ledger are derived from it, so the channels themselves keep their
        # original return contract (a plain depletion list).
        self.journal: List[Dict] = []

    @classmethod
    def from_lines(cls, conn, lines: List[Dict]) -> 'StockAllowance':
        """Snapshot on-hand for every store-SKU the batch touches (one read)."""
        if not lines:
            return cls({})
        location_ids = sorted({str(l['location_id']) for l in lines})
        product_ids = sorted({str(l['product_id']) for l in lines})
        with conn.cursor() as cur:
            cur.execute("""
                SELECT sl.location_id::text, sl.product_id::text,
                       sl.quantity_on_hand::numeric
                FROM inv.stock_levels sl
                WHERE sl.location_id = ANY(%s::uuid[])
                  AND sl.product_id  = ANY(%s::uuid[])
            """, (location_ids, product_ids))
            rows = cur.fetchall()
        return cls({(str(loc), str(prod)): float(qty) for loc, prod, qty in rows})

    def available(self, location_id: str, product_id: str) -> float:
        """Units still sellable by this tick for a store-SKU."""
        key = (str(location_id), str(product_id))
        return self._remaining.get(key, 0.0)

    def take(self, lines: List[Dict], channel: str = '') -> Dict[str, float]:
        """
        Reserve stock for `lines`; returns {line_id: granted_quantity}.

        Lines are served in the order given. Each line is granted
        `min(requested, still available)` and reserves it, so two lines for the
        same SKU in one tick split the last units between them rather than each
        taking all of them. Rounded to 3dp — the precision of
        pos.transaction_items.quantity.

        Pure arithmetic on the snapshot; nothing is written until `flush()`.
        """
        granted: Dict[str, float] = {}
        for line in lines:
            key = (str(line['location_id']), str(line['product_id']))
            available = self._remaining.get(key, 0.0)
            give = min(float(line['quantity']), max(0.0, available))
            self._remaining[key] = max(0.0, available - give)
            self._reserved[key] = self._reserved.get(key, 0.0) + give
            granted[str(line['line_id'])] = round(give, 3)
            self.journal.append({
                **line,
                'channel': line.get('channel') or channel,
                'requested': round(float(line['quantity']), 3),
                'granted': round(give, 3),
            })
        return granted

    def reserved_by_sku(self) -> Dict[SkuKey, float]:
        return dict(self._reserved)


def resolve_sales(
    allowance: StockAllowance,
    lines: List[Dict],
    channel: str = CHANNEL_POS,
) -> Tuple[Dict[str, float], List[Dict]]:
    """
    Cap `lines` against the tick's stock allowance.

    Returns (granted_by_line_id, capped_lines). `capped_lines` is the input with
    `quantity` replaced by what the shopper can actually get — the caller writes
    its sale rows from THIS list, so the revenue, the stock decrement and the
    loss record all describe the same basket.

    A line the shelf cannot cover at all (granted 0) is dropped from
    `capped_lines`: pos.transaction_items and online.order_items both carry
    `CHECK (quantity > 0)`, so a zero-quantity row is not writable at all. Its
    loss is still recorded, because `take()` journals the request before the
    drop. Keeping the drop narrow — only granted == 0 — means an empty basket
    drops out too, and the caller skips writing that sale rather than writing a
    header with no lines.
    """
    granted = allowance.take(lines, channel)
    capped = []
    for line in lines:
        quantity = granted.get(str(line['line_id']), 0.0)
        if quantity <= 0:
            continue
        capped.append({**line, 'quantity': quantity})
    return granted, capped


def flush_sales(conn, allowance: 'StockAllowance', sim_dt,
                scenario_tag: Optional[str] = None,
                fulfilled_parents: Optional[Iterable[str]] = None) -> int:
    """
    Persist a tick's resolved sales: decrement the shelf, write
    `inv.stockout_events` for every short line, and accumulate the
    requested/fulfilled split into `inv.sku_demand_daily`. Returns the number of
    stockout lines written.

    Driven by `allowance.journal`, so one flush covers both channels for the
    whole tick and the two never double-count the same line. Idempotent per
    call and safe to call with an empty journal.

    `fulfilled_parents` is the set of transaction/order ids the caller actually
    wrote a sale row for. A basket the shelf could not cover AT ALL is dropped
    by the caller (pos.transaction_items has CHECK (quantity > 0), so an empty
    basket is unwritable) — that line still appears here as a lost sale, but
    with no parent, because there is no sale to point at. Without this the
    insert would fail the FK on a transaction that was never written.
    """
    journal = allowance.journal
    if not journal:
        return 0

    fulfilled_parents = set(fulfilled_parents or ())

    stockout_records = []
    ledger: Dict[SkuKey, List[float]] = {}
    deductions: Dict[SkuKey, float] = {}

    for entry in journal:
        key = (str(entry['location_id']), str(entry['product_id']))
        channel = entry.get('channel') or CHANNEL_POS
        requested = entry['requested']
        fulfilled = min(entry['granted'], requested)
        lost = round(requested - fulfilled, 3)
        unit_price = float(entry.get('unit_price') or 0.0)

        deductions[key] = deductions.get(key, 0.0) + fulfilled

        row = ledger.get(key)
        if row is None:
            row = [0.0, 0.0, 0.0, 0.0, 0]
            ledger[key] = row
        row[0] += requested
        row[1] += fulfilled
        row[2] += lost
        row[3] += lost * unit_price
        row[4] += 1

        if lost <= 0:
            continue
        # Only point at a sale that was actually written. A basket the shelf
        # could not cover at all is dropped by the channel (its item rows
        # would violate CHECK (quantity > 0)), so its parent does not exist —
        # recording the walk-away with no parent is the honest row, and the
        # walk-away is the signal worth keeping.
        parent = str(entry.get('parent_id') or entry['line_id'])
        parent = parent if parent in fulfilled_parents else None
        stockout_records.append((
            str(entry['product_id']),
            str(entry['location_id']),
            channel,
            parent if channel == CHANNEL_POS else None,
            parent if channel == CHANNEL_ONLINE else None,
            requested,
            round(fulfilled, 3),
            lost,
            round(unit_price, 4),
            round(lost * unit_price, 2),
            sim_dt,
            scenario_tag,
        ))

    demand_date = sim_dt.date() if hasattr(sim_dt, 'date') else sim_dt
    ledger_rows = [
        (loc, prod, demand_date, round(req, 3), round(ful, 3),
         round(lost, 3), round(value, 2), int(count))
        for (loc, prod), (req, ful, lost, value, count) in ledger.items()
    ]

    with conn.cursor() as cur:
        # Set-based decrement: one UPDATE per batch, not one per line. The
        # GREATEST is belt-and-braces — allocation already bounded every line —
        # and only ever fires if another writer moved the row in between.
        if deductions:
            execute_values(cur, """
                UPDATE inv.stock_levels sl
                SET quantity_on_hand = GREATEST(
                        0, sl.quantity_on_hand - v.deduct),
                    last_updated = NOW()
                FROM (VALUES %s) AS v(location_id, product_id, deduct)
                WHERE sl.location_id = v.location_id::uuid
                  AND sl.product_id  = v.product_id::uuid
            """, [(loc, prod, round(qty, 3)) for (loc, prod), qty in deductions.items()],
                template=STOCK_LEVEL_DECREMENT_TEMPLATE)

        if stockout_records:
            execute_values(cur, """
                INSERT INTO inv.stockout_events
                    (product_id, location_id, channel, pos_transaction_id,
                     online_order_id, requested_quantity, fulfilled_quantity,
                     lost_quantity, unit_price, lost_value, event_dt, scenario_tag)
                VALUES %s
            """, stockout_records,
                template=STOCKOUT_ROW_TEMPLATE)

        if ledger_rows:
            # ON CONFLICT DO UPDATE rather than a blind insert: the ledger is a
            # running accumulator per store-SKU-day, not an event log, and a
            # realtime tick and a backfill hour can land on the same day. The
            # adds are commutative, so the total is the same either way.
            execute_values(cur, """
                INSERT INTO inv.sku_demand_daily
                    (location_id, product_id, demand_date, requested_units,
                     fulfilled_units, lost_units, lost_value, line_count)
                VALUES %s
                ON CONFLICT (location_id, product_id, demand_date) DO UPDATE SET
                    requested_units = inv.sku_demand_daily.requested_units + EXCLUDED.requested_units,
                    fulfilled_units = inv.sku_demand_daily.fulfilled_units + EXCLUDED.fulfilled_units,
                    lost_units     = inv.sku_demand_daily.lost_units     + EXCLUDED.lost_units,
                    lost_value     = inv.sku_demand_daily.lost_value     + EXCLUDED.lost_value,
                    line_count     = inv.sku_demand_daily.line_count     + EXCLUDED.line_count,
                    last_updated   = NOW()
            """, ledger_rows,
                template=SKU_DEMAND_ROW_TEMPLATE)

    conn.commit()
    allowance.journal.clear()

    if stockout_records:
        log.info("Stockout: %d line(s) short — %.1f units / $%.2f lost @ %s",
                 len(stockout_records),
                 sum(r[7] for r in stockout_records),
                 sum(r[9] for r in stockout_records),
                 sim_dt)
    return len(stockout_records)


def deplete_inventory(conn, depletion_info: List[Dict]) -> None:
    """
    Reduce inv.stock_levels for items sold in a batch of POS transactions, from
    the quantities ALREADY written on pos.transaction_items.

    Kept as the "apply what was written" step for any caller that writes a sale
    directly; the POS and online paths now resolve stock first (see
    `StockAllowance.take` / `flush_sales`) and deduct through the allowance. A
    caller that skips the resolve step still gets the old flooring behaviour
    from here rather than over-drawing the shelf.
    """
    if not depletion_info:
        return

    txn_ids = [d['transaction_id'] for d in depletion_info]
    placeholders = ','.join(['%s::uuid'] * len(txn_ids))

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT t.location_id, ti.product_id, SUM(ti.quantity)::numeric as total_qty
            FROM pos.transactions t
            JOIN pos.transaction_items ti ON ti.transaction_id = t.transaction_id
            WHERE t.transaction_id IN ({placeholders})
            GROUP BY t.location_id, ti.product_id
        """, txn_ids)
        rows = cur.fetchall()

    if not rows:
        return

    with conn.cursor() as cur:
        for loc_id, prod_id, qty in rows:
            cur.execute("""
                UPDATE inv.stock_levels
                SET quantity_on_hand = GREATEST(0, quantity_on_hand - %s),
                    last_updated = NOW()
                WHERE product_id = %s::uuid AND location_id = %s::uuid
            """, (int(qty), str(prod_id), str(loc_id)))
    conn.commit()


def deplete_online_inventory(conn, depletion_info: List[Dict]) -> None:
    """
    Reduce inv.stock_levels for items in a batch of ONLINE orders, from the
    quantities ALREADY written on online.order_items (t_24fae529 — online demand
    pulls the same store shelves). Same seam as `deplete_inventory`.
    """
    if not depletion_info:
        return

    order_ids = [d['transaction_id'] for d in depletion_info]

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE inv.stock_levels sl
            SET quantity_on_hand = GREATEST(0, sl.quantity_on_hand - v.qty),
                last_updated = NOW()
            FROM (
                SELECT o.location_id, oi.product_id,
                       SUM(oi.quantity)::numeric AS qty
                FROM online.orders o
                JOIN online.order_items oi ON oi.order_id = o.order_id
                WHERE o.order_id = ANY(%s::uuid[])
                GROUP BY 1, 2
            ) v
            WHERE sl.location_id = v.location_id AND sl.product_id = v.product_id
        """, (order_ids,))
    conn.commit()