"""
The daily vendor phase must actually WRITE a DSD drop — kanban t_5a16106b.

THE DEFECT
----------
`suppliers.generate_dsd_deliveries` raised on every simulated day, and
`main._run_vendor_phase` swallowed it:

    2026-10-25T13:37:38 ERROR grocery-generator — Vendor phase failed at
    2026-10-24 23:30:00 — continuing
    ...
    File "/app/generator/models/suppliers.py", line 923, in generate_dsd_deliveries
        execute_values(cur, \"\"\"
    ValueError: the query contains more than one '%s' placeholder

The shelf-restock statement carried the store as a second `%s`:

    FROM (VALUES %s) AS v(product_id, qty)
    WHERE sl.product_id = v.product_id::uuid
      AND sl.location_id = %s::uuid

`execute_values` substitutes the `(VALUES %s)` slot itself, and psycopg2's
`_split_sql` refuses a statement that carries any other placeholder. So the
whole DSD pass died at the first drop, before the short-ship path and before
`advance_credit_memos` ran: `inv.dsd_deliveries`, `inv.dsd_delivery_items` and
the DSD shelf restock were empty on every seed.

WHY THIS TEST RUNS THE REAL `execute_values`
--------------------------------------------
A stub that merely records the rows would have stayed green through the defect:
the fault is a property of the *statement text* and only psycopg2 notices it.
So these tests hand the module's real `psycopg2.extras.execute_values` a fake
cursor (it calls `_split_sql` first, then `mogrify`/`execute` per page), which
reproduces the ValueError without a database. The fake cursor is also where the
composed statement is captured, so the test can assert the store travels inside
the VALUES row — the shape `returns.restock_returns` already uses.
"""
import logging
import random
from datetime import date, datetime, time

import psycopg2.extras
import pytest

from grocery.generator import main as gen_main
from grocery.generator.config import Config
from grocery.generator.models import suppliers

SIM_DAY = date(2026, 10, 19)          # a Monday, i.e. weekday 0
SIM_DT = datetime(2026, 10, 19, 23, 30, 0)
STORE = 'cccccccc-0000-0000-0000-000000000003'
OTHER_STORE = 'cccccccc-0000-0000-0000-000000000004'
SUPPLIER = '11111111-0000-0000-0000-000000000001'
SCHEDULE = '22222222-0000-0000-0000-000000000002'
# The window the schedule promises, i.e. `inv.supplier_delivery_schedules.
# delivery_window_start`. The drop must carry it (kanban t_120b25b0).
WINDOW_START = time(6, 0)

# Distinguishes "the caller did not pass this" from "the caller passed an empty
# list/dict", which is the difference between the default shelf and a vendor
# whose shelf has nothing below its reorder point.
_UNSET = object()

CATALOGUE = [
    ('aaaaaaaa-0000-0000-0000-00000000000%d' % i, SUPPLIER, 'EA', 1.25 + i)
    for i in range(1, 5)
]


def _vendor():
    return {
        'supplier_id': SUPPLIER,
        'supplier_name': 'FreshFields Produce',
        'fulfillment_model': 'dsd',
        'lead_time_mean_days': 1.0,
        'lead_time_stddev_days': 0.3,
        'short_ship_rate': 0.08,
        'credit_eligible': True,
        'credit_window_days': 7,
        'credit_settle_mean_days': 8,
        'credit_settle_stddev_days': 3,
    }


class _EncodingRef:
    """What `execute_values` reads off `cursor.connection` before it splits
    the statement (`_ext.encodings[cur.connection.encoding]`)."""
    encoding = 'UTF8'


class _RecordingCursor:
    """Answers `generate_dsd_deliveries`' SELECTs and captures every write.

    `mogrify` returns bytes because the real `execute_values` joins the per-row
    fragments into the statement it executes — the path that raises when the
    statement carries a second placeholder.
    """

    def __init__(self, db):
        self.db = db
        self.connection = _EncodingRef()
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        if isinstance(sql, bytes):
            sql = sql.decode('utf-8', 'replace')
        flat = ' '.join(sql.split())
        self.db.writes.append((flat, params))
        self._rows = self.db.answer(flat)
        return None

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def mogrify(self, template, args):
        return ('(' + ','.join(str(a) for a in args) + ')').encode()


class _FakeDB:
    """A grocery DB holding nothing but the DSD vendor's own rows.

    `shelf` is what the drop pass reads from `inv.stock_levels`: the store's
    lines for that vendor that are BELOW their reorder point (the query does the
    filtering; these are its results). Defaults to all four catalogue SKUs on the
    test store, so a test that does not care about coverage still ships lines.
    """

    def __init__(self, already=(), shelf=_UNSET, demand=None, visits=_UNSET):
        self.writes = []          # (statement, params) as `execute` saw them
        self.batches = []         # (sql, rows, template) as execute_values saw them
        self.already = set(already)
        self.commits = 0
        if shelf is _UNSET:
            shelf = [(STORE, product[0], SUPPLIER, 120, product[3])
                     for product in CATALOGUE]
        self.shelf = list(shelf)
        # {(location_id, product_id): units_per_day}
        self.demand = dict(demand or {})
        # {(supplier_id, location_id): visits_per_week}
        self.visits = ({(SUPPLIER, STORE): 4} if visits is _UNSET else dict(visits))

    def cursor(self, *args, **kwargs):
        return _RecordingCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks = getattr(self, 'rollbacks', 0) + 1

    def answer(self, flat):
        lowered = flat.lower()
        if 'from inv.supplier_delivery_schedules' in lowered:
            if 'delivery_weekday' in lowered:
                return [(SCHEDULE, SUPPLIER, STORE, 'dsd', 'FreshFields Produce',
                         WINDOW_START)]
            # The visits-per-week read (GROUP BY supplier, location).
            return [(supplier, location, per_week)
                    for (supplier, location), per_week in self.visits.items()]
        if 'from inv.stock_levels sl' in lowered:
            return list(self.shelf)
        if 'from inv.sku_demand_daily' in lowered:
            return [(location, product, units)
                    for (location, product), units in self.demand.items()]
        if 'from inv.dsd_deliveries d' in lowered:
            return [(schedule_id,) for schedule_id in self.already]
        if 'from inv.products ip' in lowered:
            return list(CATALOGUE)
        return []

    # -- helpers ---------------------------------------------------------
    def statements_containing(self, needle):
        return [sql for sql, _ in self.writes if needle in sql]

    def batch_for(self, needle):
        for sql, rows, template in self.batches:
            if needle in sql:
                return sql, rows, template
        return None


@pytest.fixture
def recording_execute_values(monkeypatch):
    """Record the arguments AND run psycopg2's real implementation."""
    real = psycopg2.extras.execute_values

    def _spy(cur, sql, argslist, template=None, **kwargs):
        db = getattr(cur, 'db', None)
        if db is not None:
            db.batches.append((sql, list(argslist), template))
        return real(cur, sql, argslist, template=template, **kwargs)

    monkeypatch.setattr(suppliers, 'execute_values', _spy)
    return _spy


def _run(db, sim_dt=SIM_DT, cfg=None, vendor=None):
    random.seed(20261019)
    return suppliers.generate_dsd_deliveries(
        db, cfg or Config(), sim_dt, [vendor or _vendor()], [{'location_id': STORE}],
        [{'product_id': product[0]} for product in CATALOGUE])


def _vendor_without_shorts():
    """A vendor that always brings the whole line — so a test about WHAT was
    shipped is not also a coin flip on what `short_probability_for` drew."""
    vendor = _vendor()
    vendor['short_ship_rate'] = 0.0
    return vendor


# ---------------------------------------------------------------------------
# The defect: the restock statement, under the real execute_values
# ---------------------------------------------------------------------------
def test_the_shelf_move_is_one_placeholder_statement(recording_execute_values):
    """`execute_values` owns the `(VALUES %s)` slot; nothing else may be `%s`.

    This is the assertion that was red before the fix — psycopg2's `_split_sql`
    raised `ValueError: the query contains more than one '%s' placeholder`
    against the old `AND sl.location_id = %s::uuid`, and the caller swallowed
    it, so the failure was a log line rather than a test failure.
    """
    db = _FakeDB()
    _run(db)

    batch = db.batch_for('UPDATE inv.stock_levels')
    assert batch is not None, 'the DSD drop never moved the shelf'
    sql, rows, template = batch

    assert sql.count('%s') == 1, (
        'execute_values substitutes exactly one placeholder — the (VALUES %%s) '
        'slot. This statement carries %d:\n%s' % (sql.count('%s'), sql))
    assert template.count('%s') == 3, (
        'one slot per row field: product_id, quantity, location_id')
    assert all(len(row) == 3 for row in rows)


def test_the_store_travels_inside_each_values_row(recording_execute_values):
    """The store is a row field, not a statement placeholder — and it is the
    store the drop belongs to, so the restock cannot land at another location.
    """
    db = _FakeDB()
    counts = _run(db)

    sql, rows, _template = db.batch_for('UPDATE inv.stock_levels')
    assert rows, 'a drop with units delivered moved no shelf rows'
    assert {row[2] for row in rows} == {STORE}, (
        'every VALUES row must carry the drop\'s location; got %r'
        % ({row[2] for row in rows},))
    assert 'v.location_id::uuid' in sql
    assert len(rows) == counts['lines']


def test_a_dsd_drop_writes_the_delivery_and_its_lines(recording_execute_values):
    """The card's contract, at the writer: a simulated day produces one
    `inv.dsd_deliveries` row per scheduled drop plus its item lines.
    """
    db = _FakeDB()
    counts = _run(db)

    inserts = db.statements_containing('INSERT INTO inv.dsd_deliveries')
    assert len(inserts) == 1, 'expected one delivery, wrote %d' % len(inserts)

    params = db.writes[[sql for sql, _ in db.writes].index(inserts[0])][1]
    assert params[1] == SCHEDULE
    assert params[2] == SUPPLIER
    assert params[3] == STORE
    assert params[4] == SIM_DAY
    assert params[9] == counts['lines'] > 0

    items = db.batch_for('INSERT INTO inv.dsd_delivery_items')
    assert items is not None, 'the drop wrote no delivery lines'
    _sql, item_rows, _template = items
    assert len(item_rows) == counts['lines']
    assert all(row[2] > 0 for row in item_rows), 'a DSD drop delivered nothing'


def _delivery_params(db):
    """The one delivery INSERT's parameter tuple, in column order."""
    header = db.statements_containing('INSERT INTO inv.dsd_deliveries')[0]
    return db.writes[[sql for sql, _ in db.writes].index(header)][1]


def test_a_dsd_drop_records_the_window_its_schedule_promised(
        recording_execute_values):
    """`inv.dsd_deliveries.scheduled_window` must arrive with the drop.

    The column existed in `schema.sql` and no writer ever set it, so all 12
    rows on CT107 read NULL — and "did the truck arrive inside the window it
    promised", the one question a drop and a schedule together exist to answer,
    could not be computed at all (kanban t_120b25b0). The window is not
    something the drop has to invent: it already references `schedule_id`, and
    that schedule carries `delivery_window_start`.
    """
    db = _FakeDB()
    _run(db)

    params = _delivery_params(db)
    assert params[6] == WINDOW_START, (
        "the drop did not carry its schedule's window: %r" % (params[6],))
    assert params[6] is not None, (
        'a NULL window is the defect this card exists to fix')


def test_the_drop_reads_its_window_from_the_schedule_it_references(
        recording_execute_values):
    """The value comes from the schedule row the drop selects, not from a
    constant and not from the arrival time — so it stays the promise the
    delivery is measured against even if the arrival band changes."""
    db = _FakeDB()
    _run(db)

    statements = db.statements_containing(
        'FROM inv.supplier_delivery_schedules')
    assert statements, 'the drop pass never read a schedule'
    assert 'delivery_window_start' in statements[0], (
        'the drop query does not read the schedule\'s window:\n%s'
        % statements[0])


def test_a_second_pass_over_the_same_day_writes_nothing(recording_execute_values):
    """`generate_dsd_deliveries` is called from BOTH the realtime midnight block
    and a backfill day's end-of-day block, so a replayed day must not write a
    second drop for a schedule it already delivered.
    """
    db = _FakeDB(already=[SCHEDULE])
    counts = _run(db)

    assert counts == {'deliveries': 0, 'lines': 0, 'short_lines': 0}
    assert db.statements_containing('INSERT INTO inv.dsd_deliveries') == []
    assert db.batch_for('UPDATE inv.stock_levels') is None


# ---------------------------------------------------------------------------
# The phase, as main.py runs it
# ---------------------------------------------------------------------------
def test_the_vendor_phase_writes_the_days_dsd_deliveries(monkeypatch):
    """`_run_vendor_phase` — the caller that swallowed the ValueError — must
    write the drop and report it, not return the empty dict it fell back to.
    """
    db = _FakeDB()
    # main.py imports `suppliers` from the flat `models` package, which is a
    # different module object from `grocery.generator.models.suppliers` under
    # the repo-root layout — patch the one the caller actually holds.
    monkeypatch.setattr(gen_main.suppliers, 'fetch_suppliers',
                        lambda conn: [_vendor()])
    monkeypatch.setattr(
        gen_main.suppliers, 'advance_credit_memos',
        lambda *a, **k: {'submitted': 0, 'paid': 0, 'rejected': 0,
                         'expired': 0})
    random.seed(20261019)

    deliveries, memos = gen_main._run_vendor_phase(
        db, Config(), SIM_DT, {'stores': [{'location_id': STORE}]},
        [{'product_id': product[0]} for product in CATALOGUE], None)

    assert deliveries['deliveries'] == 1, (
        'the vendor phase reported %r for a day it should have delivered'
        % (deliveries,))
    assert deliveries['lines'] > 0
    assert any('INSERT INTO inv.dsd_deliveries' in sql for sql in
               db.statements_containing('INSERT INTO inv.dsd_deliveries'))
    assert memos['opened'] == 0
    assert memos['submitted'] == 0 and memos['paid'] == 0
    assert memos['rejected'] == 0 and memos['expired'] == 0


def test_the_vendor_phase_still_contains_a_real_failure(monkeypatch):
    """The `except` stays: a tick must not die over the vendor paperwork. It
    must, however, be visible in the log — that is the only reason this defect
    was found at all.
    """
    records = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger('grocery-generator')
    handler = _Capture(level=logging.ERROR)
    logger.addHandler(handler)

    def _boom(*args, **kwargs):
        raise RuntimeError('disk on fire')

    db = _FakeDB()
    monkeypatch.setattr(gen_main.suppliers, 'fetch_suppliers', _boom)
    try:
        deliveries, memos = gen_main._run_vendor_phase(
            db, Config(), SIM_DT, {'stores': []}, [], None)
    finally:
        logger.removeHandler(handler)

    assert deliveries == {'deliveries': 0, 'lines': 0, 'short_lines': 0}
    assert memos == {'opened': 0, 'submitted': 0, 'paid': 0, 'rejected': 0,
                     'expired': 0}
    assert any('Vendor phase failed' in record.getMessage()
               for record in records), (
        'a swallowed vendor-phase failure must leave a log line; got %r'
        % ([r.getMessage() for r in records],))


# ---------------------------------------------------------------------------
# The DSD short-ship path — t_d74d67a3
#
# It was dead code twice over: `short = short_quantity(quantity, quantity)`
# (identically zero on both branches), and if the arithmetic had been right it
# would have died on `fulfillment_item_id` / `fulfillment_id`, which were NOT
# NULL FKs and were being handed fabricated uuids for a drop that has no
# fulfillment behind it. Both halves are pinned here; the DDL half (the columns
# are nullable and the CHECK ties them to detected_source) is pinned in
# test_supplier_schema.py.
# ---------------------------------------------------------------------------
def _short_inserts(db):
    return [(sql, params) for sql, params in db.writes
            if 'INSERT INTO inv.short_ship_events' in sql]


def _cfg_with_dsd_short_rate(rate):
    """A Config whose DSD short probability is exactly `rate`.

    `_vendor()` carries short_ship_rate 0.08 and the DSD rate is that plus
    `dsd_short_ship_bonus`, so the bonus is what moves it — which is also the
    assertion that the config key is read at all.
    """
    cfg = Config()
    cfg.vendors.dsd_short_ship_bonus = max(0.0, rate - 0.08)
    return cfg


def test_a_short_dsd_line_writes_a_short_ship_event(recording_execute_values):
    """The card's arithmetic defect: with the short rate at 1.0 EVERY line is
    short, and the event must say so (`quantity_short = requested - picked > 0`).
    """
    db = _FakeDB()
    counts = _run(db, cfg=_cfg_with_dsd_short_rate(1.0))

    inserts = _short_inserts(db)
    assert inserts, 'the DSD drop shorted every line and wrote no short-ship'
    assert counts['short_lines'] == len(inserts)
    for _sql, params in inserts:
        requested, picked, short = params[4], params[5], params[6]
        assert short == pytest.approx(requested - picked), (requested, picked, short)
        assert short > 0, 'inv.short_ship_events CHECKs quantity_short > 0'
        assert 0 <= picked < requested, (requested, picked)


def test_a_dsd_short_ship_names_no_fulfillment(recording_execute_values):
    """A DSD drop has no pallet on our loads: both parent columns are NULL.

    The bug this replaces wrote `00000000-0000-0000-0000-<random>` uuids into
    both, which the FKs rejected — so the INSERT (and the memo behind it) could
    never land.
    """
    db = _FakeDB()
    _run(db, cfg=_cfg_with_dsd_short_rate(1.0))

    inserts = _short_inserts(db)
    assert inserts
    for sql, params in inserts:
        assert not any(str(p).startswith('00000000-0000-0000-0000')
                       for p in params), (
            'a fabricated uuid is back in the DSD short-ship row: %r' % (params,))
        assert 'VALUES (NULL, NULL,' in ' '.join(sql.split()), (
            'the DSD row must write NULL for both fulfillment parents')
        assert params[3] == suppliers.DETECTED_DSD


def test_a_dsd_short_ship_is_creditable_only_on_the_vendors_terms(
        recording_execute_values):
    """`is_creditable` travels with the event, so a vendor with no returns
    agreement records the shortfall and gets no claim (SEMA-17)."""
    db = _FakeDB()
    _run(db, cfg=_cfg_with_dsd_short_rate(1.0))

    assert all(params[11] is True for _sql, params in _short_inserts(db))


def test_no_short_rate_no_short_ship(recording_execute_values):
    """The mirror: a vendor that always delivers writes items and no events.
    This is what makes the test above about the DRAW rather than about rows."""
    db = _FakeDB()
    counts = _run(db, cfg=_cfg_with_dsd_short_rate(0.0))

    assert counts['short_lines'] == 0
    assert _short_inserts(db) == []
    items = db.batch_for('INSERT INTO inv.dsd_delivery_items')
    assert items is not None, 'a clean drop still delivers its lines'
    assert all(row[2] > 0 for row in items[1])


def test_a_fully_missed_line_writes_no_delivery_item(recording_execute_values):
    """`inv.dsd_delivery_items` is what ARRIVED. A line the vendor never brought
    exists only as a short-ship event, and the delivery's own totals must agree
    with the items it did write (SEMA-24 reconciles the two at the mart)."""
    db = _FakeDB()
    counts = _run(db, cfg=_cfg_with_dsd_short_rate(1.0))

    items = db.batch_for('INSERT INTO inv.dsd_delivery_items')
    item_rows = items[1] if items else []
    assert len(item_rows) == counts['lines']
    assert all(row[2] > 0 for row in item_rows), (
        'a delivery line with nothing delivered belongs in the short-ship '
        'event, not in dsd_delivery_items')

    header = db.statements_containing('INSERT INTO inv.dsd_deliveries')[0]
    params = db.writes[[sql for sql, _ in db.writes].index(header)][1]
    assert params[7] == pytest.approx(sum(row[2] for row in item_rows)), (
        'total_units must be the sum of the units that actually arrived')
    assert params[9] == len(item_rows)


# ---------------------------------------------------------------------------
# The drop is a REPLENISHMENT, not a random slice — t_d59b551f
#
# `ordering.check_and_create_orders` no longer raises a store order for a DSD
# vendor's SKUs, so the vendor's own truck is the only channel that restocks
# them. The pass used to ship `random.sample(catalogue, randint(2, 6))` lines:
# 90 DSD SKUs per store against 4 visits a week of ~4 lines each, i.e. each
# shelf line was visited by its own vendor roughly once every five weeks. That
# was survivable only because the warehouse was covering the same shelves every
# night, and it is a permanent stockout the moment it stops.
# ---------------------------------------------------------------------------
def test_a_drop_ships_the_shelf_lines_that_are_actually_low(
        recording_execute_values):
    """The lines come from `inv.stock_levels` for THIS store and vendor, and
    the trigger is the warehouse's own (`quantity_on_hand < reorder_point`)."""
    below = CATALOGUE[0][0]
    other_store_product = CATALOGUE[1][0]
    other_vendor_product = CATALOGUE[2][0]
    db = _FakeDB(shelf=[
        (STORE, below, SUPPLIER, 120, 2.5),
        (OTHER_STORE, other_store_product, SUPPLIER, 120, 3.5),
        (STORE, other_vendor_product,
         '11111111-0000-0000-0000-000000000009', 120, 4.5),
    ])
    _run(db, vendor=_vendor_without_shorts())

    items = db.batch_for('INSERT INTO inv.dsd_delivery_items')
    assert items is not None
    shipped = {row[1] for row in items[1]}
    assert shipped == {below}, (
        'a drop must ship exactly the low shelf lines it owns; got %r'
        % (shipped,))

    shelf_query = db.statements_containing('FROM inv.stock_levels sl')
    assert shelf_query, 'the drop pass never read the shelf'
    sql = shelf_query[0]
    assert 'quantity_on_hand < ip.reorder_point' in sql, (
        'the drop must restock what is BELOW the reorder point, the same '
        'trigger the warehouse uses:\n%s' % sql)
    assert "fulfillment_model = 'dsd'" in sql, (
        'only a DSD vendor restocks at the shelf:\n%s' % sql)


def test_a_visit_with_nothing_low_writes_no_delivery(
        recording_execute_values):
    """A full shelf means the truck brings nothing — and nothing is recorded.

    An empty `inv.dsd_deliveries` row is what data-lab's
    `assert_dsd_delivery_reconciles_with_items` reports as an 'empty_delivery'
    anomaly, so a visit that carried no goods must not write one. This is the
    first days of a fresh seed: every shelf starts at
    `initial_stock_per_product`, above every reorder point.
    """
    db = _FakeDB(shelf=[])
    counts = _run(db)

    assert counts == {'deliveries': 0, 'lines': 0, 'short_lines': 0}
    assert db.statements_containing('INSERT INTO inv.dsd_deliveries') == []
    assert db.batch_for('INSERT INTO inv.dsd_delivery_items') is None
    assert db.batch_for('UPDATE inv.stock_levels') is None
    assert _short_inserts(db) == []


def test_a_visit_line_covers_the_days_until_the_next_visit(
        recording_execute_values):
    """The line is sized for the GAP between visits, from measured demand.

    A daily-order quantity on a 4-visits-a-week schedule runs the shelf dry the
    day after the drop — the same starvation the warehouse was hiding.
    """
    product = CATALOGUE[0][0]
    demand_per_day = 40.0
    db = _FakeDB(shelf=[(STORE, product, SUPPLIER, 120, 2.5)],
                 demand={(STORE, product): demand_per_day},
                 visits={(SUPPLIER, STORE): 4})
    _run(db, vendor=_vendor_without_shorts())

    items = db.batch_for('INSERT INTO inv.dsd_delivery_items')
    promised = items[1][0][2]
    # 7/4 days of cover at 40/day, plus the default 25% safety fraction
    # (rounded, exactly as `ordering.reorder_quantity` rounds every line).
    expected = round(demand_per_day * (7.0 / 4) * (1 + 0.25))
    assert promised == expected, (
        'a 4-visits-a-week vendor must bring 1.75 days of demand, not one; '
        'brought %r, expected %r' % (promised, expected))


def test_dsd_visit_quantity_is_pure_and_clamped():
    """The sizing rule, without a database — one contract with the warehouse."""
    # No measured demand yet (first day of a backfill): the seeded quantity.
    assert suppliers.dsd_visit_quantity(150, None, 4, 0.25, 4.0) == 150
    # 1.75 days of 100/day, no safety.
    assert suppliers.dsd_visit_quantity(150, 100.0, 4, 0.0, 100.0) == 175
    # A daily visit is the warehouse's own cadence: one day of cover.
    assert suppliers.dsd_visit_quantity(150, 100.0, 7, 0.0, 100.0) == 100
    # Clamped to max_multiple x the seeded quantity, like every other line.
    assert suppliers.dsd_visit_quantity(150, 10_000.0, 4, 0.25, 4.0) == 600
    # A defensive floor on the visit count: 0 must not divide by zero (it is
    # read as one visit a week, the longest gap the sizing rule covers).
    assert suppliers.dsd_visit_quantity(150, 100.0, 0, 0.0, 100.0) == 700
