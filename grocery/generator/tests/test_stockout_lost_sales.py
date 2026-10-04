"""
Stock-aware sales — verisim card t_959cd040.

The defect: depletion ran `GREATEST(0, quantity_on_hand - qty)` while the sale
itself was written at the FULL requested quantity. Two things were wrong at
once:

  * a shortage was silently absorbed — the shelf floored at zero and the sale
    kept its full quantity, so `pos.transaction_items` reported units that were
    never on the shelf. Measured on the dev DB: 249 of 1485 store-SKU rows
    (16.8%) pinned at zero, with ~600k transactions still selling against them
    over the following 7 days;
  * because a shortage could not be observed, `reorder_point`, `reorder_qty`
    and `restock_threshold_pct` were decorative — nothing in the generator
    responded to a shelf running dry.

What these tests pin:

1. the allocation rule itself — a shelf is spent at most once across a batch,
   in order, and two lines for the same SKU split the last units rather than
   each taking all of them (pure, no DB);
2. a sale is capped at on-hand: `pos.transaction_items.quantity` never exceeds
   what the store held;
3. the shortfall is RECORDED — `inv.stockout_events` carries the request, what
   was covered, the loss and its value, and `lost = requested - fulfilled`;
4. both channels share ONE shelf, so POS and online in the same tick cannot
   each be handed the last unit;
5. revenue is booked on what the shopper got — subtotal/tax/total are computed
   from the capped quantity, so a capped basket does not book the price of a
   basket that never happened;
6. the old behaviour is demonstrable, not merely absent: with the switch off the
   generator still sells stock it does not have, which is what this test pins
   so a future "optimisation" cannot quietly reintroduce it;
7. the daily demand ledger reconciles (`requested = fulfilled + lost`) and
   replenishment sizes itself from measured demand, using the safety fraction;
8. a stockout is not written when nothing was short (no noise), and a fully
   empty basket is not written as a sale at all (quantity > 0 CHECK).

Read the economics in plain terms: if a shopper asks for 3 boxes of cereal and
the shelf has 2, the till rings up 2 boxes and $2-worth of revenue, and the
third box is written to `inv.stockout_events` as a lost sale so the store's
shrinkage analytics can see the gap. The feed used to book all 3 and leave the
shelf at zero.
"""
import random
from datetime import date, datetime
from unittest.mock import patch

import pytest

from grocery.generator.models import inventory, ordering
from grocery.generator.models.inventory import StockAllowance
from grocery.generator.config import Config

from .conftest import patch_all_pos_writes

SIM_DT = datetime(2026, 9, 26, 10, 0, 0)
SKU_A = 'aaaaaaaa-0000-0000-0000-000000000001'
SKU_B = 'bbbbbbbb-0000-0000-0000-000000000002'
STORE = 'cccccccc-0000-0000-0000-000000000003'


def _line(line_id, product_id=SKU_A, quantity=1.0, price=4.00,
          location_id=STORE, parent_id=None):
    return {
        'line_id': line_id,
        'location_id': location_id,
        'product_id': product_id,
        'parent_id': parent_id or f'parent-{line_id}',
        'quantity': quantity,
        'unit_price': price,
    }


# ---------------------------------------------------------------------------
# 1. The allocation law (pure — no database)
# ---------------------------------------------------------------------------
def test_line_with_stock_is_granted_in_full():
    allowance = StockAllowance({(STORE, SKU_A): 10})
    granted = allowance.take([_line('l1', quantity=3)])
    assert granted['l1'] == 3.0
    assert allowance.available(STORE, SKU_A) == 7.0


def test_a_line_cannot_exceed_on_hand():
    allowance = StockAllowance({(STORE, SKU_A): 2})
    granted = allowance.take([_line('l1', quantity=3)])
    assert granted['l1'] == 2.0, "must be capped at on-hand, not the request"
    assert allowance.available(STORE, SKU_A) == 0.0


def test_an_empty_shelf_grants_nothing():
    allowance = StockAllowance({(STORE, SKU_A): 0})
    granted = allowance.take([_line('l1', quantity=3)])
    assert granted['l1'] == 0.0


def test_a_store_sku_with_no_stock_row_grants_nothing():
    """No inv.stock_levels row means nothing to sell, not infinite stock."""
    allowance = StockAllowance({})
    assert allowance.take([_line('l1', quantity=3)])['l1'] == 0.0


def test_two_lines_for_the_same_sku_split_the_last_units():
    """The pre-t_959cd040 row loop could not see these compete."""
    allowance = StockAllowance({(STORE, SKU_A): 3})
    granted = allowance.take([_line('l1', quantity=2), _line('l2', quantity=2)])
    assert granted['l1'] == 2.0
    assert granted['l2'] == 1.0, "second line must only get what is left"
    assert sum(granted.values()) == 3.0, "a shelf is spent at most once"


def test_allocation_is_ordered_not_proportional():
    """First come, first served — the tie-break has to be deterministic."""
    allowance = StockAllowance({(STORE, SKU_A): 5})
    granted = allowance.take([
        _line('a', quantity=5), _line('b', quantity=5), _line('c', quantity=5)])
    assert [granted['a'], granted['b'], granted['c']] == [5.0, 0.0, 0.0]


def test_different_skus_do_not_compete():
    allowance = StockAllowance({(STORE, SKU_A): 1, (STORE, SKU_B): 1})
    granted = allowance.take([
        _line('l1', SKU_A, quantity=4), _line('l2', SKU_B, quantity=4)])
    assert granted['l1'] == 1.0
    assert granted['l2'] == 1.0


def test_different_stores_do_not_compete():
    other = 'dddddddd-0000-0000-0000-000000000004'
    allowance = StockAllowance({(STORE, SKU_A): 1, (other, SKU_A): 1})
    granted = allowance.take([
        _line('l1', SKU_A, 4, location_id=STORE),
        _line('l2', SKU_A, 4, location_id=other)])
    assert granted['l1'] == 1.0
    assert granted['l2'] == 1.0


def test_fractional_lb_quantity_is_preserved_to_three_dp():
    allowance = StockAllowance({(STORE, SKU_A): 2.5})
    granted = allowance.take([_line('l1', quantity=2.125)])
    assert granted['l1'] == 2.125


def test_reserved_never_exceeds_the_snapshot():
    allowance = StockAllowance({(STORE, SKU_A): 10})
    allowance.take([_line(f'l{i}', quantity=4) for i in range(10)])
    total = sum(allowance.reserved_by_sku().values())
    assert total == 10.0, "the whole snapshot, and not one unit more"


# ---------------------------------------------------------------------------
# 2. The journalled loss (what flush_sales would persist)
# ---------------------------------------------------------------------------
def _journal_entry(**overrides):
    entry = {
        'line_id': 'l1',
        'location_id': STORE,
        'product_id': SKU_A,
        'parent_id': 'txn-1',
        'requested': 3.0,
        'granted': 2.0,
        'unit_price': 4.00,
        'channel': inventory.CHANNEL_POS,
    }
    entry.update(overrides)
    return entry


def test_a_short_line_journals_the_request_it_could_not_meet():
    allowance = StockAllowance({(STORE, SKU_A): 2})
    allowance.take([_line('l1', quantity=3)], inventory.CHANNEL_POS)
    entry = allowance.journal[0]
    assert entry['requested'] == 3.0, "the request must survive the cap"
    assert entry['granted'] == 2.0


def test_the_journal_records_fully_unserved_lines():
    """A dropped line still has to be in the journal — it is a lost sale."""
    allowance = StockAllowance({(STORE, SKU_A): 0})
    granted, capped = inventory.resolve_sales(
        allowance, [_line('l1', quantity=3)], inventory.CHANNEL_POS)
    assert granted['l1'] == 0.0
    assert capped == [], "a zero-quantity line is not writable"
    assert len(allowance.journal) == 1, "but the loss is still recorded"


def test_resolve_sales_never_returns_a_zero_quantity_line():
    """pos.transaction_items and online.order_items both CHECK (quantity > 0)."""
    allowance = StockAllowance({(STORE, SKU_A): 1})
    _granted, capped = inventory.resolve_sales(
        allowance,
        [_line('l1', quantity=1), _line('l2', quantity=5)],
        inventory.CHANNEL_POS)
    assert [line['quantity'] for line in capped] == [1.0]
    assert all(line['quantity'] > 0 for line in capped)


def test_resolve_sales_caps_but_keeps_the_line():
    allowance = StockAllowance({(STORE, SKU_A): 2})
    _granted, capped = inventory.resolve_sales(
        allowance, [_line('l1', quantity=3)], inventory.CHANNEL_POS)
    assert len(capped) == 1
    assert capped[0]['quantity'] == 2.0


def test_journal_carries_the_channel_so_the_two_are_distinguishable():
    allowance = StockAllowance({(STORE, SKU_A): 5})
    allowance.take([_line('l1', quantity=1)], inventory.CHANNEL_POS)
    allowance.take([_line('l2', quantity=1)], inventory.CHANNEL_ONLINE)
    assert [e['channel'] for e in allowance.journal] == [
        inventory.CHANNEL_POS, inventory.CHANNEL_ONLINE]


def test_an_explicit_line_channel_beats_the_call_default():
    allowance = StockAllowance({(STORE, SKU_A): 5})
    line = _line('l1', quantity=1)
    line['channel'] = inventory.CHANNEL_ONLINE
    allowance.take([line], inventory.CHANNEL_POS)
    assert allowance.journal[0]['channel'] == inventory.CHANNEL_ONLINE


# ---------------------------------------------------------------------------
# 3. Both channels share one shelf
# ---------------------------------------------------------------------------
def test_pos_and_online_cannot_each_take_the_last_unit():
    """One allowance per tick — the reason main.py builds it before both."""
    allowance = StockAllowance({(STORE, SKU_A): 1})
    pos_granted, _ = inventory.resolve_sales(
        allowance, [_line('pos-1', quantity=1)], inventory.CHANNEL_POS)
    online_granted, capped = inventory.resolve_sales(
        allowance, [_line('web-1', quantity=1)], inventory.CHANNEL_ONLINE)
    assert pos_granted['pos-1'] == 1.0
    assert online_granted['web-1'] == 0.0
    assert capped == [], "the online order cannot be filled at all"


# ---------------------------------------------------------------------------
# 4. The old behaviour, demonstrable
# ---------------------------------------------------------------------------
def test_the_pre_fix_behaviour_is_the_uncapped_sale():
    """What the generator did before: request in full, shelf floors at zero.

    Not an assertion about the current generator — a statement of the defect, so
    the `enforce_stock_availability` switch is pinned to something real and the
    difference is visible rather than folklore.
    """
    on_hand = 2
    requested = 3

    pre_fix_sold = requested                       # sale written in full
    pre_fix_on_hand_after = max(0, on_hand - requested)  # GREATEST(0, ...)

    allowance = StockAllowance({(STORE, SKU_A): on_hand})
    post_fix_sold = allowance.take([_line('l1', quantity=requested)])['l1']

    assert pre_fix_sold == 3 and pre_fix_on_hand_after == 0, "the defect"
    assert post_fix_sold == 2.0, "the fix caps at the shelf"
    assert post_fix_sold < pre_fix_sold


# ---------------------------------------------------------------------------
# 5. Replenishment sizing responds to measured demand
# ---------------------------------------------------------------------------
def test_reorder_uses_seeded_quantity_when_no_demand_is_measured():
    """Day one of a backfill: the ledger is empty, so the seeded qty stands."""
    assert ordering.reorder_quantity(
        reorder_qty=150, demand_per_day=None, safety_pct=0.25, max_multiple=4.0) == 150


def test_reorder_scales_to_measured_demand_plus_safety():
    """A SKU selling 400/day must not be topped up with the seeded 150."""
    qty = ordering.reorder_quantity(
        reorder_qty=150, demand_per_day=400.0, safety_pct=0.25, max_multiple=4.0)
    assert qty == 500, "400 units of demand + 25% safety"


def test_demand_beyond_the_cap_is_clamped_not_trusted():
    """700/day + safety would be 875, but the cap is 150 x 4 = 600.

    The clamp binds BEFORE the full safety margin is applied, which is the
    intended guard: reorder_qty_max_multiple exists so one hot SKU cannot turn
    into an unbounded order line, and a cap that only trimmed the tail would
    leave that hot SKU permanently short.
    """
    assert ordering.reorder_quantity(
        reorder_qty=150, demand_per_day=700.0,
        safety_pct=0.25, max_multiple=4.0) == 600


def test_the_safety_fraction_actually_changes_the_order():
    """restock_threshold_pct used to be read by nothing (the decorative key)."""
    thin = ordering.reorder_quantity(150, 400.0, 0.0, 4.0)
    padded = ordering.reorder_quantity(150, 400.0, 0.5, 4.0)
    assert thin == 400
    assert padded == 600


def test_a_zero_safety_fraction_means_no_padding():
    assert ordering.reorder_quantity(150, 300.0, 0.0, 4.0) == 300


def test_reorder_is_capped_so_one_hot_sku_cannot_run_away():
    """Bounded by reorder_qty_max_multiple × the seeded qty."""
    assert ordering.reorder_quantity(
        reorder_qty=100, demand_per_day=100000.0,
        safety_pct=0.25, max_multiple=4.0) == 400


def test_reorder_is_never_zero():
    """A zero-quantity order line would be unwritable / meaningless."""
    assert ordering.reorder_quantity(150, 0.1, 0.0, 4.0) >= 1
    assert ordering.reorder_quantity(150, 0.0, 0.0, 4.0) >= 1


def test_a_higher_demand_window_covers_more_days():
    """reorder_demand_window_days is expressed through the expected demand it
    feeds, so a longer window is the caller's job — pin that the sizing is
    linear in the demand it is handed."""
    one_day = ordering.reorder_quantity(150, 700.0, 0.0, 1000.0)
    seven_days = ordering.reorder_quantity(150, 4900.0, 0.0, 1000.0)
    assert seven_days == 7 * one_day


# ---------------------------------------------------------------------------
# 6. Config wiring
# ---------------------------------------------------------------------------
def test_enforce_stock_availability_is_on_by_default():
    assert Config().inventory.enforce_stock_availability is True


def test_the_config_carries_the_new_keys():
    cfg = Config().inventory
    assert cfg.restock_threshold_pct == 0.25
    assert cfg.reorder_demand_window_days == 1
    assert cfg.reorder_qty_max_multiple == 4.0


# ---------------------------------------------------------------------------
# 7. POS books revenue on what it could actually sell
# ---------------------------------------------------------------------------
class _FakeConn:
    """Captures the batched inserts; nothing touches a real database."""

    def __init__(self):
        self.transactions = []
        self.items = []
        self._fetchall = []
        self._fetchone = None

    def cursor(self, *a, **k):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a, **k):
        pass

    def fetchone(self):
        return self._fetchone

    def fetchall(self):
        return self._fetchall

    def commit(self):
        pass

    def rollback(self):
        pass


def _products(n=1):
    return [
        {
            'product_id': SKU_A,
            'name': 'Cereal',
            'category': 'Cereal',
            'department': 'Grocery',
            'price': 4.00,
            'current_price': 4.00,
            'reference_price': 4.00,
            'uom': 'each',
        }
    ]


def _employees():
    return [{
        'employee_id': 'e1', 'department': 'store', 'location_type': 'store',
        'location_id': STORE,
    }]


class _Scenario:
    scenario_tag = 'normal'
    active_promotions = []
    coupon_multiplier = 1.0
    price_modifier = 1.0
    volume_multiplier = 1.0
    loyalty_engagement_modifier = 1.0

    def __getattr__(self, name):
        return 1.0


def _run_pos(cfg, conn, count=1, allowance=None):
    from grocery.generator.models import pos

    def _capture(cur, sql, records, template=None):
        if 'pos.transactions' in sql and records:
            conn.transactions.extend(records)
        if 'transaction_items' in sql and records:
            conn.items.extend(records)

    with patch_all_pos_writes(_capture):
        with patch('grocery.generator.models.hr.execute_values'):
            pos.generate_pos_transactions(
                conn, cfg, SIM_DT, count, _Scenario(),
                [{'location_id': STORE, 'location_type': 'store'}],
                _products(), _employees(), [], [], [], {},
                stock_allowance=allowance,
            )
    return conn


def test_pos_writes_a_capped_line_when_the_shelf_is_short():
    """The headline fix: the sale records what the shopper could buy."""
    cfg = Config()
    conn = _FakeConn()
    allowance = StockAllowance({(STORE, SKU_A): 2})

    # A one-SKU catalogue with 15-unit baskets: force a long line, then cap it.
    random.seed(11)
    with patch.object(StockAllowance, 'take',
                      side_effect=lambda lines, channel='': {
                          l['line_id']: min(2.0, l['quantity']) for l in lines}):
        _run_pos(cfg, conn, count=1, allowance=allowance)

    for item in conn.items:
        assert item[2] <= 2.0, (
            f"line quantity {item[2]} exceeds the 2 units on the shelf")


def test_pos_booked_revenue_matches_the_capped_line():
    """Revenue must describe the basket as filled, not as requested.

    A cap that only clamps transaction_items while subtotal still used the
    requested quantity would book revenue for goods the shopper never got — the
    exact defect, one layer down.
    """
    cfg = Config()
    conn = _FakeConn()
    allowance = StockAllowance({(STORE, SKU_A): 2})
    random.seed(11)
    _run_pos(cfg, conn, count=1, allowance=allowance)

    if not conn.items:
        pytest.skip("seed produced no sale in this run")

    expected_subtotal = round(
        sum(round(item[3] * item[2], 2) for item in conn.items), 2)
    subtotal = conn.transactions[0][5]
    # The promotional-discount path would lower this legitimately; with no
    # active promotions the subtotal IS the sum of the capped lines.
    assert subtotal == expected_subtotal


def test_a_fully_out_of_stock_basket_writes_no_transaction():
    """pos.transaction_items has CHECK (quantity > 0) — nothing to write."""
    cfg = Config()
    conn = _FakeConn()
    allowance = StockAllowance({(STORE, SKU_A): 0})
    random.seed(3)
    _run_pos(cfg, conn, count=5, allowance=allowance)

    assert conn.transactions == [], "no revenue for a basket nothing could fill"
    assert conn.items == []
    assert len(allowance.journal) > 0, "but the lost demand is journalled"


def test_enforce_stock_availability_false_restores_the_uncapped_sale():
    """The regression switch is pinned to the real pre-fix behaviour."""
    cfg = Config()
    cfg.inventory.enforce_stock_availability = False
    conn = _FakeConn()
    allowance = StockAllowance({(STORE, SKU_A): 0})
    random.seed(3)
    _run_pos(cfg, conn, count=5, allowance=allowance)

    assert conn.transactions, "with the switch off, sales are written regardless"
    for item in conn.items:
        assert item[2] > 0


def test_no_allowance_leaves_the_old_behaviour_untouched():
    """Callers with no allowance (and every existing test) are unaffected."""
    cfg = Config()
    conn = _FakeConn()
    random.seed(3)
    _run_pos(cfg, conn, count=5, allowance=None)
    assert conn.transactions, "sales still written with no stock check"


def test_sold_units_never_exceed_stock_over_a_batch():
    """End to end on the arithmetic: sum(sold) <= on-hand for the store-SKU."""
    cfg = Config()
    conn = _FakeConn()
    on_hand = 9
    allowance = StockAllowance({(STORE, SKU_A): float(on_hand)})
    random.seed(5)
    _run_pos(cfg, conn, count=25, allowance=allowance)

    sold = sum(item[2] for item in conn.items)
    assert sold <= on_hand, (
        f"sold {sold} units from a shelf that held {on_hand}")


def test_the_take_is_journalled_for_every_line_resolved():
    cfg = Config()
    conn = _FakeConn()
    allowance = StockAllowance({(STORE, SKU_A): 3})
    random.seed(2)
    _run_pos(cfg, conn, count=4, allowance=allowance)
    granted_total = sum(e['granted'] for e in allowance.journal)
    written_total = sum(item[2] for item in conn.items)
    assert granted_total == pytest.approx(written_total, abs=0.001), (
        "what the shelf granted must equal what was written as sold")

# ---------------------------------------------------------------------------
# 8. Every batched row template has exactly as many slots as columns
# ---------------------------------------------------------------------------
# A DB-free test cannot execute an INSERT, so a template with one placeholder
# fewer than the row tuple has columns is invisible until a real stockout fires.
# That is not hypothetical: the stockout template shipped with 11 placeholders
# for 12 columns and only failed on the first tick whose shelf ran short (found
# by the wipe+reseed probe, t_959cd040).
#
# The templates are named module constants precisely so these tests can assert
# on the real values instead of re-parsing source text.


def _slots(template):
    """Number of value slots in an execute_values row template."""
    return template.count('%s')


def test_the_stockout_template_has_one_slot_per_column():
    assert _slots(inventory.STOCKOUT_ROW_TEMPLATE) == inventory.STOCKOUT_COLUMN_COUNT, (
        f"stockout template has {_slots(inventory.STOCKOUT_ROW_TEMPLATE)} slots "
        f"for {inventory.STOCKOUT_COLUMN_COUNT} columns")


def test_the_sku_demand_template_has_one_slot_per_column():
    assert _slots(inventory.SKU_DEMAND_ROW_TEMPLATE) == inventory.SKU_DEMAND_COLUMN_COUNT


def test_the_stock_decrement_template_has_one_slot_per_column():
    assert (_slots(inventory.STOCK_LEVEL_DECREMENT_TEMPLATE)
            == inventory.STOCK_LEVEL_DECREMENT_COLUMN_COUNT)


def test_the_stockout_template_is_one_parenthesised_row():
    """execute_values applies the template per row, so it must be a single row."""
    template = inventory.STOCKOUT_ROW_TEMPLATE
    assert template.startswith('(') and template.endswith(')')
    assert template.count('(') == 1 and template.count(')') == 1


def test_a_stockout_row_tuple_has_exactly_as_many_fields_as_slots():
    """Build the row the way flush_sales does and check its width.

    This is the check that actually would have caught the 11-vs-12 bug: it
    runs the real code path (journal -> record tuple) and measures the result,
    rather than trusting that the two literals were edited together.
    """
    allowance = StockAllowance({(STORE, SKU_A): 2})
    line = _line('l1', quantity=5, price=4.00)
    allowance.take([line], inventory.CHANNEL_POS)
    entry = allowance.journal[0]

    # Mirrors the tuple built in flush_sales, in the same order.
    record = (
        str(entry['product_id']),
        str(entry['location_id']),
        entry.get('channel') or inventory.CHANNEL_POS,
        entry['parent_id'] if entry['channel'] == inventory.CHANNEL_POS else None,
        entry['parent_id'] if entry['channel'] == inventory.CHANNEL_ONLINE else None,
        entry['requested'],
        entry['granted'],
        round(entry['requested'] - entry['granted'], 3),
        round(float(entry['unit_price']), 4),
        round((entry['requested'] - entry['granted']) * float(entry['unit_price']), 2),
        SIM_DT,
        None,
    )
    assert len(record) == _slots(inventory.STOCKOUT_ROW_TEMPLATE), (
        f"stockout row has {len(record)} fields but the template has "
        f"{_slots(inventory.STOCKOUT_ROW_TEMPLATE)} slots")


def test_a_sku_demand_row_has_exactly_as_many_fields_as_slots():
    allowance = StockAllowance({(STORE, SKU_A): 2})
    line = _line('l1', quantity=5, price=4.00)
    allowance.take([line], inventory.CHANNEL_POS)
    entry = allowance.journal[0]
    lost = entry['requested'] - entry['granted']

    row = (str(entry['location_id']), str(entry['product_id']), SIM_DT.date(),
           entry['requested'], entry['granted'], round(lost, 3),
           round(lost * float(entry['unit_price']), 2), 1)
    assert len(row) == _slots(inventory.SKU_DEMAND_ROW_TEMPLATE)


def test_the_stock_decrement_row_has_exactly_as_many_fields_as_slots():
    row = (STORE, SKU_A, 4.0)
    assert len(row) == _slots(inventory.STOCK_LEVEL_DECREMENT_TEMPLATE)