"""
Regression tests for t_74280120 — a loyalty program that never paid out.

The ledger only ever CREDITED points: 534,250 `purchase` / `tier_upgrade` /
`bonus` rows, zero `redemption` rows, `sum(points_redeemed) = 0`. Every
redemption metric downstream therefore published 0 or NULL
(`int_loyalty_enriched.points_redeemed`,
`mart_loyalty_rfm.lifetime_points_redeemed`, and the two
`redemption_rate_pct` columns), and `pos.coupons.max_uses` — the denominator
of `mart_promotion_redemption.redemption_rate_pct` — was NULL on 100 % of
coupons, so that rate was uncomputable even with a redemption count.

Two halves are pinned here:

  1. `_record_loyalty_points` writes a redemption row for a few percent of
     member tickets, in whole 100-point steps, never more than the balance the
     ledger says the member holds at that instant, and never both directions
     on one row;
  2. every coupon seed (and every pass over an existing catalogue) leaves
     `max_uses` populated.

The connection stub below is deliberately a *ledger*: it answers the two
queries `_record_loyalty_points` runs and raises on any other SQL, so a change
to the production query fails here instead of silently passing.
"""
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import random

import pytest

import grocery.generator.models.pos as pos
from grocery.generator.config import Config


# ---------------------------------------------------------------------------
# A connection stub that actually holds the loyalty ledger
# ---------------------------------------------------------------------------

class _LedgerCursor:
    def __init__(self, conn):
        self.conn = conn
        self.closed = False
        self.rowcount = 0
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        # A real psycopg2 cursor cannot be reused after its block. Closing here
        # is what turns "wrote after the `with` exited" into a red test.
        self.closed = True
        return False

    def execute(self, sql, params=None):
        if self.closed:
            raise RuntimeError("cursor used after its `with` block closed it")
        q = " ".join(sql.split())
        if q.startswith("SELECT points_balance, tier FROM pos.loyalty_members"):
            member = self.conn.members.get(str(params[0]))
            self._row = ((member['points_balance'], member['tier'])
                         if member else None)
        elif "COALESCE(SUM(points_earned - points_redeemed)" in q:
            member_id = str(params[0])
            self._row = (sum(row[2] - row[3] for row in self.conn.rows
                             if row[0] == member_id),)
        elif q.startswith("UPDATE pos.loyalty_members"):
            balance, tier, member_id = params
            member = self.conn.members[str(member_id)]
            member['points_balance'] = balance
            member['tier'] = tier
            self.rowcount = 1
        else:
            raise AssertionError(
                f"_record_loyalty_points ran an unexpected statement: {q}")

    def fetchone(self):
        return self._row

    def fetchall(self):
        return []


class _LedgerConn:
    """Member table + point ledger, in insertion order."""

    def __init__(self, opening_balances):
        self.members = {
            member_id: {'points_balance': balance, 'tier': tier}
            for member_id, (balance, tier) in opening_balances.items()
        }
        self.rows = []
        self.commits = 0
        # seed_loyalty_members writes the opening balance as a `bonus` row, so
        # the ledger's sum equals the member's balance from the start.
        for member_id, (balance, _) in opening_balances.items():
            if balance:
                self.rows.append((member_id, None, balance, 0, 'bonus', balance))

    def cursor(self, *a, **k):
        return _LedgerCursor(self)

    def commit(self):
        self.commits += 1

    # -- helpers -----------------------------------------------------------
    def rows_for(self, member_id):
        return [row for row in self.rows if row[0] == member_id]


def _txn(txn_id, member_id, total):
    """A `txn_records` row — only the fields _record_loyalty_points reads."""
    return (txn_id, 'loc-1', 'emp-1', member_id, None,
            0.0, 0.0, 0.0, 0.0, total, 'normal')


def _run(conn, txns, cfg=None, scenario=None):
    cfg = cfg or Config()
    scenario = scenario or SimpleNamespace(loyalty_redemption_modifier=1.0)

    def _capture(cur, sql, records, template=None):
        assert 'loyalty_point_transactions' in sql
        conn.rows.extend(records)

    with patch.object(pos, 'execute_values', side_effect=_capture):
        pos._record_loyalty_points(conn, txns, cfg, scenario)


def _tickets(member_id, count, total=40.0, start=0):
    return [_txn(f'txn-{start + i}', member_id, total) for i in range(count)]


# ---------------------------------------------------------------------------
# The ledger shape: one direction per row
# ---------------------------------------------------------------------------

def test_each_row_is_exactly_one_of_earn_or_redeem():
    random.seed(11)
    conn = _LedgerConn({'m1': (5000, 'gold')})
    cfg = Config()
    cfg.loyalty.redemption_rate = 0.5          # so the sample is not all earns

    _run(conn, _tickets('m1', 200), cfg)

    redemption_rows = [r for r in conn.rows if r[4] == 'redemption']
    assert redemption_rows, "no redemption row was ever written"
    for row in conn.rows:
        assert (row[2] > 0) != (row[3] > 0), f"row is not earn-XOR-redeem: {row}"
        if row[3] > 0:
            assert row[4] == 'redemption'
        else:
            assert row[4] in ('purchase', 'tier_upgrade', 'bonus')


def test_redemption_never_exceeds_the_balance_at_that_point():
    random.seed(3)
    conn = _LedgerConn({'m1': (700, 'silver')})
    cfg = Config()
    cfg.loyalty.redemption_rate = 0.9

    _run(conn, _tickets('m1', 120, total=25.0), cfg)

    running = 0
    for row in conn.rows:
        _, _, earned, redeemed, _, balance_after = row
        if redeemed:
            assert redeemed <= running, (
                f"redeemed {redeemed} with only {running} on hand")
        running = running + earned - redeemed
        assert balance_after == running, (
            f"balance_after {balance_after} is not the ledger running total {running}")
        assert balance_after >= 0


def test_redemption_is_written_before_the_earn_for_the_same_ticket():
    """The order is load-bearing for data-lab's chain test.

    `assert_loyalty_points_balance` orders same-instant rows by balance_after,
    so the redeem row must be the LOWER one — the shopper spends at the till
    and the sale credits points afterwards.
    """
    random.seed(5)
    conn = _LedgerConn({'m1': (900, 'silver')})
    cfg = Config()
    cfg.loyalty.redemption_rate = 1.0

    _run(conn, [_txn('txn-a', 'm1', 60.0)], cfg)

    rows = conn.rows[1:]  # skip the opening bonus row
    assert [r[4] for r in rows] == ['redemption', 'purchase']
    assert rows[0][5] < rows[1][5], "the redemption must land below the earn"


# ---------------------------------------------------------------------------
# Bounds: the rate, the balance, the ticket's value
# ---------------------------------------------------------------------------

def test_default_rate_keeps_redemptions_rare():
    random.seed(17)
    conn = _LedgerConn({'m1': (20000, 'platinum')})
    tickets = _tickets('m1', 2000, total=40.0)

    _run(conn, tickets)  # Config() defaults: loyalty.redemption_rate = 0.03

    redemptions = sum(1 for r in conn.rows if r[3] > 0)
    assert redemptions > 0, "the default mix still never redeems"
    assert redemptions < len(tickets) * 0.10, (
        f"{redemptions} redemptions in {len(tickets)} tickets is not a few percent")


def test_zero_rate_writes_no_redemption_row():
    random.seed(23)
    conn = _LedgerConn({'m1': (5000, 'gold')})
    cfg = Config()
    cfg.loyalty.redemption_rate = 0.0

    _run(conn, _tickets('m1', 50), cfg)

    assert not [r for r in conn.rows if r[3] > 0]
    assert len(conn.rows) == 51  # opening bonus + one earn per ticket


def test_a_balance_below_the_minimum_never_redeems():
    """A member who cannot afford the smallest redemption pays as usual."""
    random.seed(29)
    conn = _LedgerConn({'m1': (50, 'bronze')})   # < redemption_points_min (100)
    cfg = Config()
    cfg.loyalty.redemption_rate = 1.0

    # $1 tickets earn 1 point each: 20 of them leave the member at 70 — under
    # the 100-point floor the whole way, so no redemption is ever drawn.
    _run(conn, _tickets('m1', 20, total=1.0), cfg)

    assert not [r for r in conn.rows if r[3] > 0]
    assert conn.members['m1']['points_balance'] == 70


def test_redemption_is_capped_by_what_the_ticket_is_worth():
    random.seed(31)
    conn = _LedgerConn({'m1': (100000, 'platinum')})
    cfg = Config()
    cfg.loyalty.redemption_rate = 1.0

    _run(conn, _tickets('m1', 40, total=5.0), cfg)

    redeemed = [r[3] for r in conn.rows if r[3] > 0]
    assert redeemed
    assert max(redeemed) <= int(5.0 * pos.REDEMPTION_POINTS_PER_DOLLAR)
    assert all(amount % pos.REDEMPTION_POINT_STEP == 0 for amount in redeemed)


# ---------------------------------------------------------------------------
# Scenario lever + member-row consistency
# ---------------------------------------------------------------------------

def test_redemption_rush_modifier_raises_the_redemption_count():
    def run_with(modifier):
        random.seed(41)
        conn = _LedgerConn({'m1': (50000, 'platinum')})
        scenario = SimpleNamespace(loyalty_redemption_modifier=modifier)
        _run(conn, _tickets('m1', 800, total=40.0), None, scenario)
        return sum(1 for r in conn.rows if r[3] > 0)

    baseline = run_with(1.0)
    rushed = run_with(6.0)      # Config().scenarios.redemption_rush_modifier

    assert 0 < baseline < rushed


def test_member_balance_tracks_the_ledger_and_never_goes_negative():
    random.seed(47)
    conn = _LedgerConn({'m1': (800, 'silver'), 'm2': (60, 'bronze')})
    cfg = Config()
    cfg.loyalty.redemption_rate = 0.6

    txns = _tickets('m1', 100, total=30.0) + _tickets('m2', 100, total=12.0, start=1000)
    _run(conn, txns, cfg)

    for member_id in ('m1', 'm2'):
        ledger_total = sum(r[2] - r[3] for r in conn.rows_for(member_id))
        assert conn.members[member_id]['points_balance'] == ledger_total
        assert conn.members[member_id]['points_balance'] >= 0


def test_a_redemption_never_demotes_a_tier():
    """Taking points back does not take the tier back."""
    random.seed(53)
    # Platinum by tier, but only 200 points on hand.
    conn = _LedgerConn({'m1': (200, 'platinum')})
    cfg = Config()
    cfg.loyalty.redemption_rate = 1.0

    _run(conn, _tickets('m1', 30, total=20.0), cfg)

    assert [r for r in conn.rows if r[3] > 0], "no redemption was drawn"
    assert conn.members['m1']['tier'] == 'platinum'


# ---------------------------------------------------------------------------
# Coupons: the redemption-rate denominator must exist
# ---------------------------------------------------------------------------

class _ScriptedCursor:
    def __init__(self, fetchones, log):
        self._fetchones = list(fetchones)
        self._log = log
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._log.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self._fetchones.pop(0) if self._fetchones else None

    def fetchall(self):
        return []


class _ScriptedConn:
    def __init__(self, fetchones=None):
        self.executed = []
        self._fetchones = list(fetchones or [])

    def cursor(self, *a, **k):
        return _ScriptedCursor(self._fetchones, self.executed)

    def commit(self):
        pass


def _seeded_coupons(func, *args, fetchones=None):
    conn = _ScriptedConn(fetchones)
    records = []

    def _capture(cur, sql, recs, template=None):
        records.extend(recs)

    with patch.object(pos, 'execute_values', side_effect=_capture):
        func(conn, *args)
    return records, conn


def _coupon_args():
    cfg = Config()
    departments = [{'department_id': f'dept-{i}', 'name': name}
                   for i, name in enumerate(['Produce', 'Dairy', 'Bakery', 'Meat'])]
    products = [{'product_id': f'prod-{i}'} for i in range(5)]
    return cfg, departments, products


def test_named_coupons_carry_a_usage_cap():
    _, departments, _ = _coupon_args()
    records, _ = _seeded_coupons(pos.seed_named_coupons, departments, 30,
                                 fetchones=[(None,)])
    assert records
    for record in records:
        max_uses = record[7]
        assert isinstance(max_uses, int) and max_uses > 0, (
            f"coupon {record[0]} has no max_uses: {max_uses!r}")


def test_generated_coupons_draw_a_cap_from_the_configured_range():
    cfg, departments, products = _coupon_args()
    cfg.coupons.max_uses_min = 1200
    cfg.coupons.max_uses_max = 1300
    # fetchone #1 = active-count guard, #2 = promo history horizon
    records, _ = _seeded_coupons(pos.seed_coupons, cfg, departments, products, 30,
                                 fetchones=[(0,), (None,)])
    assert records
    for record in records:
        assert 1200 <= record[7] <= 1300


def test_seed_coupons_repairs_a_catalogue_seeded_without_caps():
    """An existing data dir is healed on the next pass, not only on a reseed.

    The guard below returns early (the active set is already full), which is
    the path a live install takes — the repair must still run.
    """
    cfg, departments, products = _coupon_args()
    records, conn = _seeded_coupons(
        pos.seed_coupons, cfg, departments, products, 30,
        fetchones=[(cfg.coupons.active_at_any_time,)])

    repairs = [(sql, params) for sql, params in conn.executed
               if 'UPDATE pos.coupons' in sql and 'max_uses IS NULL' in sql]
    assert repairs, f"legacy coupons were never repaired: {conn.executed}"
    _, params = repairs[0]
    assert params == (cfg.coupons.max_uses_min, cfg.coupons.max_uses_max,
                      cfg.coupons.max_uses_min)
    assert records == [] or all(r[7] for r in records)


def test_coupon_cap_repair_is_idempotent_by_predicate():
    """The repair only ever touches NULL rows, so a second pass is a no-op."""
    cfg, _, _ = _coupon_args()
    conn = _ScriptedConn()
    pos.backfill_coupon_max_uses(conn, cfg)
    sql = " ".join(conn.executed[0][0].split())
    assert "WHERE max_uses IS NULL" in sql
