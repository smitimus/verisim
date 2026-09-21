"""
Regression tests for verisim card t_01b4fe4f — promotion validity windows.

data-lab asserts `transaction_dt between valid_from and valid_until` for every
line item that carries a `coupon_id` / `deal_id`
(`assert_coupon_dates_valid` / `assert_deal_dates_valid`). Two defects in the
generator broke that on the dev EDW (24,015 of 34,997 coupon items and 7,079
of 10,870 deal items out of window):

  1. seeding stamped `valid_from = today` while the generator back-dated the
     transactions that reference those promos across a 30-day horizon;
  2. nothing ever maintained `uses_count` — every coupon read 0 uses.

These tests pin the three halves of the fix: the seeded window reaches back
over the backfill horizon, a promo is only applied inside its own window, and
`reconcile_promotions()` derives the window + counter from the redemptions
actually on disk.
"""
from datetime import datetime, date, timedelta
from unittest.mock import patch

import random

import grocery.generator.models.pos as pos
from grocery.generator.config import Config

SIM_DT = datetime(2026, 6, 1, 12, 0, 0)  # inside the windows used below


class _ScriptedCursor:
    """Cursor stub: scripted fetchone results, records executed SQL."""

    def __init__(self, fetchones=None, rowcounts=None, log=None, label=""):
        self._fetchones = list(fetchones or [])
        self._rowcounts = list(rowcounts or [])
        self._log = log if log is not None else []
        self._label = label
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._log.append((self._label, " ".join(sql.split()), params))
        if self._rowcounts:
            self.rowcount = self._rowcounts.pop(0)

    def fetchone(self):
        return self._fetchones.pop(0) if self._fetchones else None

    def fetchall(self):
        return []


class _ScriptedConn:
    def __init__(self, fetchones=None, rowcounts=None):
        self.executed = []
        self.commits = 0
        self._fetchones = list(fetchones or [])
        self._rowcounts = list(rowcounts or [])

    def cursor(self, *a, **k):
        return _ScriptedCursor(self._fetchones, self._rowcounts, self.executed)

    def commit(self):
        self.commits += 1


_captured_items = []


def _capture_items(cur, sql, records, template=None):
    if 'transaction_items' in sql and records and len(records[0]) == 8:
        _captured_items.extend(records)


def _generate(sim_dt, coupons, deals):
    """Run one transaction batch and return the captured transaction_items."""
    random.seed(7)
    cfg = Config()
    cfg.coupons.coupon_use_rate = 1.0
    cfg.combo_deals.combo_use_rate = 1.0

    class _Scenario:
        active_promotions = []
        coupon_multiplier = 1.0
        loyalty_engagement_modifier = 1.0
        scenario_tag = "normal"

    products = [
        {"product_id": f"p{i}", "department": "Produce", "uom": "each",
         "price": 4.0, "department_id": "dept-1"}
        for i in range(4)
    ]
    stores = [{"location_id": "loc1", "location_type": "store"}]
    employees = [{"department": "store", "location_type": "store",
                  "employee_id": "e1", "location_id": "loc1"}]

    _captured_items.clear()
    with patch.object(random, "random", lambda: 0.0), \
         patch("grocery.generator.models.pos.execute_values", side_effect=_capture_items):
        pos.generate_pos_transactions(
            _ScriptedConn(), cfg, sim_dt, 4, _Scenario(), stores, products,
            employees, [{"member_id": "m1"}], coupons, deals,
        )
    return list(_captured_items)


def _coupon(**over):
    c = {"coupon_id": "c1", "coupon_type": "percent_off", "discount_value": 0.1,
         "department_id": None, "product_id": None,
         "valid_from": date(2026, 5, 1), "valid_until": date(2026, 7, 1)}
    c.update(over)
    return c


def _deal(**over):
    d = {"deal_id": "d1", "deal_type": "x_for_price", "trigger_qty": 2,
         "trigger_department_id": None, "trigger_product_id": None,
         "deal_price": 1.0,
         "valid_from": date(2026, 5, 1), "valid_until": date(2026, 7, 1)}
    d.update(over)
    return d


# ---------------------------------------------------------------------------
# Application-time window enforcement
# ---------------------------------------------------------------------------

def test_promo_is_applied_inside_its_window():
    """Control: a promo valid on the simulated date still gets tagged."""
    items = _generate(SIM_DT, [_coupon()], [_deal()])
    assert items, "no transaction_items captured"
    assert any(r[5] == "c1" for r in items), "coupon not tagged inside its window"
    assert any(r[6] == "d1" for r in items), "deal not tagged inside its window"


def test_coupon_not_applied_before_its_window():
    """The reported bug: a back-dated transaction must not carry a coupon id
    whose window opens later."""
    items = _generate(SIM_DT, [_coupon(valid_from=date(2026, 6, 2),
                                      valid_until=date(2026, 7, 1))], [_deal()])
    assert items, "no transaction_items captured"
    assert all(r[5] is None for r in items), "coupon tagged before valid_from"


def test_deal_not_applied_before_its_window():
    items = _generate(SIM_DT, [_coupon()], [_deal(valid_from=date(2026, 6, 2),
                                                  valid_until=date(2026, 7, 1))])
    assert items, "no transaction_items captured"
    assert all(r[6] is None for r in items), "deal tagged before valid_from"


def test_promo_not_applied_after_its_window():
    items = _generate(SIM_DT, [_coupon(valid_from=date(2026, 1, 1),
                                       valid_until=date(2026, 5, 31))],
                      [_deal(valid_from=date(2026, 1, 1),
                             valid_until=date(2026, 5, 31))])
    assert items, "no transaction_items captured"
    assert all(r[5] is None for r in items), "coupon tagged after valid_until"
    assert all(r[6] is None for r in items), "deal tagged after valid_until"


def test_promo_without_a_window_is_not_applied():
    """Fail closed: an unknown window must not produce a violating row."""
    items = _generate(SIM_DT, [{"coupon_id": "c1", "coupon_type": "percent_off",
                                "discount_value": 0.1, "department_id": None,
                                "product_id": None}],
                      [{"deal_id": "d1", "deal_type": "x_for_price",
                        "trigger_qty": 2, "trigger_department_id": None,
                        "trigger_product_id": None, "deal_price": 1.0}])
    assert items, "no transaction_items captured"
    assert all(r[5] is None for r in items)
    assert all(r[6] is None for r in items)


def test_promo_applies_on_boundaries():
    assert pos._promo_applies_on(_coupon(), date(2026, 5, 1))
    assert pos._promo_applies_on(_coupon(), date(2026, 7, 1))
    assert not pos._promo_applies_on(_coupon(), date(2026, 4, 30))
    assert not pos._promo_applies_on(_coupon(), date(2026, 7, 2))


def test_window_accepts_iso_strings_from_json():
    promo = {"valid_from": "2026-05-01T00:00:00", "valid_until": "2026-07-01"}
    assert pos._promo_applies_on(promo, date(2026, 6, 1))
    assert not pos._promo_applies_on(promo, date(2026, 8, 1))


# ---------------------------------------------------------------------------
# Seeding: the window must reach back over the backfill horizon
# ---------------------------------------------------------------------------

HISTORY_DAYS = 30


def _seeded_records(func, fetchones, *args):
    conn = _ScriptedConn(fetchones=fetchones)
    records = []

    def _capture(cur, sql, recs, template=None):
        records.extend(recs)

    with patch("grocery.generator.models.pos.execute_values", side_effect=_capture):
        func(conn, *args)
    return records


def test_named_coupons_are_back_dated_to_the_horizon():
    """A fresh DB (no transactions yet) still gets a window covering the whole
    backfill the generator is about to write."""
    records = _seeded_records(pos.seed_named_coupons, [(None,)],
                              [], HISTORY_DAYS)
    assert records, "no coupons seeded"
    horizon = date.today() - timedelta(days=HISTORY_DAYS)
    # tuple layout: (. . . max_uses, uses_count, valid_from, valid_until, active)
    assert all(r[9] == horizon for r in records), \
        "named coupon window must start at the backfill horizon"
    assert all(r[10] > date.today() for r in records)
    assert all(r[8] == 0 for r in records)


def test_named_coupon_horizon_never_starts_after_existing_history():
    """A database older than the horizon keeps a window that covers its own
    oldest transaction."""
    oldest = date.today() - timedelta(days=HISTORY_DAYS + 15)
    records = _seeded_records(pos.seed_named_coupons, [(oldest,)],
                              [], HISTORY_DAYS)
    assert all(r[9] == oldest for r in records)


def test_generated_coupons_start_at_or_before_the_horizon():
    cfg = Config()
    depts = [{"department_id": "dept-1"}]
    prods = [{"product_id": "prod-1"}]
    # fetchone #1 = active-count guard, #2 = min(transaction_dt)
    records = _seeded_records(pos.seed_coupons, [(0,), (None,)],
                              cfg, depts, prods, HISTORY_DAYS)
    assert records, "no coupons seeded"
    horizon = date.today() - timedelta(days=HISTORY_DAYS)
    assert all(r[9] <= horizon for r in records), "generated coupon starts too late"
    assert all(r[10] >= date.today() for r in records)


def test_combo_deals_start_at_or_before_the_horizon():
    cfg = Config()
    depts = [{"department_id": "dept-1"}]
    prods = [{"product_id": "prod-1"}]
    records = _seeded_records(pos.seed_combo_deals, [(0,), (None,)],
                              cfg, depts, prods, HISTORY_DAYS)
    assert records, "no deals seeded"
    horizon = date.today() - timedelta(days=HISTORY_DAYS)
    # tuple layout: (name, desc, deal_type, trigger_qty, product, dept,
    #                deal_price, valid_from, valid_until, is_active)
    assert all(r[7] <= horizon for r in records), "deal starts too late"
    assert all(r[8] >= date.today() for r in records)


def test_active_promo_fetch_carries_the_window():
    """The generation-time guard can only enforce the window if the fetch
    hands it over — this pins the returned dict shape."""
    rows = [("c1", "percent_off", 0.1, None, None,
             date(2026, 5, 1), date(2026, 7, 1))]
    cur = _ScriptedCursor()
    cur.fetchall = lambda: rows
    coupons = pos._fetch_active_coupons(cur)
    assert coupons[0]["valid_from"] == date(2026, 5, 1)
    assert coupons[0]["valid_until"] == date(2026, 7, 1)

    deal_rows = [("d1", "x_for_price", 2, None, None, 1.0,
                  date(2026, 5, 1), date(2026, 7, 1))]
    cur = _ScriptedCursor()
    cur.fetchall = lambda: deal_rows
    deals = pos._fetch_active_deals(cur)
    assert deals[0]["valid_from"] == date(2026, 5, 1)
    assert deals[0]["valid_until"] == date(2026, 7, 1)


# ---------------------------------------------------------------------------
# Reconcile: windows + uses_count derived from recorded redemptions
# ---------------------------------------------------------------------------

def test_reconcile_widens_windows_and_recounts_uses():
    conn = _ScriptedConn(rowcounts=[3, 2, 1])
    touched = pos.reconcile_promotions(conn)

    assert touched == {"coupons": 3, "deals": 2, "coupons_zeroed": 1}
    assert conn.commits == 1, "reconcile must commit its own work"

    sql = [s for _, s, _ in conn.executed]
    assert len(sql) == 3

    coupon_sql = sql[0]
    assert "UPDATE pos.coupons" in coupon_sql
    assert "LEAST(c.valid_from, u.min_used)" in coupon_sql
    assert "GREATEST(c.valid_until, u.max_used)" in coupon_sql
    assert "count(DISTINCT ti.transaction_id)" in coupon_sql
    assert "uses_count = u.redemptions" in coupon_sql

    deal_sql = sql[1]
    assert "UPDATE pos.combo_deals" in deal_sql
    assert "LEAST(d.valid_from, u.min_used)" in deal_sql
    assert "GREATEST(d.valid_until, u.max_used)" in deal_sql

    zero_sql = sql[2]
    assert "SET uses_count = 0" in zero_sql


def test_reconcile_is_quiet_when_nothing_changed():
    conn = _ScriptedConn(rowcounts=[0, 0, 0])
    assert pos.reconcile_promotions(conn) == {
        "coupons": 0, "deals": 0, "coupons_zeroed": 0}
