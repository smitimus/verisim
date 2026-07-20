"""
Regression test for verisim#14 — coupon/deal transaction linkage.

`pos.transaction_items` already has `coupon_id`/`deal_id` columns; the generator
used to leave them NULL. This test forces a coupon + a combo deal to apply and
asserts the applied IDs are propagated onto the relevant line items (no schema
change — just population of the existing columns).
"""
from datetime import datetime
from unittest.mock import patch

import random

import grocery.generator.models.pos as pos
from grocery.generator.config import Config


class _FakeConn:
    def cursor(self, *a, **k):
        class _C:
            def __enter__(self):
                return self

            def __exit__(self, *e):
                return False

            def execute(self, *a, **k):
                pass

            def fetchone(self):
                return None

            def fetchall(self):
                return []

        return _C()

    def commit(self):
        pass


_captured_items = []


def _fake_execute_values(cur, sql, records, template=None):
    # Capture pos.transaction_items inserts (8-tuple: txn, pid, qty, unit_price,
    # discount, coupon_id, deal_id, line_total).
    if 'transaction_items' in sql and records and len(records[0]) == 8:
        _captured_items.extend(records)


def test_coupon_deal_linkage(monkeypatch):
    random.seed(1)
    cfg = Config()
    cfg.coupons.coupon_use_rate = 1.0
    cfg.combo_deals.combo_use_rate = 1.0
    # Force the random gate to always apply the promo.
    monkeypatch.setattr(random, "random", lambda: 0.0)

    conn = _FakeConn()
    sim_dt = datetime(2026, 6, 1, 12, 0, 0)

    class _Scenario:
        active_promotions = []
        coupon_multiplier = 1.0
        scenario_tag = "normal"

    scenario = _Scenario()
    stores = [{"location_id": "loc1", "location_type": "store"}]
    products = [
        {
            "product_id": f"p{i}",
            "department": "Produce",
            "uom": "each",
            "price": 1.0,
            "current_price": 1.0,
            "department_name": "Produce",
        }
        for i in range(5)
    ]
    employees = [
        {
            "department": "store",
            "location_type": "store",
            "employee_id": "e1",
            "location_id": "loc1",
        }
    ]
    members = [{"member_id": "m1"}]
    coupons = [
        {
            "coupon_id": "c1",
            "coupon_type": "percent_off",
            "discount_value": 0.1,
            "department_id": None,
            "product_id": None,
        }
    ]
    deals = [
        {
            "deal_id": "d1",
            "deal_type": "x_for_price",
            "trigger_qty": 2,
            "trigger_department_id": None,
            "trigger_product_id": None,
            "deal_price": 1.0,
        }
    ]

    _captured_items.clear()
    with patch("grocery.generator.models.pos.execute_values", side_effect=_fake_execute_values):
        pos.generate_pos_transactions(
            conn, cfg, sim_dt, 5, scenario, stores, products, employees, members, coupons, deals
        )

    items = _captured_items
    assert items, "no transaction_items captured"

    # Whole-transaction coupon must be linked to every line item.
    coupon_tagged = [r for r in items if r[5] == "c1"]
    assert coupon_tagged, "coupon_id not linked to line items"

    # Combo deal must be linked to its trigger line items.
    deal_tagged = [r for r in items if r[6] == "d1"]
    assert deal_tagged, "deal_id not linked to trigger line items"
