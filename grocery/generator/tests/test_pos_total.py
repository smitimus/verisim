from datetime import datetime
from unittest.mock import patch

import grocery.generator.models.pos as pos
from grocery.generator.config import Config


class _DummyScenario:
    def __init__(self):
        self.active_promotions = []  # no promo discounts
        self.coupon_multiplier = 1.0
        self.scenario_tag = 'normal'


class _FakeConn:
    def __init__(self, fetch_records=None):
        self._fetch_records = fetch_records
    def cursor(self, *args, **kwargs):
        records = self._fetch_records
        class _C:
            def __enter__(self_inner):
                return self_inner
            def __exit__(self_inner, exc_type, exc, tb):
                return False
            def execute(self_inner, *a, **k):
                pass
            def fetchone(self_inner):
                return None
            def fetchall(self_inner):
                return records if records is not None else []
        # Bind access to outer scope value via closure
        return _C()
    def commit(self):
        pass


def test_transaction_total_calculation(monkeypatch):
    import random
    random.seed(0)
    # Prepare a fake DB and a simple single-product transaction
    fake_conn = _FakeConn()
    cfg = Config()
    sim_dt = datetime.now()
    scenario = _DummyScenario()
    store_locations = [{'location_id': 'loc1'}]
    products = [{'product_id': 'p1', 'department': 'Produce', 'uom': 'each', 'price': 10.0}]
    employees = [{
        'department': 'store',
        'location_type': 'store',
        'employee_id': 'e1',
        'location_id': 'loc1',
        'cashier_role': True,
        'manager_role': False,
    }]
    members = []
    coupons = []
    deals = []

    captured = []

    def fake_execute_values(cur, sql, records, template=None):
        captured.append(list(records))
        return None

    with patch('grocery.generator.models.pos.execute_values', side_effect=fake_execute_values):
        pos.generate_pos_transactions(fake_conn, cfg, sim_dt, 1, scenario,
                                    store_locations, products, employees, members, coupons, deals)

    # Basic structural checks: ensure a record was captured and has a total field
    assert captured, "No records captured from execute_values"
    first_batch = captured[0]
    assert first_batch and len(first_batch) > 0
    first_txn = first_batch[0]
    total_index = 9
    assert len(first_txn) > total_index
    assert isinstance(first_txn[total_index], (int, float))
    assert first_txn[total_index] > 0
