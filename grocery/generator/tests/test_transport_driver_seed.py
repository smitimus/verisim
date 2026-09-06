"""Regression: every warehouse seed must include >=1 'transport' employee.

Root cause of the 2026-09-06 e2e full-cycle failure: warehouse employee
departments were pure weighted-random (1 transport slot in 6), and a fresh
reseed drew ZERO transport employees -> dispatch_loads had no drivers ->
every transport.loads row got driver_id NULL -> the dbt not_null gate
(error tier) failed the pipeline.
"""
import grocery.generator.models.hr as hr
from grocery.generator.config import Config


class _Cursor:
    def __init__(self, sink):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, *args, **kwargs):
        pass

    def fetchone(self):
        return (0,)  # "active employees already present?" -> no, keep seeding

    def fetchall(self):
        return []


class _Conn:
    def __init__(self):
        self.records = []

    def cursor(self, *args, **kwargs):
        return _Cursor(self.records)

    def commit(self):
        pass


def _capture_execute_values(cur, sql, values, **kwargs):
    cur._sink.extend(values)


def _locations():
    return {
        'stores': [{'location_id': 's1', 'name': 'S1', 'location_type': 'store'}],
        'warehouses': [
            {'location_id': 'w1', 'name': 'W1', 'location_type': 'warehouse'},
            {'location_id': 'w2', 'name': 'W2', 'location_type': 'warehouse'},
        ],
    }


def test_warehouse_seed_always_has_transport(monkeypatch):
    monkeypatch.setattr(hr, 'execute_values', _capture_execute_values)
    cfg = Config()

    # 50 iterations: each warehouse draws 10-20 dept rolls; under the old
    # random-only seed, a warehouse missing 'transport' had ~8%/run chance,
    # so this would flake relentlessly.
    for _ in range(50):
        conn = _Conn()
        hr.seed_employees(conn, cfg, _locations())
        by_wh = {}
        for rec in conn.records:
            loc_id, dept = rec[0], rec[5]
            by_wh.setdefault(loc_id, []).append(dept)
        for wh in ('w1', 'w2'):
            assert 'transport' in by_wh[wh], (
                f'warehouse {wh} seeded without a transport driver: {by_wh[wh]}'
            )
