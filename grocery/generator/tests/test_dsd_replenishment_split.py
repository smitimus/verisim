"""
One shelf, one replenishment channel — kanban t_d59b551f.

THE DEFECT
----------
Nothing excluded a DSD vendor's SKUs from warehouse replenishment. `ordering.
check_and_create_orders` raised a store order for ANY low-stock SKU, so a
direct-store-delivery vendor's perishables were ordered from our warehouse,
rode one of our `transport.loads`, and a pick short was recorded at the dock as
`detected_source = 'receiving'` — against a vendor whose own row says
direct-store-delivery only.

That is what made `SEMA-16` red the first time it ever ran against populated
vendor tables: 20-37 rows on a fresh seed, in both `origin/main` and the branch
that found it. And it is a real modelling error, not a test artefact: the fill
rate and on-time marts were attributing one vendor's failures to two different
channels, one of which that vendor does not even use.

WHAT THESE TESTS PIN
--------------------
1. the store-order query itself excludes `fulfillment_model = 'dsd'` — the
   split is made once, at the only place a warehouse order is raised, so it
   cannot be forgotten by fulfillment or transport downstream;
2. a product with no vendor is still ordered (the LEFT JOIN must not swallow
   an unassigned SKU);
3. `SEMA-30` exists and says the same thing in SQL, so the invariant is checked
   against a real seed and not only against a statement's text.
"""
import os
import pathlib
from datetime import datetime

import pytest

from grocery.generator.models import ordering
from grocery.generator.tests import cross_schema_integrity as csi


class _Cursor:
    def __init__(self, db):
        self.db = db
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        flat = ' '.join(str(sql).split())
        self.db.statements.append(flat)
        if 'FROM inv.stock_levels sl' in flat:
            self._rows = self.db.low_stock
        else:
            self._rows = []

    def fetchall(self):
        return self._rows


class _Conn:
    """A connection that answers only the low-stock read and records the SQL."""

    def __init__(self, low_stock=()):
        self.statements = []
        self.low_stock = list(low_stock)
        self.commits = 0

    def cursor(self, *args, **kwargs):
        return _Cursor(self)

    def commit(self):
        self.commits += 1


def _low_stock_query():
    """The statement `check_and_create_orders` runs to find what to reorder."""
    conn = _Conn(low_stock=[])
    ordering.check_and_create_orders(
        conn, [{'location_id': 'store-1'}], [{'location_id': 'wh-1'}], [],
        datetime(2026, 10, 19, 0, 0, 0))
    statements = [s for s in conn.statements if 'FROM inv.stock_levels sl' in s]
    assert statements, 'check_and_create_orders never read the shelf'
    return statements[0]


def test_the_store_order_query_skips_dsd_vendor_skus():
    """The channel split lives in the one place a warehouse order is raised."""
    sql = _low_stock_query()

    assert 'inv.suppliers' in sql, (
        'the store-order query does not consult the vendor at all, so a DSD '
        'vendor\u2019s SKUs are ordered from our warehouse:\n%s' % sql)
    assert "= 'dsd'" in sql and 'NOT (' in sql, (
        'the query must exclude a DSD vendor\u2019s SKUs by the vendor\u2019s own '
        'fulfillment_model:\n%s' % sql)
    assert 'is_active' in sql, (
        'a deactivated DSD vendor must not take its shelves out of the '
        'warehouse channel — that would starve them:\n%s' % sql)


def test_the_split_does_not_swallow_an_unassigned_product():
    """A SKU with no vendor row is still ours to order — LEFT JOIN, not JOIN."""
    sql = _low_stock_query()

    assert 'LEFT JOIN inv.suppliers' in sql, (
        'an inner join would drop every product whose supplier_id is NULL, '
        'and those shelves would never be replenished at all:\n%s' % sql)
    assert "COALESCE(sup.fulfillment_model, 'warehouse')" in sql, (
        'an unassigned SKU must default to the warehouse channel:\n%s' % sql)


def test_sema_30_publishes_the_same_invariant():
    """The rule as data, not only as code — it has to hold on a real seed.

    SEMA-16 asks whether the short-ship's `detected_source` matches the vendor's
    model. That is a symmetric check on the EVENT, and it only goes red once the
    wrong-channel order has already reached a dock. SEMA-30 asks the upstream
    question, which is the one that prevents it: an order line may not exist for
    a SKU the vendor delivers direct.
    """
    spec = next((a for a in csi.ASSERTIONS if a.id == 'SEMA-30'), None)
    assert spec is not None, (
        'SEMA-30 is missing from cross_schema_integrity.ASSERTIONS — the '
        'channel split would then be unguarded on a seeded database')
    sql = spec.sql.lower()
    assert 'ordering.store_order_items' in sql
    assert 'inv.suppliers' in sql and "'dsd'" in sql
    assert 'current_date' in sql, (
        'the assertion must scope to completed days, or a partial backfill day '
        'produces false orphans')


def test_the_sql_spec_carries_sema_30():
    """The published copy is generated; this is the check that it was."""
    path = pathlib.Path(__file__).resolve().parents[1] / 'sql' / \
        'check_cross_schema_integrity.sql'
    if not os.path.exists(path):
        pytest.skip('sql spec not present')
    contents = path.read_text(encoding='utf-8')
    assert 'SEMA-30' in contents, (
        'regenerate with grocery/generator/scripts/regen_integrity_sql.py')
