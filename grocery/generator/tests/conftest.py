import contextlib
import importlib
from unittest.mock import patch

import pytest
from datetime import datetime, date

# The POS model was split into four sibling modules under `models/`
# (t_c2eca5dd: pos_catalog, pos_promotions, pos_loyalty, pos_txn), with
# `models/pos.py` left as a facade that re-exports every public name. Each
# sibling does `from psycopg2.extras import execute_values`, exactly as every
# other model module does — so patching `models.pos.execute_values` no longer
# reaches the writer. The helpers below patch every module that actually owns
# the symbol, so a test that captures bulk-insert payloads keeps working
# whether a function lives in `pos` or in one of its siblings.
POS_WRITE_MODULES = (
    "grocery.generator.models.pos_txn",
    "grocery.generator.models.pos_loyalty",
    "grocery.generator.models.pos_catalog",
    "grocery.generator.models.pos_promotions",
    # the facade keeps its own binding, for any caller that still patches it
    "grocery.generator.models.pos",
)


@contextlib.contextmanager
def patch_all_pos_writes(side_effect):
    """Patch `execute_values` on every POS model module.

    `side_effect` is required: an unpatched `execute_values` tries to encode
    SQL against the scripted cursor and fails with an AttributeError that has
    nothing to do with the test's intent.
    """
    with contextlib.ExitStack() as stack:
        for dotted in POS_WRITE_MODULES:
            try:
                mod = importlib.import_module(dotted)
            except ModuleNotFoundError:
                continue
            stack.enter_context(patch.object(mod, "execute_values", side_effect=side_effect))
        yield stack


@contextlib.contextmanager
def patch_txn_writes(side_effect):
    """Patch only the transaction writer's binding.

    Use when the test asserts on `pos.transactions` / `pos.transaction_items`
    payloads alone and wants the loyalty earn step left on the real symbol.
    """
    with patch("grocery.generator.models.pos_txn.execute_values", side_effect=side_effect):
        yield


class _CursorStub:
    def __init__(self, fetchone_result=None, fetchall_result=None):
        self._fetchone = fetchone_result
        self._fetchall = fetchall_result or []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *args, **kwargs):
        pass

    def fetchone(self):
        return self._fetchone

    def fetchall(self):
        return self._fetchall


class _ConnStub:
    def __init__(self, cursor_factory=None, fetchone_result=None, fetchall_result=None):
        self._fetchone = fetchone_result
        self._fetchall = fetchall_result or []
        self._cursor_factory = cursor_factory
    def cursor(self, *args, **kwargs):
        return _CursorStub(fetchone_result=self._fetchone, fetchall_result=self._fetchall)
    def commit(self):
        pass


@pytest.fixture
def conn_with_has_data():
    # Simulate a DB with data for a date
    # fetchone returns a non-None value, so has_data_for_date() -> True
    return _ConnStub(fetchone_result=(1,))


@pytest.fixture
def conn_without_has_data():
    # Simulate a DB with no data for a date
    # fetchone returns None, so has_data_for_date() -> False
    return _ConnStub(fetchone_result=None)
