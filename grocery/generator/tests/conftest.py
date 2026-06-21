import pytest
from datetime import datetime, date


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
