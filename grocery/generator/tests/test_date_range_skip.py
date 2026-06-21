from datetime import date
from grocery.generator.main import has_data_for_date


def test_has_data_for_date_true(conn_with_has_data):
    assert has_data_for_date(conn_with_has_data, date(2020, 1, 1)) is True


def test_has_data_for_date_false(conn_without_has_data):
    assert has_data_for_date(conn_without_has_data, date(2020, 1, 1)) is False
