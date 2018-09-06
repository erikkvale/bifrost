import pytest
from ..dbhandlers.postgresql import (
    initialize_engine,
    DataframeLoader
)
from sqlalchemy.exc import OperationalError


def test_initialize_engine_no_error():
    good_conn_str = 'postgresql+psycopg2://postgres:Gunnar14@localhost/test'
    initialize_engine(good_conn_str)


def test_intialize_engine_raises_error():
    bad_conn_str = 'postgresql+psycopg2://postgres:Gunnar14@localhost/tes'
    with pytest.raises(OperationalError):
        initialize_engine(bad_conn_str)







