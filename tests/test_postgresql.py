import pytest
from sqlalchemy.exc import ArgumentError, OperationalError
from sqlalchemy.pool import _ConnectionFairy
from ..dbhandlers.postgresql import PostgresHandler


class TestPostgresHandler:

    @pytest.fixture(autouse=True)
    def postgres_handler(self):
        self.CONN_STR = "postgresql+psycopg2://postgres:Gunnar14@localhost:5432/test"
        self.pg_handler = PostgresHandler(self.CONN_STR)

    def test_initialization_sets_db_connection(self):
        assert self.pg_handler.conn_str == self.CONN_STR

    def test_db_connection_raises_error(self):
        bad_conn_str = "Bad DSN connection string"
        bad_pg_handle = PostgresHandler(bad_conn_str)
        with pytest.raises(ArgumentError):
            bad_pg_handle.connection

        bad_pass_conn_str = "postgresql+psycopg2://postgres:Derpy@localhost:5432/test"
        bad_pass_pg_handle = PostgresHandler(bad_pass_conn_str)
        with pytest.raises(OperationalError):
            bad_pass_pg_handle.connection

        raise pytest.fail("Extract these tests and test _check_connection method with conn strings instead")


    def test_db_connection_does_not_raise_error(self):
        self.pg_handler.connection
        raise pytest.fail("Extract these tests and test _check_connection method with conn strings instead")

    def test_db_connection_is_of_correct_type(self):
        assert isinstance(self.pg_handler.connection, _ConnectionFairy)

