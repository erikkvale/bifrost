import pytest
from ..dbhandlers.postgresql import PostgresHandler


class TestPostgresHandler:

    @pytest.fixture(autouse=True)
    def postgres_handler(self):
        self.CONN_STR = "postgresql+psycopg2://postgres:3rikkv@le@localhost:5432/test"
        self.pg_handler = PostgresHandler(self.CONN_STR)

    def test_initialization_sets_db_connection(self):
        assert self.pg_handler.conn_str == self.CONN_STR

