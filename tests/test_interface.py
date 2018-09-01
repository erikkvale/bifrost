import pytest
from ..dbhandlers.interface import DbHandler


class TestDbHandler:

    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            dbh = DbHandler()

    def test_fulfilled_interface_of_subclass(self):
        _conn_str = "Some database connection string"

        class SubDbHandler(DbHandler):
            def __init__(self, conn_str):
                pass
            def connection(self):
                pass
            def read(self):
                pass
            def write(self):
                pass

        sub_db_handler = SubDbHandler(_conn_str)

    def test_un_fulfilled_interface_of_subclass(self):
        _conn_str = "Some database connection string"

        class SubDbHandler(DbHandler):
            def __init__(self, conn_str):
                pass
            # def connection(self):
            #     pass
            def read(self):
                pass
            def write(self):
                pass

        with pytest.raises(TypeError):
            sub_db_handler = SubDbHandler(_conn_str)
