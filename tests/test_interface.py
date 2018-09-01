import pytest
from ..dbhandlers.interface import DbHandler


class TestDbHandler:

    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            dbh = DbHandler()
