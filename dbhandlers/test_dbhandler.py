import unittest
import psycopg2
import psycopg2.extensions as pg_ext
from .dbhandler import PostgreSqlHandle
from collections import OrderedDict


class TestPostgreSqlHandle(unittest.TestCase):

    def setUp(self):
        self.DB_CREDENTIALS = OrderedDict({
            'dbname': 'test_pg',
            'user': 'postgres',
            'password': 'Gunnar14',
            'host': 'localhost',
            'port': 5432
        })
        self.pg_handle = PostgreSqlHandle(**self.DB_CREDENTIALS)

    def test_connection_status_is_ready(self):
        self.assertEqual(self.pg_handle.conn.status, pg_ext.STATUS_READY)


    def test_repr_argument_ordering(self):
        self.assertEqual(
            repr(self.pg_handle),
            "PostgreSqlHandle("
            "{dbname}, {user}, {password}, "
            "{host}, {port})".format(**self.DB_CREDENTIALS)
        )





