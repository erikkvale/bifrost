import unittest
import psycopg2
import psycopg2.extensions as pg_ext
from .dbhandler import PostgreSqlHandle


class TestPostgreSqlHandle(unittest.TestCase):

    def setUp(self):
        DB_CREDENTIALS = {
            'dbname': 'test_pg',
            'user': 'postgres',
            'password': 'Gunnar14',
            'host': 'localhost',
            'port': 5432
        }
        self.pg_handle = PostgreSqlHandle(**DB_CREDENTIALS)

    def test_connection_status_is_ready(self):
        self.assertEqual(self.pg_handle.conn.status, pg_ext.STATUS_READY)
        




