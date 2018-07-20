import unittest
import psycopg2
import psycopg2.extensions as pg_ext
from dbhandlers.dbhandler import PostgreSqlHandle
from collections import OrderedDict


TEST_DB_NAME = 'test_pg'
TEST_USER = 'postgres'
TEST_PASSWORD = 'Gunnar14'
TEST_HOST = 'localhost'
TEST_PORT = 5432


class TestPostgreSqlHandle(unittest.TestCase):

    def setUp(self):
        self.pg_handle = PostgreSqlHandle(
            dbname=TEST_DB_NAME,
            user=TEST_USER,
            password=TEST_PASSWORD,
            host=TEST_HOST,
            port=TEST_PORT
        )

    def test_connection_status_is_ready(self):
        self.assertEqual(
            self.pg_handle.conn.status,
            pg_ext.STATUS_READY
        )

    def test_repr(self):
        self.assertEqual(
            eval(repr(self.pg_handle)),
            self.pg_handle
        )





