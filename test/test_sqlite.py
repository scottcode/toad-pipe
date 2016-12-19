import unittest
import sqlite3

from toadpipe.sql_task import ExecSqlList


TEST_DB = 'test.db'
SQL_FILES = []

class xxx(unittest.TestCase):
    def setUp(self):
        sqlite3.connect(TEST_DB)

    def test_sql_list(self):
        ExecSqlList(date='2016-12-05', sql_path_list=SQL_FILES)

    def tearDown(self):
        pass