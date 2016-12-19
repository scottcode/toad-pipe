import unittest
import sqlite3
from datetime import datetime
from toadpipe.sql_task import ExecSqlList
from toadpipe.db import SqlLiteConn
import luigi

TEST_DB = 'test.db'
SQL_FILES = ['table_people.sql','table_job.sql','join_people_jobs.sql']
date = datetime.strptime('2016-12-05','%Y-%m-%d')
class xxx(unittest.TestCase):
    def setUp(self):
        self.db = SqlLiteConn(TEST_DB)

    def test_sql_list(self):
        luigi.build([ExecSqlList(date=date, sql_path_list=SQL_FILES, db=self.db)],local_scheduler=True)

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()