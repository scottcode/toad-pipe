import unittest
import sqlite3
from datetime import datetime
import os

from toadpipe.sql_task import ExecSqlList
from toadpipe.db import PSQLConn
import luigi


TEST_DIR = os.path.dirname(__file__)
SQL_FILES = ['table_people.sql','table_job.sql','join_people_jobs.sql']
SQL_PATHS = [os.path.join(TEST_DIR, f) for f in SQL_FILES]

date = datetime.strptime('2016-12-05','%Y-%m-%d')
class xxx(unittest.TestCase):
    def setUp(self):
        self.db = PSQLConn(os.getenv("TEST_DATABASE"), os.getenv("TEST_USER"), os.getenv("TEST_PASSWORD"), os.getenv("TEST_HOST"))
        self.conn = self.db.connect()
        with self.conn.cursor() as curs:
            curs.execute("""drop table if exists people; 
                drop table if exists job; 
                drop table if exists ppl_job;""")

    def test_sql_list(self):
        luigi.build([ExecSqlList(date=date, sql_path_list=SQL_PATHS, db=self.db)],local_scheduler=True)
        with self.conn.cursor() as curs:
            curs.execute('select count(1) from ppl_job;')
            res1 = curs.fetchone()
        self.assertEqual(res1[0], 2, 'Expected ppl_job table to have 3, got {}'.format(res1[0]))

    def tearDown(self):
        with self.conn.cursor() as curs:
            curs.execute("""drop table if exists people; 
                drop table if exists job; 
                drop table if exists ppl_job;""")

if __name__ == '__main__':
    unittest.main()