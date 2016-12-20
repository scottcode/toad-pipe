import os
import luigi
from .db import PSQLConn
from datetime import date
# from utils import PSQLConn,run_pca_models
# import sql_files

cred = PSQLConn(os.getenv("GPDB_DATABASE"), os.getenv("GPDB_USER"), os.getenv("GPDB_PASSWORD"), os.getenv("GPDB_HOST"))
TARGET_PATH = "target/{}/".format(date.today())


def exec_sql(sql_string, conn):
    curs = conn.cursor()
    curs.execute(sql_string)
    curs.close()
    conn.commit()
    return curs


def format_cursor(curs):
    return "Rowcount: {rc}, Status: {status}".format(rc=curs.rowcount, status=curs.statusmessage)


def read_file(path):
    with open(path, 'r') as f:
        return f.read()


# task to run user definied functions
class ExecSqlTask(luigi.Task):
    """Define user defined functions"""
    sql_path = luigi.Parameter()
    date = luigi.DateParameter(default=date.today())
    db = luigi.Parameter(default=cred)
    def run(self):
        conn = self.db.connect()
        curs = exec_sql(read_file(self.sql_path), conn)

        with self.output().open('w') as out_file:
            out_file.write(format_cursor(curs))

    def output(self):
        file_name = '{}'.format(self.sql_path).split('/')[-1].split('.')[0]
        return luigi.LocalTarget(os.path.join(TARGET_PATH,file_name))


class ExecSqlList(luigi.WrapperTask):
    date = luigi.DateParameter(default=date.today())
    #sql_path_list = luigi.ListParameter()
    sql_path_list = luigi.Parameter()
    db = luigi.Parameter(default=cred)

    def requires(self):
        return [ExecSqlTask(date=self.date, sql_path=sql_path, db=self.db) for sql_path in self.sql_path_list]

    def run(self):
        msg = "{cls} completed: date={date}, SQL Files: {sql}".format(
            cls=self.__class__, date=self.date, sql=self.sql_path_list)
        with self.output().open('w') as out_file:
            out_file.write(msg)

    def output(self):
        return luigi.LocalTarget(os.path.join(TARGET_PATH,'dummy'))
