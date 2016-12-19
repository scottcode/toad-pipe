# import psycopg2.pool
#
# psycopg2.pool.SimpleConnectionPool
# psycopg2.pool.ThreadedConnectionPool
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sqlite3


class PSQLConn(object):
    """Stores the connection to psql."""
    def __init__(self, db, user, password, host):
        self.db = db
        self.user = user
        self.password = password
        self.host = host

    def connect(self):
        connection = psycopg2.connect(
                host=self.host,
                database=self.db,
                user=self.user,
                password=self.password)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection

class SqlLiteConn(object):
    """Stores the connection to psql."""
    def __init__(self, db, **args):
        self.db = db
        self.args = args

    def connect(self):
        connection = sqlite3.connect(self.db)
        return connection
