from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor


class DbClient:
    """
    PostgreSQL Database client wrapper for connecting and writing monitor stats.
    """

    def __init__(self, host, db, port, username, table_name):
        self.table_name = table_name

        # Read postgre user's password from file.
        with open('keys/psql.pass') as f:
            passw = f.read().rstrip()

        # Uri format got from Aiven console.
        uri = f'postgres://{username}:{passw}@{host}:{port}/{db}?sslmode=require'

        db_conn = psycopg2.connect(uri)
        db_conn.autocommit = True
        self.cursor = db_conn.cursor(cursor_factory=RealDictCursor)

    def insert(
            self,
            site_url: str,
            http_status: str,
            response_time_ms: int,
            matching: str
    ):
        query = (f'INSERT INTO {self.table_name} '
                 '(site_url, http_status, response_time_ms, found_pattern) '
                 'VALUES (%s, %s, %s, %s);')
        self.cursor.execute(
                query,
                (site_url, http_status, response_time_ms, matching)
        )
