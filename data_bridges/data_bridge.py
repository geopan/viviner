import utils.constants as c
from utils.requester import Requester

import psycopg2 as pg
from psycopg2.extensions import connection


class DataBridge:
    def __init__(self, conn: connection):
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.r = Requester(c.API_URL)

    def commit(self):
        self.cursor.close()
        self.conn.commit()
