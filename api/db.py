import sqlite3
from contextlib import contextmanager

DB_PATH = r"data\prices.db"

@contextmanager
def get_conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        yield con
    finally:
        con.close()
