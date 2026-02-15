import psycopg2
from contextlib import contextmanager
from config import PG

@contextmanager
def get_conn():
    conn = psycopg2.connect(**PG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
