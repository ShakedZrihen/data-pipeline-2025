import psycopg
from psycopg.rows import dict_row

def connect(dsn: str):
    return psycopg.connect(dsn, row_factory=dict_row)

UPSERT_SQL = """
INSERT INTO prices(provider,branch,type,ts,product,price,unit)
VALUES (%(provider)s,%(branch)s,%(type)s,%(ts)s,%(product)s,%(price)s,%(unit)s)
ON CONFLICT (provider,branch,type,ts,product) DO UPDATE
SET price = EXCLUDED.price,
    unit = EXCLUDED.unit,
    updated_at = now();
"""

def upsert_items(pg, rows: list[dict]) -> int:
    with pg.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
    pg.commit()
    return len(rows)
