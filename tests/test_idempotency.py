import psycopg, os
from ingest_consumer.storage import UPSERT_SQL

def test_upsert_idempotent():
    dsn = os.environ.get("PG_DSN")
    if not dsn: return
    with psycopg.connect(dsn) as pg:
        with pg.cursor() as cur:
            cur.execute("DELETE FROM prices;")
            pg.commit()
        row = {"provider":"p","branch":"b","type":"t","ts":"2025-01-01T00:00:00Z","product":"x","price":1.0,"unit":"ea"}
        with pg.cursor() as cur:
            cur.execute(UPSERT_SQL, row)
            cur.execute(UPSERT_SQL, row)
        pg.commit()
        with pg.cursor() as cur:
            cur.execute("SELECT count(*) FROM prices;")
            assert cur.fetchone()[0] == 1
