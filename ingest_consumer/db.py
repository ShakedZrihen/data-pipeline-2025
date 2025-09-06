import sqlite3

def upsert_supermarket(db_path, provider: str, branch: str, name: str=None):
    con = sqlite3.connect(db_path)
    try:
        con.execute("""
            INSERT INTO supermarkets(provider, branch, name)
            VALUES(?, ?, ?)
            ON CONFLICT(provider, branch) DO UPDATE SET
              name = COALESCE(excluded.name, supermarkets.name)
        """, (provider, branch, name or provider))
        con.commit()
    finally:
        con.close()
