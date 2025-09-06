import sqlite3, sys, pathlib
db = sys.argv[1]
sql = pathlib.Path(sys.argv[2]).read_text(encoding="utf-8")
con = sqlite3.connect(db)
try:
    con.executescript(sql)
    con.commit()
    print("OK")
finally:
    con.close()
