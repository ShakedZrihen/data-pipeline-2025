import datetime

try:
    from pymongo import MongoClient, ASCENDING
    from pymongo.errors import PyMongoError
except Exception:
    MongoClient = None
    ASCENDING = None
    PyMongoError = Exception

from .config import STATE_BACKEND, MONGO_URI, MONGO_DB, MONGO_COL

_mongo_col = None

def ensure_mongo():
    global _mongo_col
    if STATE_BACKEND != "mongo":
        return None
    if MongoClient is None:
        raise RuntimeError("pymongo not installed — cannot use MongoDB")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COL]
    col.create_index(
        [("provider", ASCENDING), ("branch", ASCENDING), ("type", ASCENDING)],
        unique=True,
    )
    _mongo_col = col
    print(f"[Init] Mongo ready: {MONGO_URI} db={MONGO_DB} col={MONGO_COL}")
    return _mongo_col

def update_last_run(provider: str, branch: str, data_type: str, last_ts_iso: str):
    now_iso = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    if STATE_BACKEND == "mongo":
        if _mongo_col is None:
            raise RuntimeError("Mongo collection not initialized")
        _mongo_col.update_one(
            {"provider": provider, "branch": branch, "type": data_type},
            {"$set": {"last_run": last_ts_iso, "updated_at": now_iso}},
            upsert=True,
        )
    else:
        print(f"[State] Skipped (STATE_BACKEND={STATE_BACKEND})")
