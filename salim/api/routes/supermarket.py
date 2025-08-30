from fastapi import APIRouter

from ..database import conn

router = APIRouter(prefix="/supermarkets", tags=["supermarkets"])

cur = conn.cursor()


def to_json(cur, lst):
    records = []
    for row in lst:
        record = {}
        for i, column in enumerate(cur.description):
            record[column.name] = row[i]
        records.append(record)
    return records


@router.get("/")
async def get_supermarkets():
    query = "SELECT * FROM supermarkets"
    cur.execute(query)
    supermarkets = cur.fetchall()
    # Convert rows to a list of dictionaries
    records = to_json(cur, supermarkets)
    return records


@router.get("/{supermarket_id}")
async def get_super_by_id(id: int):
    query = "SELECT * FROM supermarkets WHERE id = %s"
    cur.execute(query, (id,))
    supermarket = cur.fetchone()
    if supermarket is None:
        return {"error": "Supermarket not found"}
    # Convert row to a dictionary
    record = {}
    for i, column in enumerate(cur.description):
        record[column.name] = supermarket[i]
    return record
