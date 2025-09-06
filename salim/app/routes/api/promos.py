from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from ...db import get_session

router = APIRouter(prefix="/promos", tags=["promos"])

@router.get("/")
async def list_promos(session: AsyncSession = Depends(get_session)):
    sql = text("""
        SELECT *
        FROM promos_full
        LIMIT 10
    """)
    rows = (await session.execute(sql)).mappings().all()
    return {"items": [dict(r) for r in rows]}
