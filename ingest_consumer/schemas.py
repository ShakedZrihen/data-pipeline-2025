from pydantic import BaseModel, Field, validator
from typing import List, Optional
from datetime import datetime
import hashlib

class Item(BaseModel):
    product: str
    price: Optional[float]
    unit: Optional[str] = None

class Envelope(BaseModel):
    provider: str
    branch: str
    type: str
    timestamp: datetime
    items_total: int
    items_sample: List[Item] = []
    outbox_path: Optional[str] = None

def make_uid(provider, branch, type_, ts, product, unit):
    raw = f"{provider}|{branch}|{type_}|{ts.isoformat()}|{product}|{unit or ''}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()
