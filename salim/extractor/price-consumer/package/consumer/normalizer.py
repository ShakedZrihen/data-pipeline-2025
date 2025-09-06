# consumer/normalizer.py
from __future__ import annotations
import re
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

CANON_WS = re.compile(r"\s+")

def _canon(x: Optional[str]) -> str:
    x = (x or "").strip()
    x = CANON_WS.sub(" ", x)
    return x

def _to_float(val: Optional[str | float | int]) -> Optional[float]:
    if val is None: 
        return None
    if isinstance(val, (float, int)):
        return float(val)
    txt = str(val).strip().replace(",", ".")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", txt)
    return float(m.group(0)) if m else None

def parse_ts(value: Optional[str | datetime]) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        if not value:
            return datetime.now(timezone.utc)
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

_BRAND_RE = re.compile(r"^\s*(?:brand[:\-\s])?\s*([A-Za-z\u0590-\u05FF][^,|]*)[,|]?\s*(.+)$")

def _split_brand_name(name: str) -> tuple[Optional[str], str]:
    # naive split: "Brand, Product ..." or "Brand | Product"
    n = _canon(name)
    m = _BRAND_RE.match(n)
    if m:
        brand = _canon(m.group(1))
        prod  = _canon(m.group(2))
        return (brand or None), (prod or n)
    return None, n

class DataNormalizer:
    """
    Convert producer 'items' (product/price/unit/barcode/date/...) into
    DB-ready dicts:
      product_name, brand_name, barcode, price, discount_price?, ts?
    """
    def _norm_item(self, raw: Dict) -> Optional[Dict]:
        name = _canon(raw.get("product") or raw.get("name") or "")
        barcode = (raw.get("barcode") or "").strip() or None
        price = _to_float(raw.get("price"))
        discount = _to_float(raw.get("discount_price"))
        if not (name or barcode):
            return None

        brand, pname = _split_brand_name(name)
        return {
            "product_name": pname or (barcode or ""),
            "brand_name": brand,
            "barcode": barcode,
            "price": price,
            "discount_price": discount,
            "ts": parse_ts(raw.get("date") or raw.get("update") or raw.get("ts")),
        }

    def normalize_prices(self, raws: Iterable[Dict]) -> List[Dict]:
        out: List[Dict] = []
        for r in raws:
            n = self._norm_item(r)
            if n:
                out.append(n)
        return out

    def normalize_promos(self, raws: Iterable[Dict]) -> List[Dict]:
        # same shape for now
        return self.normalize_prices(raws)
