
# -*- coding: utf-8 -*-
from __future__ import annotations
import re
from datetime import datetime
from typing import Dict, Iterable, List, Optional


class DataNormalizer:
    """
    Stateless helpers that turn 'raw' dicts from XMLProcessor into clean, uniform dicts.
    Output fields (per item):
      - product: str
      - price: float
      - unit: Optional[str]
      - barcode: Optional[str]
      - date: Optional[ISO8601]
      - meta: Optional[dict] (provider-lean details we keep but don't guarantee schema for)
    """

    _NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")

    @staticmethod
    def _to_float(val: Optional[str]) -> Optional[float]:
        if not val:
            return None
        m = DataNormalizer._NUM_RE.search(val.replace(" ", ""))
        if not m:
            return None
        txt = m.group(0).replace(",", ".")
        try:
            return float(txt)
        except Exception:
            return None

    @staticmethod
    def _to_date_iso(val: Optional[str]) -> Optional[str]:
        if not val:
            return None
        
        candidates = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y",
            "%Y-%m-%d",
        ]
        for fmt in candidates:
            try:
                return datetime.strptime(val.strip(), fmt).replace(microsecond=0).isoformat() + "Z"
            except Exception:
                continue
        return None

    @staticmethod
    def _norm_unit(u: Optional[str]) -> Optional[str]:
        if not u:
            return None
        u = u.strip().lower()
       
        mapping = {
            "l": "liter",
            "lt": "liter",
            "liter": "liter",
            "kg": "kg",
            "g": "g",
            "gram": "g",
            "unit": "unit",
            "pcs": "unit",
            "piece": "unit",
        }
        return mapping.get(u, u)

    def _norm_item(self, raw: Dict) -> Optional[Dict]:
        name = (raw.get("name") or "").strip()
        barcode = (raw.get("barcode") or "").strip() or None
        price = self._to_float(raw.get("price"))
        if not name and not barcode:
            return None

        item = {
            "product": name or barcode,
            "price": price,
            "unit": self._norm_unit(raw.get("unit")),
            "barcode": barcode,
            "date": self._to_date_iso(raw.get("date") or raw.get("update")),
            "meta": {
                k: v
                for k, v in raw.items()
                if k not in {"name", "price", "unit", "barcode", "date", "update"}
                and v is not None
            },
        }
        return item

   
    def normalize_prices(self, raws: Iterable[Dict]) -> List[Dict]:
        out: List[Dict] = []
        for r in raws:
            n = self._norm_item(r)
            if n:
                out.append(n)
        return out

    def normalize_promos(self, raws: Iterable[Dict]) -> List[Dict]:
       
        return self.normalize_prices(raws)
