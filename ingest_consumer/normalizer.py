from __future__ import annotations
import re
from typing import Optional,List, Dict
from .validator import Envelope, Item
from ingest_consumer.normalizer import normalize, build_rows

_UNIT_WORDS_LITER = [
    r"\b1(\.0)?\s*ליטר\b",
    r"\b1(\.0)?\s*l\b",
    r"\b1\s*ltr\b",
]

_unit_liter_regex = re.compile("|".join(_UNIT_WORDS_LITER), flags=re.IGNORECASE)


def normalize(envelope: dict) -> dict:
    """
      provider, branch, type, timestamp (ISO string), items: [{product, price, unit?}, ...]
    """
    return envelope

def build_rows(envelope: dict) -> List[Dict]:
    """
     prices.
    """
    env = normalize(envelope)
    provider = env["provider"]
    branch   = env["branch"]
    typ      = env["type"]
    ts       = env["timestamp"]

    rows: List[Dict] = []
    for item in env["items"]:
        rows.append({
            "provider": provider,
            "branch":   branch,
            "type":     typ,
            "ts":       ts,
            "product":  item["product"],
            "price":    float(item["price"]),
            "unit":     item.get("unit"),
        })
    return rows

def enrich(rec: dict) -> dict:
    unit = (rec.get("unit") or "").strip().lower()
    if unit in {"ltr", "lt", "l"}: unit = "liter"
    if not unit: unit = "unit"
    rec["unit"] = unit
    rec["product"] = " ".join(str(rec["product"]).split())
    return rec

def _infer_unit(product_name: str) -> Optional[str]:
    name = product_name or ""
    if _unit_liter_regex.search(name):
        return "liter"
    return None

def _clean_price(p) -> float:
    if isinstance(p, (int, float)):
        return float(p)
    s = str(p).replace(",", "").strip()
    return float(s)

def normalize(envelope: Envelope) -> Envelope:
    """
    (trim)
    """
    fixed_items: list[Item] = []
    for it in envelope.items:
        unit = it.unit or _infer_unit(it.product)
        fixed_items.append(
            Item(
                product=it.product.strip(),
                price=_clean_price(it.price),
                unit=unit
            )
        )

    return Envelope(
        provider=envelope.provider.strip(),
        branch=envelope.branch.strip(),
        type=envelope.type.strip(),
        timestamp=envelope.timestamp,
        items=fixed_items
    )
