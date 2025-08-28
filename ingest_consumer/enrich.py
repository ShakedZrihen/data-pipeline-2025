from decimal import Decimal
from datetime import datetime, timezone

def normalize_ts(ts: str | datetime) -> datetime:
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc)
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if isinstance(ts, str) else ts
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc)
    raise ValueError("invalid timestamp")

def normalize_unit(unit: str | None, product: str) -> str:
    if unit:
        return unit.strip()
    return "liter" if "חלב" in product or "milk" in product.lower() else "ea"

def normalize_price(p: float | int | str) -> Decimal:
    return Decimal(str(p)).quantize(Decimal("0.01"))

def normalize_product(p: str) -> str:
    return " ".join(p.split()).replace(" %", "%")

def normalize_branch(b: str) -> str:
    return " ".join(b.split())

def enrich_item(provider: str, branch: str, type_: str, ts, item: dict) -> dict:
    return {
        "provider": provider.strip(),
        "branch": normalize_branch(branch),
        "type": type_.strip(),
        "ts": normalize_ts(ts),
        "product": normalize_product(item["product"]),
        "price": normalize_price(item["price"]),
        "unit":  normalize_unit(item.get("unit"), item["product"]),
    }
