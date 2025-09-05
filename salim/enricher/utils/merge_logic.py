# enricher/merge_logic.py
from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

def _to_decimal(x) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None

def _parse_ts(ts: str | None) -> datetime:
    if not ts:
        return datetime(1970,1,1, tzinfo=timezone.utc)
    try:
        # allow '...Z'
        if ts.endswith("Z"):
            ts = ts.replace("Z","+00:00")
        return datetime.fromisoformat(ts).astimezone(timezone.utc)
    except Exception:
        return datetime(1970,1,1, tzinfo=timezone.utc)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

# -------- PRICES --------
def build_rows_from_prices(
    chain_id: str,
    store_id: str,
    payload_ts: str,
    items: List[Dict[str, Any]],
    existing_by_code: Dict[str, Dict[str, Any]]
) -> List[Tuple[Any,...]]:
    """
    Return rows ready for batch_upsert_items() VALUES:
    (chain_id, store_id, code, name, brand, unit, qty, unit_price,
     regular_price, promo_price, promo_start, promo_end, last_price_ts, last_promo_ts)
    We only 'update' if payload_ts is newer than existing last_price_ts (decided here).
    """
    ts = _parse_ts(payload_ts)
    out: List[Tuple[Any,...]] = []
    for it in items:
        code = (it.get("code") or "").strip()
        if not code:
            continue

        ex   = existing_by_code.get(code)
        ex_price_ts = _parse_ts(ex.get("last_price_ts")) if ex else datetime(1970,1,1, tzinfo=timezone.utc)
        if ts <= ex_price_ts:
            if ex is None:
                pass
            else:
                continue

        name  = it.get("clean_name") or it.get("name")
        brand = it.get("brand") or it.get("manufacturer_norm") or it.get("manufacturer")
        unit  = it.get("unit")
        qty   = _to_decimal(it.get("qty"))
        unit_price = _to_decimal(it.get("unit_price"))
        regular_price = _to_decimal(it.get("price"))

        promo_price = ex.get("promo_price") if ex else None
        promo_start = ex.get("promo_start") if ex else None
        promo_end   = ex.get("promo_end") if ex else None
        last_promo_ts = ex.get("last_promo_ts") if ex else None

        out.append((
            chain_id, store_id, code,
            name, brand, unit, qty, unit_price,
            regular_price, promo_price, promo_start, promo_end,
            ts, last_promo_ts
        ))
    return out

# -------- PROMOS --------
def _is_active(start: Optional[str], end: Optional[str], ref: datetime) -> bool:
    s = _parse_ts(start) if start else None
    e = _parse_ts(end) if end else None
    if s and s > ref: return False
    if e and e < ref: return False
    return True

def _best_promo_for_code(
    code: str,
    promos: List[Dict[str, Any]],
    regular_price: Optional[Decimal],
    ref: datetime
) -> Optional[Tuple[Decimal, datetime | None, datetime | None]]:
    if regular_price is None:
        return None
    best_price: Optional[Decimal] = None
    best_start: Optional[datetime] = None
    best_end: Optional[datetime] = None

    for p in promos:
        if p.get("code") != code:
            continue
        if not _is_active(p.get("start_at"), p.get("end_at"), ref):
            continue

        abs_p = _to_decimal(p.get("discounted_price"))
        rate  = _to_decimal(p.get("discount_rate_pct"))
        if abs_p is not None:
            promo_price = abs_p
        elif rate is not None:
            promo_price = (regular_price * (Decimal(1) - (rate / Decimal(100)))).quantize(Decimal("0.01"))
        else:
            continue

        if promo_price >= regular_price:
            continue

        s = _parse_ts(p.get("start_at")) if p.get("start_at") else None
        e = _parse_ts(p.get("end_at")) if p.get("end_at") else None

        if (best_price is None) or (promo_price < best_price):
            best_price, best_start, best_end = promo_price, s, e

    if best_price is None:
        return None
    return (best_price, best_start, best_end)

def build_rows_from_promos(
    chain_id: str,
    store_id: str,
    apply_ts: str,
    promos: List[Dict[str, Any]],
    existing_by_code: Dict[str, Dict[str, Any]]
) -> List[Tuple[Any,...]]:
    """
    Returns rows where promo is improved/changed and is newer than last_promo_ts
    Fields order matches batch_upsert_items().
    """
    ref = _parse_ts(apply_ts)
    out: List[Tuple[Any,...]] = []

    promo_codes = {p.get("code") for p in promos if p.get("code")}
    for code in promo_codes:
        ex = existing_by_code.get(code)
        if not ex:
            continue  
        regular_price = _to_decimal(ex.get("regular_price"))
        best = _best_promo_for_code(code, promos, regular_price, ref)
        ex_promo_ts = _parse_ts(ex.get("last_promo_ts")) if ex else datetime(1970,1,1,tzinfo=timezone.utc)
        if not best:
            if ex.get("promo_price") is not None:
                if ref > ex_promo_ts:
                    out.append((
                        ex.get("chain_id", chain_id), store_id, code,
                        ex.get("name"), ex.get("brand"), ex.get("unit"), ex.get("qty"), ex.get("unit_price"),
                        ex.get("regular_price"),
                        None, None, None,    # clear promo
                        ex.get("last_price_ts"), ref
                    ))
            continue

        promo_price, s, e = best
        if ref <= ex_promo_ts:
            continue

        out.append((
            ex.get("chain_id", chain_id), store_id, code,
            ex.get("name"), ex.get("brand"), ex.get("unit"), ex.get("qty"), ex.get("unit_price"),
            ex.get("regular_price"),
            promo_price, s, e,
            ex.get("last_price_ts"), ref
        ))

    return out
