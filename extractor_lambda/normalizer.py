# normalizer.py
import re
from datetime import datetime, timezone

def _ts_to_iso(ts: str) -> str:
    """'YYYYMMDDHH' או 'YYYYMMDDHHMM' -> ISO8601 Z"""
    if len(ts) == 12:
        dt = datetime.strptime(ts, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    elif len(ts) == 10:
        dt = datetime.strptime(ts, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    else:
        raise ValueError(f"Bad timestamp: {ts}")
    return dt.isoformat().replace("+00:00", "Z")

def parse_key(key: str):
    """
    תומך בשני דגמים:
    A) providers/<prov>/<branch>/(pricesFull|promoFull)_<YYYYMMDDHH[MM]>.gz
    B) providers/<prov>/<branch>/(Price|Promo)(Full)?<chain>-<branch>-<YYYYMMDDHH[MM]>.gz
       למשל: providers/Keshet/103/PriceFull7290785400000-103-202509010630.gz
    מחזיר: (provider, branch, kind, iso_ts), כאשר kind הוא 'pricesFull'/'promoFull'
    """
    k = key.replace("\\", "/")

    # דגם A — S3-ready
    m = re.match(r"^providers/([^/]+)/([^/]+)/(?:prices?full|promofull)_(\d{10,12})\.gz$", k, flags=re.I)
    if m:
        provider, branch, ts = m.groups()
        kind = "pricesFull" if "price" in k.lower() else "promoFull"
        return provider, branch, kind, _ts_to_iso(ts)

    # דגם B — שם מקורי מהפורטל
    m = re.match(
        r"^providers/([^/]+)/([^/]+)/(price|promo)(?:full)?[A-Za-z0-9._-]*-[A-Za-z0-9._-]*-(\d{10,12})\.gz$",
        k, flags=re.I
    )
    if m:
        provider, branch, base_kind, ts = m.groups()
        kind = "pricesFull" if base_kind.lower() == "price" else "promoFull"
        return provider, branch, kind, _ts_to_iso(ts)

    raise ValueError(f"Bad key format: {key}")
