import re
from datetime import datetime, timezone

KEY_RE = re.compile(r"^providers/(?P<provider>[^/]+)/(?P<branch>[^/]+)/(?P<type>pricesFull|promoFull)_(?P<ts>.+).gz$")

def parse_key(key: str):
    m = KEY_RE.match(key)
    if not m:
        raise ValueError(f"Bad key format: {key}")
    provider = m.group("provider")
    branch = m.group("branch")
    type_ = m.group("type")
    ts = m.group("ts")
    try:
        dt = datetime.strptime(ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        iso_ts = dt.isoformat()
    except:
        iso_ts = datetime.now(tz=timezone.utc).isoformat()
    return provider, branch, type_, iso_ts
