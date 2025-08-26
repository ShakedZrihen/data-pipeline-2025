from dateutil import parser

def to_iso8601_utc(s):
    if not s:
        return None
    dt = parser.parse(str(s))

    if not dt.tzinfo:
        from datetime import timezone
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz=None).isoformat().replace("+00:00", "Z")

def str_or_none(v):
    if v is None:
        return None
    return str(v)

def num_or_none(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None
