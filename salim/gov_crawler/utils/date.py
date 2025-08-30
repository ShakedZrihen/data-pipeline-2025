from __future__ import annotations
import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# All parsed datetimes will be timezone-aware in Israel time.
JERUSALEM = ZoneInfo(os.getenv("TZ", "Asia/Jerusalem"))

# Relative expressions like:
# "3 minutes ago", "about 2 weeks ago", "an hour ago", "A day ago", etc.
_RELATIVE = re.compile(
    r"^(?:about|approximately|around|roughly)?\s*"
    r"(?P<qty>a|an|\d+)\s+"
    r"(?P<unit>seconds?|minutes?|hours?|days?|weeks?|months?|years?)\s+ago$",
    flags=re.IGNORECASE,
)

# Absolute datetime: "HH:MM DD/MM/YYYY" or "DD/MM/YYYY HH:MM".
# Also allows "-" as a separator and 2-digit years.
_ABS_TIME_DATE = re.compile(
    r"^(?:"
    r"(?P<time1>\d{1,2}:\d{2})\s+(?P<day1>\d{1,2})[\/\-](?P<month1>\d{1,2})[\/\-](?P<year1>\d{2,4})"
    r"|"
    r"(?P<day2>\d{1,2})[\/\-](?P<month2>\d{1,2})[\/\-](?P<year2>\d{2,4})\s+(?P<time2>\d{1,2}:\d{2})"
    r")$",
    flags=re.IGNORECASE,
)


def parse_absolute_date(text: str) -> datetime | None:
    """Parse strings like '12:45 29/08/2025' or '29-08-25 12:45' into tz-aware datetimes."""
    text = text.strip()
    m = _ABS_TIME_DATE.search(text)
    if not m:
        return None

    if m.group("time1"):
        time_str = m.group("time1")
        day = int(m.group("day1")); month = int(m.group("month1")); year = int(m.group("year1"))
    else:
        time_str = m.group("time2")
        day = int(m.group("day2")); month = int(m.group("month2")); year = int(m.group("year2"))

    if year < 100:
        year += 2000  # heuristic: 24 -> 2024

    try:
        base = datetime(year, month, day)
        h, mi = map(int, time_str.split(":"))
        if not (0 <= h < 24 and 0 <= mi < 60):
            return None
        base = base.replace(hour=h, minute=mi, tzinfo=JERUSALEM)
        return base
    except ValueError:
        return None

def parse_relative_date(text: str) -> datetime | None:
    """Parse strings like 'about 3 hours ago' into tz-aware datetimes."""
    text = text.strip().lower()
    m = _RELATIVE.match(text)
    if not m:
        return None

    qty_raw = m.group("qty")
    unit = m.group("unit").lower()

    qty = 1 if qty_raw in ("a", "an") else int(qty_raw)
    now = datetime.now(tz=JERUSALEM)

    if unit.startswith("second"): return now - timedelta(seconds=qty)
    if unit.startswith("minute"): return now - timedelta(minutes=qty)
    if unit.startswith("hour"):   return now - timedelta(hours=qty)
    if unit.startswith("day"):    return now - timedelta(days=qty)
    if unit.startswith("week"):   return now - timedelta(weeks=qty)
    if unit.startswith("month"):  return now - timedelta(days=30 * qty)   # approx
    if unit.startswith("year"):   return now - timedelta(days=365 * qty)  # approx
    return None

def parse_date(text: str) -> datetime | None:
    """Try relative first, then absolute."""
    return parse_relative_date(text) or parse_absolute_date(text)
