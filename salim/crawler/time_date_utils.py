from datetime import datetime, timedelta
from consts import JERUSALEM, _ABS_TIME_DATE, _RELATIVE

def parse_absolute_date(text):
    text = text.strip()
    m = _ABS_TIME_DATE.search(text)
    if not m:
        return None  # must have both time and date in one of the two orders

    # determine which branch matched
    if m.group("time1"):
        time_str = m.group("time1")
        day = int(m.group("day1"))
        month = int(m.group("month1"))
        year = int(m.group("year1"))
    else:
        time_str = m.group("time2")
        day = int(m.group("day2"))
        month = int(m.group("month2"))
        year = int(m.group("year2"))

    if year < 100:
        year += 2000  # two-digit heuristic

    try:
        base = datetime(year, month, day)
    except ValueError:
        return None

    # apply time
    try:
        h, mi = map(int, time_str.split(":"))
    except ValueError:
        return None
    if 0 <= h < 24 and 0 <= mi < 60:
        base = base.replace(hour=h, minute=mi)

    return base.replace(tzinfo=JERUSALEM)


def parse_relative_date(text):
    text = text.strip().lower()
    m = _RELATIVE.match(text)
    if not m:
        return None

    qty_raw = m.group("qty")
    unit = m.group("unit").lower()

    if qty_raw in ("a", "an"):
        qty = 1
    else:
        try:
            qty = int(qty_raw)
        except ValueError:
            return None

    now = datetime.now(tz=JERUSALEM)

    if unit.startswith("second"):
        return now - timedelta(seconds=qty)
    if unit.startswith("minute"):
        return now - timedelta(minutes=qty)
    if unit.startswith("hour"):
        return now - timedelta(hours=qty)
    if unit.startswith("day"):
        return now - timedelta(days=qty)
    if unit.startswith("week"):
        return now - timedelta(weeks=qty)
    if unit.startswith("month"):
        return now - timedelta(days=30 * qty)
    if unit.startswith("year"):
        return now - timedelta(days=365 * qty)

    return None


def parse_date(text):
    rel = parse_relative_date(text)
    if rel:
        return rel
    return parse_absolute_date(text)