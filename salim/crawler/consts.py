import re
from zoneinfo import ZoneInfo

# timezone used for all parsed datetimes
JERUSALEM = ZoneInfo("Asia/Jerusalem")

# relative expression with optional prefix like "about"
_RELATIVE = re.compile(
    r"^(?:about|approximately|around|roughly)?\s*"
    r"(?P<qty>a|an|\d+)\s+"
    r"(?P<unit>seconds?|minutes?|hours?|days?|weeks?|months?|years?)\s+ago$",
    flags=re.IGNORECASE,
)

# absolute datetime: either "time + date" or "date + time"
# explicitly name groups separately to avoid reuse conflicts
_ABS_TIME_DATE = re.compile(
    r"^(?:"
    r"(?P<time1>\d{1,2}:\d{2})\s+(?P<day1>\d{1,2})[\/\-](?P<month1>\d{1,2})[\/\-](?P<year1>\d{2,4})"
    r"|"
    r"(?P<day2>\d{1,2})[\/\-](?P<month2>\d{1,2})[\/\-](?P<year2>\d{2,4})\s+(?P<time2>\d{1,2}:\d{2})"
    r")",
    flags=re.IGNORECASE,
)
