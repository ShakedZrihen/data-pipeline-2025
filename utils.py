# utils.py
import os
import certifi
import requests
from datetime import datetime, timedelta
import os
from urllib.parse import urlparse, unquote, urljoin
from config import ABS_DATE_RE, REL_HE_RE, logger

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def parse_absolute_he_date(text):
    if not text:
        return None
    m = ABS_DATE_RE.search(text)
    if not m:
        return None
    hh, mm, dd, mo, yy = m.groups()
    try:
        return datetime(int(yy), int(mo), int(dd), int(hh), int(mm))
    except Exception:
        return None

def parse_relative_he(text):
    if not text:
        return None
    m = REL_HE_RE.search(text)
    if not m:
        return None
    num = m.group(1)
    unit = m.group(2)
    n = int(num) if num and num.isdigit() else 1
    now = datetime.now()
    unit = unit.strip()
    if unit.startswith("שנייה"):
        return now - timedelta(seconds=n)
    if unit.startswith("דקה"):
        return now - timedelta(minutes=n)
    if unit.startswith("שעה"):
        return now - timedelta(hours=n)
    if unit.startswith("יום"):
        return now - timedelta(days=n)
    return now

def session_from_driver(driver):
    """Create requests.Session that copies cookies and user-agent from Selenium driver."""
    s = requests.Session()
    ua = driver.execute_script("return navigator.userAgent;")
    s.headers.update({"User-Agent": ua})
    for c in driver.get_cookies():
        # set cookie on session (domain may be None - requests handles that)
        s.cookies.set(c['name'], c['value'], domain=c.get('domain', None), path=c.get('path', '/'))
    return s

def download_stream(session, url, dest, verify=True):
    """Stream file to `dest` using provided requests.Session."""
    try:
        verify_target = certifi.where() if verify else False
        with session.get(url, stream=True, timeout=60, verify=verify_target) as r:
            r.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in r.iter_content(8192):
                    if chunk:
                        fh.write(chunk)
        return True
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        return False

# small helper to normalize file name from URLs
def filename_from_url(u):
    return unquote(_os_module.path.basename(urlparse(u).path))

# expose some util names for easier imports
__all__ = [
    "ensure_dir",
    "parse_absolute_he_date",
    "parse_relative_he",
    "session_from_driver",
    "download_stream",
    "filename_from_url",
]
