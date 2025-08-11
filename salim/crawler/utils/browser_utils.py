import re
import time
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def get_chromedriver(headless: bool = True):
    """Create a Chrome WebDriver with sensible defaults."""
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1280,1000")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=opts)
    print("Setting up Chrome driver...")
    try:
        print(f"Chrome driver path: {driver.capabilities.get('chrome', {}).get('chromedriverVersion', 'unknown')}")
    except Exception:
        pass
    return driver


def get_html_parser(driver, url: str, wait_secs: float = 0.5):
    """Navigate to URL with Selenium and return a BeautifulSoup of the page."""
    print(f"Navigating to {url}")
    driver.get(url)
    time.sleep(wait_secs)
    return BeautifulSoup(driver.page_source, "html.parser")


def session_from_driver(driver) -> requests.Session:
    """
    Build a requests.Session that carries Selenium's cookies + UA + Referer.
    Useful for downloading files behind auth.
    """
    session = requests.Session()

    try:
        ua = driver.execute_script("return navigator.userAgent;")
        if ua:
            session.headers.update({"User-Agent": ua})
    except Exception:
        pass

    try:
        if driver.current_url:
            session.headers.update({"Referer": driver.current_url})
    except Exception:
        pass

    for c in driver.get_cookies():
        cookie_kwargs = {
            "name": c.get("name"),
            "value": c.get("value", ""),
            "domain": c.get("domain"),
            "path": c.get("path", "/"),
        }
        cookie_kwargs = {k: v for k, v in cookie_kwargs.items() if v}
        try:
            session.cookies.set(**cookie_kwargs)
        except Exception:
            session.cookies.set(c.get("name"), c.get("value", ""))

    return session


_ILLEGAL_WIN_CHARS = r'[<>:"/\\|?*\x00-\x1F]'

def sanitize_path_component(name: str) -> str:
    """
    Keep Unicode (e.g., Hebrew) but remove Windows-illegal characters.
    Also trims trailing dots/spaces which Windows rejects.
    """
    if not name:
        return "default"
    name = re.sub(_ILLEGAL_WIN_CHARS, "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.rstrip(" .")
    return name or "default"
