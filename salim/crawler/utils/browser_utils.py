import re
import time
import requests
import os
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


def get_chromedriver(headless: bool | None = None):
    """Create a Chrome WebDriver with sensible defaults."""
    if headless is None:
        headless_env = os.getenv("HEADLESS", "1").lower()
        headless = headless_env in ("1", "true", "yes")

    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,1000")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    remote_url = os.getenv("SELENIUM_REMOTE_URL")
    print("Setting up Chrome driver...")

    try:
        if remote_url:
            print(f"Using remote Selenium at {remote_url}")
            driver = webdriver.Remote(command_executor=remote_url, options=opts)
        else:
            chromedriver_path = os.getenv("CHROMEDRIVER_BIN")
            if chromedriver_path:
                print(f"Using local chromedriver: {chromedriver_path}")
                service = Service(executable_path=chromedriver_path)
                driver = webdriver.Chrome(service=service, options=opts)
            else:
                driver = webdriver.Chrome(options=opts)

        try:
            ver = driver.capabilities.get("chrome", {}).get("chromedriverVersion", "unknown")
            print(f"Chrome driver path/version: {ver}")
        except Exception:
            pass

        return driver

    except Exception as e:
        print(f"[ERROR] Failed to start Chrome WebDriver: {e}")
        raise


def get_html_parser(driver, url: str, wait_secs: float = 0.5):
    print(f"Navigating to {url}")
    driver.get(url)
    time.sleep(wait_secs)
    return BeautifulSoup(driver.page_source, "html.parser")


def session_from_driver(driver) -> requests.Session:
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
    if not name:
        return "default"
    name = re.sub(_ILLEGAL_WIN_CHARS, "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.rstrip(" .")
    return name or "default"
