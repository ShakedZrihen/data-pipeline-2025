import time
import platform
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def init_chrome_options():
    chrome_options = Options()

    # headless chrome
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # force english UI/headers
    chrome_options.add_argument("--lang=en-US")
    chrome_options.add_experimental_option(
        "prefs",
        {"intl.accept_languages": "en,en_US"}
    )
    return chrome_options


def get_chromedriver_path():
    """Get the correct chromedriver path for the current system."""
    try:
        # Special-case macOS ARM
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            print("Detected macOS ARM64, using specific chromedriver...")
            from webdriver_manager.core.os_manager import ChromeType
            driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        else:
            driver_path = ChromeDriverManager().install()

        print(f"Chrome driver path: {driver_path}")
        return driver_path
    except Exception as e:
        print(f"Error with webdriver-manager: {e}")
        print("Falling back to system chromedriver...")
        return "chromedriver"


def get_chromedriver():
    chrome_options = init_chrome_options()

    print("Setting up Chrome driver...")
    try:
        chromedriver_path = get_chromedriver_path()
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        print("Trying alternative approach...")
        driver = webdriver.Chrome(options=chrome_options)
    return driver


def get_html_parser(driver, url):
    print(f"Navigating to {url}")
    driver.get(url)
    time.sleep(2)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    return soup


def session_from_driver(driver) -> requests.Session:
    """
    Build a requests.Session carrying Selenium's cookies + User-Agent,
    so downloads happen as the logged-in user.
    """
    session = requests.Session()

    # copy user-agent
    try:
        ua = driver.execute_script("return navigator.userAgent")
        if ua:
            session.headers["User-Agent"] = ua
    except Exception:
        pass
    session.headers.setdefault("Referer", driver.current_url)

    # Prefer CDP cookies (gets HttpOnly), fallback to Selenium cookies
    try:
        cookies = driver.execute_cdp_cmd("Network.getAllCookies", {}).get("cookies", [])
        for c in cookies:
            session.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
    except Exception:
        for c in driver.get_cookies():
            session.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))

    return session