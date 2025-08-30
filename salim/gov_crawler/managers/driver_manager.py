import time, platform, requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from urllib.parse import urlparse

class DriverManager:
    def __init__(self):
        self.driver = None

    def init_chrome_options(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--lang=en-US")
        chrome_options.add_experimental_option("prefs", {"intl.accept_languages": "en,en_US"})
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        return chrome_options

    def get_chromedriver_path(self):
        try:
            if platform.system() == "Darwin" and platform.machine() == "arm64":
                print("Detected macOS ARM64, using specific chromedriver...")
                driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
            else:
                driver_path = ChromeDriverManager().install()
            print(f"Chrome driver path: {driver_path}")
            return driver_path
        except Exception as e:
            print(f"Error with webdriver-manager: {e}")
            print("Falling back to system chromedriver...")
            return "chromedriver"

    def get_chromedriver(self):
        chrome_options = self.init_chrome_options()
        print("Setting up Chrome driver...")
        try:
            chromedriver_path = self.get_chromedriver_path()
            service = Service(chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            print(f"Failed to initialize Chrome driver: {e}")
            print("Trying alternative approach...")
            self.driver = webdriver.Chrome(options=chrome_options)
        return self.driver

    def get_html_parser(self, url):
        if not self.driver:
            self.get_chromedriver()
        print(f"Navigating to {url}")
        self.driver.get(url)
        time.sleep(2)
        return BeautifulSoup(self.driver.page_source, "html.parser")

    def build_session(self) -> requests.Session:
        """יוצר requests.Session עם UA ו-headers בסיסיים, ומסנכרן קוקיז התחלתיים מהדפדפן."""
        if not self.driver:
            self.get_chromedriver()
        s = requests.Session()
        try:
            ua = self.driver.execute_script("return navigator.userAgent;")
        except Exception:
            ua = "Mozilla/5.0"
        s.headers.update({
            "User-Agent": ua,
            "Accept": "*/*",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        })
        self.sync_cookies(s)  
        return s

    def sync_cookies(self, session: requests.Session, url: str | None = None):
        """ממזג קוקיז מהדפדפן לתוך הסשן. אם ניתן URL, נסנן לקוקיז שרלוונטיים לדומיין הזה."""
        host = urlparse(url).hostname.lower() if url else None
        cookies = []
        try:
            cookies = self.driver.execute_cdp_cmd("Network.getAllCookies", {}).get("cookies", [])
        except Exception:
            pass
        if not cookies:
            try:
                cookies = self.driver.get_cookies()
            except Exception:
                cookies = []

        for c in cookies:
            dom = (c.get("domain") or "").lstrip(".").lower()
            if host and dom and not (host == dom or host.endswith("." + dom)):
                continue
            session.cookies.set(
                c.get("name"),
                c.get("value", ""),
                domain=dom or None,
                path=c.get("path", "/"),
            )
