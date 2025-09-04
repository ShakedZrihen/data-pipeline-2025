import platform
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def init_chrome_options(download_dir):
    options = Options()
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    return options

def get_chromedriver():
    try:
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            from webdriver_manager.core.os_manager import ChromeType
            return ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        return ChromeDriverManager().install()
    except Exception as e:
        print(f"Error with webdriver-manager: {e}")
        print("Falling back to system chromedriver...")
        return "chromedriver"

def start_driver(chrome_options):
    try:
        return webdriver.Chrome(service=Service(get_chromedriver()), options=chrome_options)
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        print("Trying alternative approach without Service...")
        return webdriver.Chrome(options=chrome_options)
