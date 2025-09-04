# driver.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from config import HEADLESS

def make_driver(headless=HEADLESS):
    opts = Options()
    if headless:
        # modern headless flag; depending on chrome version you might need "--headless"
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(45)
    return driver
