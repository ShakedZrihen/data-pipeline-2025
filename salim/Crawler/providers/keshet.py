# Crawler/providers/keshet.py
import os, json, time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import requests
from urllib.parse import urlparse, urljoin

LOGIN_URL   = "https://url.publishedprices.co.il/login"
PAGE_URL    = "https://url.publishedprices.co.il/file"
BASE_HOST   = "https://url.publishedprices.co.il"

USERNAME    = "Keshet"                 # רק שם משתמש
USER_SEL    = "input[name='username']" # אם לא עובד, נסי '#username'
SUBMIT_SEL  = "#login-button, .row button"

FORM_SEL    = "form#file-op-form"
LINK_SEL    = "a[href$='.gz']"
PRICE_TOKEN = "pricefull"
PROMO_TOKEN = "promofull"

SLUG        = "keshet"

def _driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

def _login_and_get_form_html(driver):
    driver.get(LOGIN_URL)
    time.sleep(1.0)

    # הזנת שם משתמש ולחיצה על התחברות
    driver.find_element(By.CSS_SELECTOR, USER_SEL).clear()
    driver.find_element(By.CSS_SELECTOR, USER_SEL).send_keys(USERNAME)
    driver.find_element(By.CSS_SELECTOR, SUBMIT_SEL).click()
    time.sleep(1.0)

    # מעבר לעמוד רשימת הקבצים
    driver.get(PAGE_URL)
    time.sleep(1.0)
    return driver.page_source

def _collect_links_from_form(html):
    soup = BeautifulSoup(html, "html.parser")
    form = soup.select_one(FORM_SEL) or soup
    return [
        urljoin(BASE_HOST, a.get("href"))
        for a in form.select(LINK_SEL)
        if a.get("href")
    ]

def _split_price_promo(links):
    prices = [u for u in links if PRICE_TOKEN in u.lower()]
    promos = [u for u in links if PROMO_TOKEN in u.lower()]
    return prices, promos

def _session_from_driver(driver) -> requests.Session:
    s = requests.Session()
    for c in driver.get_cookies():
        s.cookies.set(
            name=c.get("name"),
            value=c.get("value"),
            domain=c.get("domain"),
            path=c.get("path", "/")
        )
    try:
        ua = driver.execute_script("return navigator.userAgent;")
        s.headers.update({"User-Agent": ua})
    except Exception:
        pass
    return s

def _download_files(session: requests.Session, urls, dest_folder):
    os.makedirs(dest_folder, exist_ok=True)
    for url in urls:
        if not url.lower().startswith("http"):
            url = urljoin(BASE_HOST, url)
        filename = os.path.basename(urlparse(url).path)
        out_path = os.path.join(dest_folder, filename)
        print(f"Downloading {filename} ...")
        try:
            with session.get(url, stream=True, timeout=120, verify=False) as r:
                r.raise_for_status()
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(1024 * 64):
                        if chunk:
                            f.write(chunk)
        except Exception as e:
            print(f"[WARN] failed: {url} -> {e}")

def run(ts: str):
    os.makedirs("out", exist_ok=True)
    out_prices = os.path.join("out", f"{SLUG}_prices_{ts}.json")
    out_promos = os.path.join("out", f"{SLUG}_promos_{ts}.json")
    out_folder = os.path.join("out", f"{SLUG}_{ts}")  # תיקיית הקבצים עצמם

    driver = _driver()
    try:
        html = _login_and_get_form_html(driver)
        links = _collect_links_from_form(html)
        prices, promos = _split_price_promo(links)

        # שמירת רשימות הלינקים
        with open(out_prices, "w", encoding="utf-8") as f:
            json.dump(prices, f, ensure_ascii=False, indent=2)
        with open(out_promos, "w", encoding="utf-8") as f:
            json.dump(promos, f, ensure_ascii=False, indent=2)

        session = _session_from_driver(driver)
        _download_files(session, prices, out_folder)
        _download_files(session, promos, out_folder)

        return out_prices, out_promos
    finally:
        driver.quit()

if __name__ == "__main__":
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    p, q = run(ts)
    print("saved:", p)
    print("saved:", q)
