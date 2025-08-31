# -*- coding: utf-8 -*-
"""
Single-file crawler:
1) Downloads all PDF/XLS/XLSX from https://www.gov.il/he/pages/cpfta_prices_regulations
2) Crawls 3 providers (including yohananof) on publishedprices, downloads latest PriceFull/PromoFull .gz,
   saves original filenames AND an S3-ready copy:
      ./providers/<provider>/<branch>/pricesFull_YYYYMMDDHH[MM].gz
      ./providers/<provider>/<branch>/promoFull_YYYYMMDDHH[MM].gz

Run:  python crawler.py
Deps: pip install requests beautifulsoup4 lxml selenium webdriver-manager
"""

import os, re, sys, time, logging, shutil
from datetime import datetime
from urllib.parse import urljoin, urlsplit, unquote
from collections import deque
import requests
from bs4 import BeautifulSoup

# ---- Selenium ----
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ================== CONFIG ==================
HEADLESS = False
TIMEOUT_SEC = 25

BASE_GOVIL_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"
DOWNLOAD_ROOT = os.path.join(os.path.dirname(__file__), "downloads", "gov_il")
FILE_EXTS = (".pdf", ".xls", ".xlsx")

PROVIDERS = ["yohananof", "Keshet", "RamiLevi"]
PUBLISHEDPRICES_LOGIN = "https://url.publishedprices.co.il/login"

LOGLEVEL = logging.INFO
logging.basicConfig(level=LOGLEVEL, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")

# ================== UTILS ==================
def ensure_dir(path: str): os.makedirs(path, exist_ok=True)

def today_folder(root: str) -> str:
    d = datetime.now().strftime("%Y-%m-%d")
    out = os.path.join(root, d); ensure_dir(out); return out

def safe_filename_from_url(url: str) -> str:
    name = os.path.basename(unquote(urlsplit(url).path))
    name = re.sub(r'[<>:"/\\|?*]+', "_", name).strip()
    return name or "downloaded_file"

def request_html(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    for i in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=20)
            r.encoding = r.encoding or "utf-8"
            r.raise_for_status()
            return r.text
        except Exception as e:
            logging.warning(f"HTTP attempt {i+1} failed: {e}"); time.sleep(1.0)
    return ""

def download_stream(url: str, out_dir: str) -> str:
    ensure_dir(out_dir)
    name = safe_filename_from_url(url)
    out_path = os.path.join(out_dir, name)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(8192):
                if chunk: f.write(chunk)
    logging.info(f"⬇️ Saved: {out_path}")
    return out_path

def parse_ts_from_str(s: str):
    # prefer 12 digits (YYYYMMDDHHMM), else 10 digits (YYYYMMDDHH)
    m12 = re.search(r'(\d{12})', s)
    if m12:
        try: return datetime.strptime(m12.group(1), "%Y%m%d%H%M"), m12.group(1)
        except: pass
    m10 = re.search(r'(\d{10})', s)
    if m10:
        try: return datetime.strptime(m10.group(1), "%Y%m%d%H"), m10.group(1)
        except: pass
    return None, None

# ================== GOV.IL CRAWL ==================
def extract_links_static(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml"); links = []

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip(); 
        if not href: continue
        full = urljoin(base_url, href)
        if full.lower().endswith(FILE_EXTS): links.append(full)

    for el in soup.select("[data-url], [data-href]"):
        href = (el.get("data-url") or el.get("data-href") or "").strip()
        if href:
            full = urljoin(base_url, href)
            if full.lower().endswith(FILE_EXTS): links.append(full)

    for m in re.finditer(r'href=["\']([^"\']+?\.(?:pdf|xls|xlsx))["\']', html, flags=re.IGNORECASE):
        full = urljoin(base_url, m.group(1)); links.append(full)

    uniq, seen = [], set()
    for u in links:
        if u not in seen: seen.add(u); uniq.append(u)
    return uniq

def build_driver(download_dir: str = None) -> webdriver.Chrome:
    opts = Options()
    if HEADLESS: opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu"); opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1920,1080"); opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    if download_dir:
        ensure_dir(download_dir)
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        opts.add_experimental_option("prefs", prefs)
    path = ChromeDriverManager().install()
    logging.info(f"Chrome driver: {path}")
    return webdriver.Chrome(service=Service(path), options=opts)

def click_expanders_and_scroll(driver: webdriver.Chrome):
    texts_he = ["הצג עוד","טען עוד","פתח","הרחב","הצגת כל","כל הרשימות","רשימות מחירים","מחירונים","מסמכים","פרטים נוספים"]
    texts_en = ["load more","show more","expand","documents","files","all"]
    for t in texts_he+texts_en:
        for el in driver.find_elements(By.XPATH, f"//*[contains(normalize-space(.), '{t}')]"):
            try: driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el); el.click(); time.sleep(0.3)
            except: pass
    for sel in ["[aria-expanded='false']","[role='button']",".accordion,.gov-accordion,.expand,.collapse,.more,.load-more,.btn,.button","details summary"]:
        for el in driver.find_elements(By.CSS_SELECTOR, sel):
            try:
                if el.get_attribute("aria-expanded") in (None,"false"):
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el); el.click(); time.sleep(0.2)
            except: pass
    last_h = 0
    for _ in range(6):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);"); time.sleep(0.7)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h: break
        last_h = h

def selenium_collect_links(url: str) -> list[str]:
    driver = build_driver()
    try:
        driver.get(url)
        WebDriverWait(driver, TIMEOUT_SEC).until(EC.presence_of_element_located((By.TAG_NAME,"body")))
        time.sleep(1.0)
        click_expanders_and_scroll(driver)
        urls = set()
        for a in driver.find_elements(By.XPATH, "//a[@href]"):
            href = a.get_attribute("href") or ""
            if href.lower().endswith(FILE_EXTS): urls.add(href)
        html = driver.page_source
        for m in re.finditer(r'(https?://[^\s"\'<>]+?\.(?:pdf|xls|xlsx))', html, flags=re.IGNORECASE):
            urls.add(m.group(1))
        return sorted(urls)
    finally:
        driver.quit()
def crawl_govil():
    out_dir = today_folder(DOWNLOAD_ROOT)
    seen = set()
    q = deque([(BASE_GOVIL_URL, 0)])
    found_any = False

    def is_file_url(u: str) -> bool:
        return u.lower().endswith(FILE_EXTS)

    def want_child(base_url: str, href: str) -> str | None:
        """החלטה אם להיכנס לדף משנה; מחזיר URL מוחלט אם כן."""
        if not href or href.startswith("#"):
            return None
        full = urljoin(base_url, href)
        # אם זה קובץ — נוריד בלי להעמיק
        if is_file_url(full):
            return full  # נשתמש בזה כהורדה ישירה (לא כ"דף")
        # נרד לדפי משנה גם אם הם מחוץ ל-gov.il, כל עוד זה עומק קטן
        return full

    while q:
        url, depth = q.popleft()
        if (url, depth) in seen or depth > 2:
            continue
        seen.add((url, depth))

        logging.info(f"[gov.il] Visiting (depth {depth}): {url}")
        html = request_html(url)
        if not html:
            continue

        # קבצים ישירים מהדף הנוכחי (סטטי)
        links = extract_links_static(html, url)

        # אם זה הדף הראשון ולא מצאנו — לנסות Selenium כדי לפתוח "הצג עוד"/אקורדיון
        if not links and depth == 0:
            logging.info("[gov.il] No links in static HTML, switching to Selenium …")
            try:
                links = selenium_collect_links(url)
            except Exception as e:
                logging.error(f"[gov.il] Selenium failed: {e}")

        if links:
            found_any = True
            logging.info(f"[gov.il] Found {len(links)} file(s) @ depth {depth}.")
            for u in links:
                try:
                    download_stream(u, out_dir)
                except Exception as e:
                    logging.error(f"[gov.il] Download failed for {u}: {e}")

        # הוספת דפי משנה/קבצים מהקישורים בדף:
        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            child = want_child(url, href)
            if not child:
                continue
            # אם זה קובץ — נוריד כאן ולא נוסיף ל-q
            if is_file_url(child):
                try:
                    download_stream(child, out_dir)
                    found_any = True
                except Exception as e:
                    logging.error(f"[gov.il] Download failed for {child}: {e}")
            else:
                # זה דף — נוסיף לתור עמוק יותר
                if (child, depth + 1) not in seen and depth + 1 <= 2:
                    q.append((child, depth + 1))

    if not found_any:
        logging.warning("[gov.il] No downloadable files found in page or children.")
        dbg = os.path.join(DOWNLOAD_ROOT, "debug_govil.html")
        ensure_dir(DOWNLOAD_ROOT)
        with open(dbg, "w", encoding="utf-8") as f:
            f.write(request_html(BASE_GOVIL_URL) or "")
        logging.info(f"[gov.il] Saved debug: {dbg}")

# ================== PROVIDERS CRAWL ==================
def provider_downloads(provider: str):
    base_dir = os.path.join(os.path.dirname(__file__), "providers", provider)
    temp_dir = os.path.join(base_dir, "temp"); ensure_dir(temp_dir)
    driver = build_driver(download_dir=temp_dir)

    def wait_for_files_area(d):
        sels = [
            "tbody.context.allow-dropdown-overflow tr",
            "table tbody tr",
            "table tr",
        ]
        for s in sels:
            if d.find_elements(By.CSS_SELECTOR, s):
                return True
        return False

    def collect_links_dom_regex():
        items = []
        # DOM
        rows = driver.find_elements(By.CSS_SELECTOR, "tbody.context.allow-dropdown-overflow tr, table tbody tr, table tr")
        for r in rows:
            try:
                a = r.find_element(By.CSS_SELECTOR, "a[href]")
                href = a.get_attribute("href") or ""
                txt  = (a.text or a.get_attribute("textContent") or "")
                joined = href + " " + txt
                if (".gz" in joined) and (("PriceFull" in joined) or ("PromoFull" in joined)):
                    tsdt, ts_raw = parse_ts_from_str(joined)
                    if tsdt:
                        items.append((href, txt, tsdt, ts_raw))
            except Exception:
                continue
        # Regex על כל ה-HTML (כולל lazy content)
        html = driver.page_source
        for m in re.finditer(r'(https?://[^\s"\'<>]+?\.gz)', html, flags=re.IGNORECASE):
            url = m.group(1)
            if ("PriceFull" in url) or ("PromoFull" in url):
                tsdt, ts_raw = parse_ts_from_str(url)
                if tsdt:
                    items.append((url, "", tsdt, ts_raw))
        # ביטול כפילויות לפי שם
        by_name = {}
        for href, txt, tsdt, ts_raw in items:
            name = os.path.basename(href.split("?")[0])
            by_name[name] = (href, txt, tsdt, ts_raw)
        return list(by_name.values())

    try:
        logging.info(f"[{provider}] Opening login …")
        driver.get(PUBLISHEDPRICES_LOGIN)
        WebDriverWait(driver, TIMEOUT_SEC).until(EC.presence_of_element_located((By.NAME,"username")))
        u = driver.find_element(By.NAME,"username"); p = driver.find_element(By.NAME,"password")
        u.clear(); u.send_keys(provider); p.clear(); p.send_keys(""); p.send_keys(Keys.RETURN)

        # המתנה לניווט /file ואז לטעינת רשומות או לפחות הגעת HTML עם "gz"
        WebDriverWait(driver, max(TIMEOUT_SEC, 35)).until(lambda d: d.current_url and "/file" in d.current_url)
        WebDriverWait(driver, max(TIMEOUT_SEC, 35)).until(lambda d: wait_for_files_area(d) or ("gz" in d.page_source))

        # גלילות לטעינה עצלה
        last_h = 0
        for _ in range(10):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.9)
            h = driver.execute_script("return document.body.scrollHeight;")
            if h == last_h: break
            last_h = h

        items = collect_links_dom_regex()
        if not items:
            # שמירת דיבאג
            png = os.path.join(base_dir, "debug_no_files.png")
            html = os.path.join(base_dir, "debug_no_files.html")
            try:
                driver.save_screenshot(png)
                with open(html, "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                logging.warning(f"[{provider}] No PriceFull/PromoFull links detected. Saved debug: {png} , {html}")
            except Exception:
                logging.warning(f"[{provider}] No links + debug save failed.")
            return

        latest_hour = max(x[2] for x in items)
        latest = [x for x in items if x[2] == latest_hour]
        logging.info(f"[{provider}] Found {len(latest)} files from {latest_hour.strftime('%Y-%m-%d %H:%M')}")

        for href, txt, tsdt, ts_raw in latest:
            filename = os.path.basename((href or txt).split("?")[0])
            try:
                # טריגר הורדה
                try:
                    el = driver.find_element(By.LINK_TEXT, filename); el.click()
                except Exception:
                    driver.get(href)
                # המתנה להורדה מלאה ל-temp
                for _ in range(180):
                    if os.path.exists(os.path.join(temp_dir, filename)) and not any(f.endswith(".crdownload") for f in os.listdir(temp_dir)):
                        break
                    time.sleep(1)
                src = os.path.join(temp_dir, filename)
                if not os.path.exists(src):
                    raise FileNotFoundError(f"Downloaded file not found: {src}")

                parts = filename.split("-")
                branch_id = parts[1] if len(parts) >= 3 else "unknown"
                final_dir = os.path.join(base_dir, branch_id); ensure_dir(final_dir)
                dst = os.path.join(final_dir, filename)
                os.replace(src, dst)
                logging.info(f"[{provider}] Saved original: {dst}")

                # S3-ready שם מקביל
                lower = (href + " " + txt).lower()
                kind = "pricesFull" if "pricefull" in lower else ("promoFull" if "promofull" in lower else None)
                if kind and ts_raw:
                    s3_name = f"{kind}_{ts_raw}.gz"
                    s3_path = os.path.join(final_dir, s3_name)
                    try: shutil.copyfile(dst, s3_path); logging.info(f"[{provider}] S3-ready copy: {s3_path}")
                    except Exception as e: logging.warning(f"[{provider}] S3 copy failed: {e}")

            except Exception as e:
                logging.error(f"[{provider}] Download error for {filename}: {e}")

    finally:
        driver.quit()
        try: shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e: logging.warning(f"[{provider}] temp cleanup: {e}")

def crawl_providers():
    for p in PROVIDERS:
        provider_downloads(p)

# ================== MAIN ==================
def main():
    # שלב 1: gov.il
    crawl_govil()
    # שלב 2: 3 ספקים (כולל yohananof)
    crawl_providers()
    logging.info("✅ Done.")

if __name__ == "__main__":
    main()
