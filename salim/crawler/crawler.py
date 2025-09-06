                      
# -*- coding: utf-8 -*-

import os
import sys
import re
import gzip
import json
import time
import logging
import requests
import urllib3
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin

import boto3

          
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import StaleElementReferenceException

                                                        
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("crawler")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class UniversalSupermarketCrawler:
    """
    Crawler that:
      - Logs in with Selenium (if needed) and syncs cookies to a requests.Session
      - Finds .gz/.xml links (and Download endpoints), makes them absolute, and tries to download with requests
      - Falls back to a real browser click or URL navigation if direct GET fails
      - Validates GZIP/XML before upload
      - Uploads to S3 as: providers/<provider>/<branch>/<file>
        where <file> is pricesFull_<UTC>_b{n}.gz, promoFull_<UTC>_b{n}.gz, or storesFull_<UTC>.{gz|xml}
    """

    def __init__(self, bucket_name: str, config_file: str | None = None, local_mode: bool = False):
        """
        bucket_name: when local_mode=True, treated as a local folder path to save files under ./providers/...
        local_mode: if True, do not upload to S3; save under <bucket_name>/providers/<provider>/<branch>/.
        """
        self.bucket = bucket_name
        self.local_mode = bool(local_mode)
        self.s3 = boto3.client("s3") if not self.local_mode else None
        self.config = self._load_config(config_file)

        self.download_dir = Path.cwd() / "downloads"
        self.download_dir.mkdir(exist_ok=True)

                                                        
        self.ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

                                                                        
        self.session = requests.Session()
        self.session.verify = False                                              
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
            "Accept": "application/x-gzip, application/gzip, application/octet-stream;q=0.9, */*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
        })

                                         
    def _slugify(self, s: str) -> str:
        s = (s or '').strip().lower()
                                                                                   
        import re as _re
        s = _re.sub(r"[^A-Za-z0-9\u0590-\u05FF ]+", "", s)
        s = _re.sub(r"\s+", "-", s)
        return s or "branch"

    def _load_stores_names(self, provider: str) -> list[str]:
        """Load provider stores list (names in order) from S3 mapping if available."""
        if self.local_mode or not self.s3:
            return []
        key = f"mappings/{provider}_stores.json"
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            import json as _json
            data = _json.loads(obj["Body"].read().decode("utf-8"))
            stores = data.get('stores') if isinstance(data, dict) else None
            if isinstance(stores, list):
                names = [(x.get('name') or '').strip() for x in stores]
                return [n for n in names if n]
        except Exception:
            pass
        return []
    def _read_xml_bytes(self, path: Path) -> bytes:
        data = path.read_bytes()
        if path.suffix.lower() == '.gz':
            try:
                return gzip.decompress(data)
            except Exception:
                                         
                with gzip.open(path, 'rb') as gz:
                    return gz.read()
        return data

    def _parse_stores_xml(self, xml_bytes: bytes) -> list[dict]:
        import xml.etree.ElementTree as ET
        import re as _re

                                                                                
        root = None
        raw_text = None
        for enc in (None, 'utf-8', 'windows-1255', 'iso-8859-8'):
            try:
                if enc is None:
                    root = ET.fromstring(xml_bytes)
                    raw_text = xml_bytes.decode('utf-8', errors='ignore')
                else:
                    txt = xml_bytes.decode(enc, errors='ignore').lstrip('\ufeff')
                    root = ET.fromstring(txt)
                    raw_text = txt
                if root is not None:
                    break
            except Exception:
                continue
        if raw_text is None:
            try:
                raw_text = xml_bytes.decode('utf-8', errors='ignore')
            except Exception:
                raw_text = ''

        stores: list[dict] = []

        def norm_tag(t: str) -> str:
            t = t or ''
            return t.split('}')[-1].lower()

                                                   
        name_keys    = {'storename','branchname','name','שם','שם_סניף','שם סניף','שם_חנות','שם חנות'}
        addr_keys    = {'address','street','כתובת','רחוב'}
        city_keys    = {'city','עיר','ישוב','יישוב'}

        def pick(info: dict, t: str, val: str):
            lt = t.replace('_','').lower()
            if info['name'] is None and (lt in name_keys or any(k in lt for k in name_keys)) and 'chain' not in lt and 'code' not in lt and 'id' not in lt:
                info['name'] = val
                return True
            if info['address'] is None and (lt in addr_keys or any(k in lt for k in addr_keys)) and 'code' not in lt and 'id' not in lt:
                info['address'] = val
                return True
            if info['city'] is None and (lt in city_keys or any(k in lt for k in city_keys)) and 'code' not in lt and 'id' not in lt:
                vclean = (val or '').strip()
                info['city'] = vclean
                return True
            return False

                                                
        if root is not None:
            candidates = []
            for n in root.iter():
                try:
                    t = norm_tag(n.tag)
                except Exception:
                    continue
                                                                                                                          
                if 'store' in t:
                    candidates.append(n)
                    continue
                ctags = [norm_tag(c.tag) for c in list(n)]
                if any(ct in ('storename','branchname','name','שם') for ct in ctags):
                    candidates.append(n)

            for node in candidates:
                info = {'name': None, 'address': None, 'city': None}
                                      
                for ch in list(node):
                    t = norm_tag(ch.tag)
                    val = (ch.text or '').strip()
                    if val:
                        pick(info, t, val)
                                      
                    for k, v in (ch.attrib or {}).items():
                        if v:
                            pick(info, k, v)
                                 
                for k, v in (getattr(node,'attrib',{}) or {}).items():
                    if v:
                        pick(info, k, v)
                                                   
                if not info['name'] or (not info['address'] and not info['city']):
                    for ch in list(node):
                        for g in list(ch):
                            t = norm_tag(g.tag)
                            val = (g.text or '').strip()
                            if val:
                                pick(info, t, val)
                if info['name']:
                    stores.append(info)

                                                  
        if not stores and raw_text:
                             
            names = _re.findall(r"<\s*(?:StoreName|BranchName|Name|שם(?:[_ ](?:סניף|חנות))?)\b[^>]*>(.*?)<\s*/\s*(?:StoreName|BranchName|Name|שם(?:[_ ](?:סניף|חנות))?)\s*>", raw_text, _re.IGNORECASE | _re.DOTALL)
            addrs = _re.findall(r"<\s*(?:Address|Street|כתובת|רחוב)\b[^>]*>(.*?)<\s*/\s*(?:Address|Street|כתובת|רחוב)\s*>", raw_text, _re.IGNORECASE | _re.DOTALL)
            cities = _re.findall(r"<\s*(?:City|עיר|ישוב|יישוב)\b[^>]*>(.*?)<\s*/\s*(?:City|עיר|ישוב|יישוב)\s*>", raw_text, _re.IGNORECASE | _re.DOTALL)
                                           
            for m in _re.finditer(r"<\s*Store\b([^>]*)>", raw_text, _re.IGNORECASE):
                attrs = m.group(1) or ''
                for k, v in _re.findall(r"([A-Za-z\u0590-\u05FF_]+)\s*=\s*\"([^\"]+)\"", attrs):
                    kl = k.strip().lower()
                    vv = v.strip()
                    if 'name' in kl or 'שם' in kl:
                        names.append(vv)
                    elif 'address' in kl or 'street' in kl or 'כתובת' in kl or 'רחוב' in kl:
                        addrs.append(vv)
                    elif 'city' in kl or 'עיר' in kl or 'ישוב' in kl or 'יישוב' in kl:
                        cities.append(vv)

            def clean(x):
                return ' '.join((x or '').strip().split())
            nmax = max(len(names), len(addrs), len(cities))
            for i in range(nmax):
                nm = clean(names[i] if i < len(names) else '')
                ad = clean(addrs[i] if i < len(addrs) else '')
                ct = clean(cities[i] if i < len(cities) else '')
                if nm:
                    stores.append({'name': nm, 'address': ad or None, 'city': ct or None})

        return stores

    def _upload_branch_map_json(self, provider: str, entries: list[dict]) -> None:
        """Upload provider stores to S3 as mappings/<provider>_stores.json"""
        if self.local_mode or not self.s3:
            return
        try:
            key = f"mappings/{provider}_stores.json"
            body = json.dumps({'provider': provider, 'stores': entries}, ensure_ascii=False).encode('utf-8')
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType='application/json; charset=utf-8')
            logger.info(f"Uploaded stores mapping JSON to s3://{self.bucket}/{key} ({len(entries)} entries)")
        except Exception as e:
            logger.warning(f"Failed to upload stores mapping JSON: {e}")

    def _upload_combined_branch_map(self, provider: str, entries: list[dict]) -> None:
        """Merge provider stores into mappings/branch_map.json as:
        { provider: { <store_name>: {city,address}, ... }, ... }
        """
        if self.local_mode or not self.s3:
            return
        combined_key = "mappings/branch_map.json"
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=combined_key)
            data = obj["Body"].read()
            combined = json.loads(data.decode("utf-8")) if data else {}
            if not isinstance(combined, dict):
                combined = {}
        except Exception:
            combined = {}

        prov_map = {}
        for r in entries:
            name = (r.get('name') or '').strip()
            if not name:
                continue
            prov_map[name] = {
                'city': (r.get('city') or '').strip() or None,
                'address': (r.get('address') or '').strip() or None,
            }
        combined[provider] = prov_map

        try:
            body = json.dumps(combined, ensure_ascii=False).encode('utf-8')
            self.s3.put_object(Bucket=self.bucket, Key=combined_key, Body=body, ContentType='application/json; charset=utf-8')
            logger.info(f"Updated combined mapping at s3://{self.bucket}/{combined_key}")
        except Exception as e:
            logger.warning(f"Failed to upload combined branch_map.json: {e}")

    def _upload_stores_debug(self, provider: str, s3_key: str, raw_bytes: bytes, parsed_count: int) -> None:
        """Upload a small debug file with a text preview of the Stores content to help troubleshooting."""
        if self.local_mode or not self.s3:
            return
        try:
            try:
                txt = raw_bytes.decode('utf-8', errors='ignore')
            except Exception:
                txt = ''
            sample = txt[:2048]
            debug_key = f"mappings/debug/{provider}_stores_debug_{self.ts}.txt"
            body = (
                f"provider: {provider}\n"
                f"stores_key: {s3_key}\n"
                f"size_bytes: {len(raw_bytes)}\n"
                f"parsed_count: {parsed_count}\n"
                f"sample_first_2kb:\n{sample}\n"
            ).encode('utf-8')
            self.s3.put_object(Bucket=self.bucket, Key=debug_key, Body=body, ContentType='text/plain; charset=utf-8')
            logger.info(f"Uploaded stores debug to s3://{self.bucket}/{debug_key}")
        except Exception as e:
            logger.warning(f"Failed to upload stores debug: {e}")

                                                                
            
                                                                
    def _load_config(self, file_path: str | None):
        if file_path and Path(file_path).exists():
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
                                    
        return {
            "supermarkets": [
                {
                    "name": "yohananof",
                    "display_name": "יוחננוף",
                    "url": "https://url.publishedprices.co.il/login",
                    "username": "yohananof",
                    "password": ""
                },
                {"name": "victory", "display_name": "ויקטורי", "url": "https://laibcatalog.co.il/"},
                {"name": "carrefour", "display_name": "קרפור", "url": "https://prices.carrefour.co.il/"},
                {"name": "shufersal", "display_name": "שופרסל", "url": "http://prices.shufersal.co.il/"},
                {"name": "mega", "display_name": "מגה", "url": "https://prices.mega.co.il/"},
                {"name": "wolt", "display_name": "Wolt", "url": "https://wm-gateway.wolt.com/isr-prices/public/v1/index.html"},
                {"name": "super-pharm", "display_name": "סופר-פארם", "url": "http://prices.super-pharm.co.il/"},
                {"name": "hazi-hinam", "display_name": "חצי חינם", "url": "https://shop.hazi-hinam.co.il/Prices"}
            ],
            "max_branches": 2,
            "max_files_per_site": 10
        }

                                                                
              
                                                                
    def setup_driver(self):
        options = Options()
        try:
            options.add_argument("--headless=new")
        except Exception:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        prefs = {
            "download.default_directory": str(self.download_dir.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(options=options)
                                            
        try:
            driver.execute_cdp_cmd("Page.setDownloadBehavior", {
                "behavior": "allow",
                "downloadPath": str(self.download_dir.absolute()),
            })
        except Exception:
            pass
        return driver

    def sync_cookies_to_session(self, driver):
        """Copy Selenium cookies into the requests session and set Referer."""
        jar = requests.cookies.RequestsCookieJar()
        for c in driver.get_cookies():
            name, value = c.get("name"), c.get("value")
            domain = c.get("domain") or ""
            path = c.get("path") or "/"
            if name and value:
                jar.set(name, value, domain=domain, path=path)
        self.session.cookies.update(jar)
        try:
            self.session.headers["Referer"] = driver.current_url
        except Exception:
            pass
        logger.info("Synchronized Selenium cookies into requests session")

    def detect_site_type(self, driver, url):
        logger.info(f"Detecting site type for {url}")
        driver.get(url)
        time.sleep(2)

                    
        for selector in [
            "input[type='password']",
            "input[name*='user']",
            "#username",
            ".login-form",
            "form[action*='login']"
        ]:
            if driver.find_elements(By.CSS_SELECTOR, selector):
                logger.info("Detected: Login required site")
                return "login"

                               
        tables = driver.find_elements(By.TAG_NAME, "table")
        if tables:
            for t in tables:
                if t.find_elements(By.CSS_SELECTOR, "a[href*='.gz'], a[href*='download']"):
                    logger.info("Detected: Table-based site")
                    return "table"

                       
        for selector in [".downloadBtn", "a[href*='.gz']", "a[href*='Price']", "a[href*='Promo']", "a[href*='download']"]:
            if driver.find_elements(By.CSS_SELECTOR, selector):
                logger.info("Detected: Direct download site")
                return "direct"

        logger.info("Detected: Generic site")
        return "generic"

    def handle_login(self, driver, supermarket) -> bool:
        """Best-effort login; still sync cookies even if no password is required."""
        if "username" not in supermarket:
            return False
        logger.info(f"Attempting login for {supermarket.get('display_name', supermarket['name'])}")
        try:
            user = None
            for sel in ["#username", "input[name*='user']", "input[type='text']"]:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    user = els[0]; break
            if user:
                user.clear()
                user.send_keys(supermarket["username"])

            pwd = None
            for sel in ["input[type='password']", "input[name*='pass']"]:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    pwd = els[0]; break
            if pwd and supermarket.get("password"):
                pwd.clear()
                pwd.send_keys(supermarket["password"])

            for sel in ["button[type='submit']", "input[type='submit']", ".login-button", "button:not([type='button'])"]:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    els[0].click()
                    time.sleep(2)
                    break

            self.sync_cookies_to_session(driver)
            return True
        except Exception as e:
            logger.warning(f"Login flow issue: {e}")
            self.sync_cookies_to_session(driver)
            return False

                                                                
                                                                     
                                                                
    def wait_for_gz_links(self, driver, timeout=20):
        """Wait for any file/Download anchors (gz/xml or download endpoint, or 'הורדה' text)."""
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                    "a[href$='.gz'], a[href*='.gz?'], a[href$='.xml'], a[href*='.xml?'], "
                    "a[href*='download'], a[download]"))
            )
            return True
        except Exception:
                                                 
            try:
                WebDriverWait(driver, 4).until(
                    EC.presence_of_element_located((By.XPATH, "//a[contains(.,'הורדה')]"))
                )
                return True
            except Exception:
                return False

    def scroll_and_check(self, driver, steps=4, pause=0.7):
        for _ in range(steps):
            driver.execute_script("window.scrollBy(0, document.body.scrollHeight/3);")
            time.sleep(pause)
            if driver.find_elements(By.CSS_SELECTOR,
                "a[href$='.gz'], a[href*='.gz?'], a[href$='.xml'], a[href*='.xml?'], a[href*='download'], a[download]"):
                return True
        return False

    def click_nav_candidates_to_find_links(self, driver):
        keywords = [
            "מחיר", "מבצע", "קבצים", "רשימות", "חיפוש",                            
            "Price", "Promo", "Download", "Files", "List", "Search"
        ]
        anchors = driver.find_elements(By.CSS_SELECTOR, "a, button")
        anchors = sorted(
            anchors,
            key=lambda el: (
                "menu" not in (el.get_attribute("role") or "").lower(),
                len((el.text or "").strip()) == 0
            )
        )
        tried = 0
        for el in anchors:
            if tried >= 8:
                break
            txt = (el.text or "").strip()
            if not txt:
                continue
            low = txt.lower()
            if any(k in low for k in [k.lower() for k in keywords]):
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    el.click()
                    time.sleep(1.5)
                    if self.wait_for_gz_links(driver, timeout=6) or self.scroll_and_check(driver):
                        return True
                    tried += 1
                except Exception:
                    continue
        return False

    def dump_debug_html(self, driver, tag="no_links"):
        try:
            dbg = (self.download_dir / f"debug_{tag}_{int(time.time())}.html")
            dbg.write_text(driver.page_source, encoding="utf-8")
            logger.info(f"Saved debug HTML: {dbg}")
        except Exception:
            pass

                                                                
                                          
                                                                
    def find_all_download_links(self, driver):
        links = []
        seen = set()
        MAX = 120
        base = driver.current_url

                                                                               
        for sel in [
            "a[href*='.gz']",
            "a[href*='.xml']",
            "a[download]",
            "a[href*='download']",
            "a[href*='Download']",
            ".downloadBtn",
            "a[href*='Price']", "a[href*='price']",
            "a[href*='Promo']", "a[href*='promo']",
            "a[href*='Store']", "a[href*='Stores']", "a[href*='store']", "a[href*='stores']",
            "a[href*='Branch']", "a[href*='Branches']", "a[href*='branch']", "a[href*='branches']",
        ]:
            if len(links) >= MAX:
                break
            try:
                for el in driver.find_elements(By.CSS_SELECTOR, sel)[:100]:
                    href = el.get_attribute("href") or ""
                    if not href:
                        continue
                    href = urljoin(base, href)
                    if href in seen:
                        continue
                    seen.add(href)

                                                                              
                    try:
                        parent = el.find_element(By.XPATH, "./ancestor::tr[1]")
                        parent_text = (parent.text or "").lower()
                    except Exception:
                        parent_text = (el.text or "").lower()

                    filename = href.split("/")[-1].lower()
                                                                                                   
                    if ".gz" not in filename and ".xml" not in filename:
                        m = re.search(r"(\S+\.gz|\S+\.xml)", parent_text)
                        if m:
                            filename = m.group(1).lower()

                    STORE_KWS = ["store", "stores", "branch", "branches", "storesfull", "storefull",
                                 "סניף", "סניפים", "חנויות", "חנות"]
                    combined = (filename + " " + parent_text).lower()

                    if any(k in combined for k in STORE_KWS):
                        ftype = "stores"
                    elif any(kw in combined for kw in ["price", "pricefull", "מחיר"]):
                        ftype = "price"
                    elif any(kw in combined for kw in ["promo", "promofull", "מבצע", "promotion"]):
                        ftype = "promo"
                    else:
                        ftype = "unknown"

                    links.append({
                        "url": href,
                        "text": (el.text or "")[:50],
                        "type": ftype,
                        "filename": filename,
                        "element": el,                             
                    })
            except Exception:
                pass

                                                                 
        if len(links) < 10:
            try:
                for table in driver.find_elements(By.TAG_NAME, "table")[:2]:
                    for row in table.find_elements(By.TAG_NAME, "tr")[:30]:
                        row_text = (row.text or "").lower()
                        for a in row.find_elements(By.TAG_NAME, "a")[:4]:
                            href = a.get_attribute("href") or ""
                            if not href:
                                continue
                            url = urljoin(base, href)
                            if url in seen:
                                continue
                            seen.add(url)
                            name = url.split("/")[-1].lower()

                            STORE_KWS = ["store", "stores", "branch", "branches", "storesfull", "storefull",
                                         "סניף", "סניפים", "חנויות", "חנות"]
                            if any(k in row_text for k in STORE_KWS):
                                ftype = "stores"
                            elif "price" in row_text or "מחיר" in row_text:
                                ftype = "price"
                            elif "promo" in row_text or "מבצע" in row_text:
                                ftype = "promo"
                            else:
                                ftype = "unknown"

                                                                          
                            if ".gz" not in name and ".xml" not in name:
                                m = re.search(r"(\S+\.gz|\S+\.xml)", row_text)
                                if m:
                                    name = m.group(1).lower()

                            links.append({
                                "url": url,
                                "text": (a.text or "")[:50],
                                "type": ftype,
                                "filename": name,
                                "element": a,
                            })
            except Exception:
                pass

                                                                                                       
        try:
            stores_pat = re.compile(r"\bstores(full)?\d{5,}", re.IGNORECASE)
            for a in driver.find_elements(By.TAG_NAME, 'a')[:200]:
                href = a.get_attribute('href') or ''
                txt = (a.text or '').strip()
                if not href:
                    continue
                url = urljoin(base, href)
                if url in seen:
                    continue
                name = url.split('/')[-1]
                low_txt = txt.lower()
                if stores_pat.search(low_txt) or stores_pat.search(name.lower()):
                    seen.add(url)
                    links.append({
                        'url': url,
                        'text': txt[:50],
                        'type': 'stores',
                        'filename': name.lower(),
                        'element': a,
                    })
        except Exception:
            pass

        logger.info(f"Found {len(links)} download links (limited search)")
        return links

    def _is_gzip_magic(self, data: bytes) -> bool:
        return len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B

                                                                          
    def _sniff_ext_for_path(self, p: Path) -> str:
        """Return 'gz' or 'xml' (or 'unknown') by sniffing file contents."""
        try:
            b = p.read_bytes()
            if self._is_gzip_magic(b[:4]):
                return "gz"
            head = b[:4096].decode("utf-8", errors="ignore").lower()
                                   
            if "<!doctype html" in head or "<html" in head:
                return "html"
            if "<" in head:
                return "xml"
            return "unknown"
        except Exception:
            return "unknown"

    def download_via_browser_url(self, driver, url: str, final_filename: str, timeout: int = 60) -> Path | None:
        """Navigate to URL in the browser and attempt to trigger/download a file. Accepts files without extension."""
        before = {p for p in self.download_dir.glob("*")}
        try:
                                                                         
            if (url or "").strip().lower().startswith("javascript:"):
                js = url.strip()[len("javascript:"):]
                try:
                    driver.execute_script(js)
                except Exception as e:
                    logger.error(f"Executing javascript: URL failed: {e}")
                    return None
                time.sleep(1.0)
            else:
                driver.get(url)
                time.sleep(1.0)
                                                        
            try:
                el = WebDriverWait(driver, 6).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR,
                        "a[href$='.gz'], a[href$='.xml'], a[href*='.gz?'], a[href*='.xml?'], a[href*='download'], a[download]"))
                )
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                except Exception:
                    pass
                el.click()
            except Exception:
                                                                                         
                pass
        except Exception as e:
            logger.error(f"Browser navigate failed: {e}")
            return None

        deadline = time.time() + timeout
        latest = None
        while time.time() < deadline:
            after = {p for p in self.download_dir.glob("*")}
                                                        
            new_files = [p for p in (after - before) if not p.name.endswith('.crdownload')]
            if new_files:
                latest = max(new_files, key=lambda p: p.stat().st_mtime)
                break
            time.sleep(0.5)
        if not latest:
            logger.error("Browser download-by-URL timed out or nothing downloaded")
            return None

                                                    
        kind = self._sniff_ext_for_path(latest)
        if final_filename.lower().endswith(".gz") and kind == "xml":
            final_filename = re.sub(r"\.gz$", ".xml", final_filename, flags=re.IGNORECASE)
        elif final_filename.lower().endswith(".xml") and kind == "gz":
            final_filename = re.sub(r"\.xml$", ".gz", final_filename, flags=re.IGNORECASE)
        elif not (final_filename.lower().endswith(".gz") or final_filename.lower().endswith(".xml")):
                                                    
            final_filename += (".gz" if kind == "gz" else ".xml" if kind == "xml" else "")

        target = self.download_dir / final_filename
        try:
            latest.replace(target)
        except Exception:
            import shutil
            shutil.copy2(latest, target)
            try:
                latest.unlink(missing_ok=True)
            except Exception:
                pass

                  
        try:
            if target.suffix.lower() == ".gz":
                                                                                             
                with gzip.open(target, "rb") as gz:
                    head_bytes = gz.read(4096)
                try:
                    head_text = head_bytes.decode('utf-8', errors='ignore').lower()
                except Exception:
                    head_text = ''
                if '<!doctype html' in head_text or '<html' in head_text or 'http 404' in head_text or '404 not found' in head_text:
                    raise OSError('Gzipped HTML/error page received')
            else:
                head = target.read_text(encoding="utf-8", errors="ignore").lower()[:4096]
                                                                            
                if "<!doctype html" in head or "<html" in head:
                    raise OSError("HTML served instead of XML")
                if "<" not in head:
                    raise OSError("XML validation failed")
            return target
        except Exception as e:
            logger.error(f"Browser file validation failed: {e}")
            try:
                target.unlink(missing_ok=True)
            except Exception:
                pass
            return None

    def download_via_browser(self, driver, element, final_filename: str, timeout: int = 60, url: str | None = None) -> Path | None:
        """Click an existing element; if stale/fails, fall back to URL navigation. Accepts files without extension."""
        before = {p for p in self.download_dir.glob("*")}
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
            element.click()
        except StaleElementReferenceException:
            if url and not (url.strip().lower().startswith('javascript:')):
                logger.warning("Element stale; retrying via URL navigation fallback")
                return self.download_via_browser_url(driver, url, final_filename, timeout)
            logger.error("Element stale and no URL provided")
            return None
        except Exception as e:
            logger.error(f"Browser click failed: {e}")
            if url and not (url.strip().lower().startswith('javascript:')):
                return self.download_via_browser_url(driver, url, final_filename, timeout)
            return None

        deadline = time.time() + timeout
        latest = None
        while time.time() < deadline:
            after = {p for p in self.download_dir.glob("*")}
            new_files = [p for p in (after - before) if not p.name.endswith(".crdownload")]
            if new_files:
                latest = max(new_files, key=lambda p: p.stat().st_mtime)
                break
            time.sleep(0.5)

        if not latest or not latest.exists():
            logger.error("Browser download timed out or not found")
            if url and not (url.strip().lower().startswith('javascript:')):
                return self.download_via_browser_url(driver, url, final_filename, timeout)
            return None

                                                           
        kind = self._sniff_ext_for_path(latest)
        if final_filename.lower().endswith(".gz") and kind == "xml":
            final_filename = re.sub(r"\.gz$", ".xml", final_filename, flags=re.IGNORECASE)
        elif final_filename.lower().endswith(".xml") and kind == "gz":
            final_filename = re.sub(r"\.xml$", ".gz", final_filename, flags=re.IGNORECASE)
        elif not (final_filename.lower().endswith(".gz") or final_filename.lower().endswith(".xml")):
            final_filename += (".gz" if kind == "gz" else ".xml" if kind == "xml" else "")

        target = self.download_dir / final_filename
        try:
            latest.replace(target)
        except Exception:
            import shutil
            shutil.copy2(latest, target)
            try:
                latest.unlink(missing_ok=True)
            except Exception:
                pass

                                   
        try:
            if target.suffix.lower() == ".gz":
                                                                                             
                with gzip.open(target, "rb") as gz:
                    head_bytes = gz.read(4096)
                try:
                    head_text = head_bytes.decode('utf-8', errors='ignore').lower()
                except Exception:
                    head_text = ''
                if '<!doctype html' in head_text or '<html' in head_text or 'http 404' in head_text or '404 not found' in head_text:
                    raise OSError('Gzipped HTML/error page received')
            elif target.suffix.lower() == ".xml":
                head = target.read_text(encoding='utf-8', errors='ignore').lower()[:4096]
                                         
                if '<!doctype html' in head or '<html' in head:
                    raise OSError('HTML served instead of XML')
                if '<' not in head:
                    raise OSError('XML validation failed')
            return target
        except Exception as e:
            logger.error(f"File validation failed after browser download: {e}")
            try:
                target.unlink(missing_ok=True)
            except Exception:
                pass
            return None

    def download_file_with_validation(self, driver, link: dict, final_filename: str) -> Path | None:
        """
        Try authenticated requests first; if it fails (TLS/HTML/etc), fallback to browser (element or URL).
        """
        url = link["url"]
        local_path = self.download_dir / final_filename
        logger.info(f"Downloading (validated): {final_filename}")

                                                                                                
        if (url or "").strip().lower().startswith('javascript:'):
            logger.info("Link is javascript: postback; using browser click path")
            el = link.get("element")
            if el:
                return self.download_via_browser(driver, el, final_filename, url=None)
            else:
                                                                      
                return self.download_via_browser_url(driver, url, final_filename)

                                                         
        try:
            try:
                self.session.headers["Referer"] = driver.current_url
            except Exception:
                self.session.headers["Referer"] = url

            resp = self.session.get(url, stream=True, timeout=45, allow_redirects=True)
            ct = resp.headers.get("Content-Type", "") or ""
            logger.info(f"   HTTP {resp.status_code}, Content-Type={ct!r}")
            resp.raise_for_status()

            it = resp.iter_content(chunk_size=8192)
            first = next(it, b"")
            if not first:
                raise RuntimeError("Empty response body")

            with open(local_path, "wb") as f:
                f.write(first)
                for chunk in it:
                    if chunk:
                        f.write(chunk)

                                         
            low_ct = ct.lower()
            fb = first.lstrip()

            def _infer_kind():
                if len(first) >= 2 and first[0] == 0x1F and first[1] == 0x8B:
                    return "gz"
                if "gzip" in low_ct:
                    return "gz"
                if "xml" in low_ct or fb.startswith(b"<?xml") or fb.startswith(b"<"):
                    return "xml"
                if "text/html" in low_ct or fb.startswith(b"<!"):
                    return "html"
                return "unknown"

            kind = _infer_kind()

                                                                             
            if kind == "xml" and local_path.suffix.lower() != ".xml":
                local_path_xml = local_path.with_suffix(".xml")
                try:
                    local_path.rename(local_path_xml)
                    local_path = local_path_xml
                except Exception:
                    pass                                               
            elif kind == "gz" and local_path.suffix.lower() != ".gz":
                local_path_gz = local_path.with_suffix(".gz")
                try:
                    local_path.rename(local_path_gz)
                    local_path = local_path_gz
                except Exception:
                    pass

                              
            if kind == "gz":
                                                                                             
                with gzip.open(local_path, "rb") as gz:
                    head_bytes = gz.read(4096)
                try:
                    head_text = head_bytes.decode('utf-8', errors='ignore').lower()
                except Exception:
                    head_text = ''
                if '<!doctype html' in head_text or '<html' in head_text or 'http 404' in head_text or '404 not found' in head_text:
                    try:
                        local_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    raise RuntimeError('Gzipped HTML/error page received')
            elif kind == "xml":
                head = local_path.read_text(encoding='utf-8', errors='ignore').lower()[:4096]
                                         
                if '<!doctype html' in head or '<html' in head:
                    raise RuntimeError('HTML served instead of XML')
                if '<' not in head:
                    raise RuntimeError('XML validation failed')
            else:
                                                        
                try:
                    local_path.unlink(missing_ok=True)
                except Exception:
                    pass
                raise RuntimeError(f"Non-file content via requests: kind={kind}")

            return local_path

        except Exception as e:
            logger.warning(f"Direct download failed ({e}); trying browser fallback...")

                             
        el = link.get("element")
        if not el:
                                            
            return self.download_via_browser_url(driver, url, final_filename)

        return self.download_via_browser(driver, el, final_filename, url=url)

                                                                
                           
                                                                
    def handle_carrefour_filters(self, driver):
        """Carrefour usually lists newest files; clicking 'Search' can refresh."""
        try:
            buttons = driver.find_elements(By.XPATH, "//button[contains(., 'חיפוש') or contains(., 'Search')]")
            if buttons:
                buttons[0].click()
                logger.info("Carrefour: clicked search")
                time.sleep(2.0)
        except Exception:
            pass

        self.wait_for_gz_links(driver, timeout=10)
        self.scroll_and_check(driver)

    def nudge_carrefour_stores(self, driver):
        """Generic attempts to surface 'Stores' files on a page (works for multiple sites)."""
        try:
            stores_terms = ['stores', 'store', 'storesfull', 'branches', 'חנויות', 'חנות', 'סניפים', 'סניף']
                                                               
            try:
                for sel in driver.find_elements(By.TAG_NAME, 'select'):
                    try:
                        s = Select(sel)
                        for i, opt in enumerate(s.options):
                            txt = (opt.text or '').strip().lower()
                            if any(term in txt for term in stores_terms):
                                s.select_by_index(i)
                                logger.info("Stores nudge: selected 'Stores' in dropdown")
                                time.sleep(1.0)
                                break
                    except Exception:
                        continue
            except Exception:
                pass

                                            
            for css in [
                "input[type='search']",
                "input[placeholder*='Search']",
                "input[name*='search']",
                "#search",
                "input[type='text']",
            ]:
                try:
                    els = driver.find_elements(By.CSS_SELECTOR, css)
                    if els:
                        box = els[0]
                        box.clear()
                        box.send_keys('Stores')
                        try:
                            box.send_keys(Keys.ENTER)
                        except Exception:
                            pass
                        logger.info("Stores nudge: typed 'Stores' into search")
                        time.sleep(1.5)
                        break
                except Exception:
                    continue

                                        
            try:
                buttons = driver.find_elements(By.XPATH, "//button[contains(., 'Search') or contains(., 'חפש') or contains(., 'חיפוש') or contains(., 'סנן')]")
                if buttons:
                    buttons[0].click()
                    logger.info("Stores nudge: clicked search/filter button")
                    time.sleep(2.0)
            except Exception:
                pass
        except Exception:
            pass

                               
        self.wait_for_gz_links(driver, timeout=6)
        self.scroll_and_check(driver)

    def _activate_stores_view(self, driver, link: dict):
        """If a Stores link is a javascript postback or button, activate it so that real .gz/.xml links appear."""
        try:
            el = link.get('element')
            url = (link.get('url') or '').strip()
            if el is not None:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                except Exception:
                    pass
                try:
                    el.click()
                except Exception:
                                               
                    if url.lower().startswith('javascript:'):
                        js = url[len('javascript:'):]
                        driver.execute_script(js)
                    elif url:
                        driver.get(url)
            else:
                if url.lower().startswith('javascript:'):
                    js = url[len('javascript:'):]
                    driver.execute_script(js)
                elif url:
                    driver.get(url)
        except Exception:
            pass

                                                         
        try:
            time.sleep(1.0)
            self.wait_for_gz_links(driver, timeout=6)
            self.scroll_and_check(driver)
        except Exception:
            pass

    def nudge_select_category(self, driver, terms: list[str]):
        """Select a category option containing any of the terms, optionally type in a search box, and click Search."""
        try:
                             
            try:
                for sel in driver.find_elements(By.TAG_NAME, 'select'):
                    try:
                        s = Select(sel)
                                                   
                        found = False
                        for i, opt in enumerate(s.options):
                            txt = (opt.text or '').strip().lower()
                            val = (opt.get_attribute("value") or "").strip().lower()
                            if any(term.lower() in txt or term.lower() in val for term in terms):
                                s.select_by_index(i)
                                found = True
                                logger.info(f"Category nudge: selected '{terms[0]}' in dropdown")
                                time.sleep(1.0)
                                break
                        if found:
                            break
                    except Exception:
                        continue
            except Exception:
                pass

                               
            try:
                for lbl in driver.find_elements(By.XPATH, "//label"):
                    t = (lbl.text or "").strip().lower()
                    if any(term.lower() in t for term in terms):
                        try:
                            lbl.click()
                            logger.info("Category nudge: clicked matching label")
                            time.sleep(0.8)
                            break
                        except Exception:
                            pass
            except Exception:
                pass

                                            
            for css in [
                "input[type='search']",
                "input[placeholder*='Search']",
                "input[name*='search']",
                "#search",
                "input[type='text']",
            ]:
                try:
                    els = driver.find_elements(By.CSS_SELECTOR, css)
                    if els:
                        box = els[0]
                        box.clear()
                        box.send_keys(terms[0])
                        try:
                            box.send_keys(Keys.ENTER)
                        except Exception:
                            pass
                        logger.info(f"Category nudge: typed '{terms[0]}' into search")
                        time.sleep(1.0)
                        break
                except Exception:
                    continue

                                        
            try:
                buttons = driver.find_elements(By.XPATH, "//button[contains(., 'Search') or contains(., 'חפש') or contains(., 'חיפוש') or contains(., 'סנן')]")
                if buttons:
                    buttons[0].click()
                    logger.info("Category nudge: clicked search button")
                    time.sleep(1.5)
            except Exception:
                pass
        except Exception:
            pass
        self.wait_for_gz_links(driver, timeout=6)
        self.scroll_and_check(driver)

    def _click_download_buttons(self, driver, *, button_terms: list[str], out_prefix: str, count: int) -> list[Path]:
        """Find up to `count` visible download buttons/links matching button_terms and click them,
        saving files under downloads/ with name pattern f"{out_prefix}_{self.ts}_b{{i}}.gz".
        Returns list of saved Paths.
        """
        saved: list[Path] = []
        try:
                                                  
            candidates = []
            for sel in ["a,button,[role='button']"]:
                try:
                    for el in driver.find_elements(By.CSS_SELECTOR, sel):
                        txt = (el.text or "").strip()
                        if not txt:
                            continue
                        low = txt.lower()
                        if any(term.lower() in low for term in button_terms):
                            candidates.append(el)
                except Exception:
                    continue
                           
            i = 0
            for el in candidates:
                if i >= count:
                    break
                fname = f"{out_prefix}_{self.ts}_b{i+1}.gz"
                path = self.download_via_browser(driver, el, fname)
                if path:
                    saved.append(path)
                    i += 1
        except Exception:
            pass
        return saved

    def handle_victory_optimized(self, driver):
        """
        Victory sometimes has large tables; try quick .gz anchors first then fallback to generic.
        Properly identify file types as price/promo/stores.
        """
                                                                   
        links = driver.find_elements(By.CSS_SELECTOR, "a[href$='.gz'], a[href$='.xml']")
        if links:
            result = []
            for a in links[:50]:
                href = a.get_attribute("href") or ""
                text = a.text or ""
                filename = href.split("/")[-1].lower()

                                                                          
                try:
                    parent = a.find_element(By.XPATH, "./ancestor::tr[1]")
                    parent_text = parent.text.lower() if parent else ""
                except Exception:
                    parent_text = ""

                combined = (filename + " " + text + " " + parent_text).lower()

                                                       
                if any(kw in combined for kw in ["store", "stores", "branch", "branches", "storesfull", "storefull",
                                                 "סניף", "סניפים", "חנויות", "חנות"]):
                    ftype = "stores"
                elif any(kw in combined for kw in ["price", "pricefull", "מחיר"]):
                    ftype = "price"
                elif any(kw in combined for kw in ["promo", "promofull", "מבצע", "promotion"]):
                    ftype = "promo"
                else:
                                                      
                    if "pricefull" in filename or filename.startswith("price"):
                        ftype = "price"
                    elif "promofull" in filename or filename.startswith("promo"):
                        ftype = "promo"
                    else:
                        ftype = "unknown"

                                                                     
                if filename.endswith('.xml'):
                    ftype = 'stores'

                result.append({
                    "url": urljoin(driver.current_url, href),
                    "text": text[:50],
                    "type": ftype,
                    "filename": filename,
                    "element": a
                })

                                                 
            price_count = sum(1 for l in result if l['type'] == 'price')
            promo_count = sum(1 for l in result if l['type'] == 'promo')
            stores_count = sum(1 for l in result if l['type'] == 'stores')
            unknown_count = sum(1 for l in result if l['type'] == 'unknown')
            logger.info(f"Victory link types: price={price_count}, promo={promo_count}, stores={stores_count}, unknown={unknown_count}")

            return result
        return self.find_all_download_links(driver)

    def handle_hazihinam(self, driver):
        """
        Hazi-Hinam: pick 'חנויות' in 'סוג קובץ', click 'חיפוש', then harvest links.
        Works even when the download link has no .gz/.xml extension.
        """
                                                         
        links_initial = self.find_all_download_links(driver)
        if any(l.get("type") == "stores" for l in links_initial):
            return links_initial

                                             
        try:
                                                                            
            sel = None
            try:
                sel = driver.find_element(
                    By.XPATH, "//label[contains(normalize-space(.),'סוג קובץ')]/following::select[1]"
                )
            except Exception:
                pass

            if sel:
                try:
                    s = Select(sel)
                    chosen = False
                    for i, opt in enumerate(s.options):
                        if "חנויות" in (opt.text or ""):
                            s.select_by_index(i)
                            chosen = True
                            break
                    if not chosen:
                                                                   
                        for i, opt in enumerate(s.options):
                            t = (opt.text or "").lower()
                            if "store" in t:
                                s.select_by_index(i)
                                break
                except Exception:
                    pass
            else:
                                                         
                try:
                    combo = driver.find_element(
                        By.XPATH,
                        "//label[contains(normalize-space(.),'סוג קובץ')]/following::*[@role='combobox' or contains(@class,'select')][1]"
                    )
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", combo)
                    combo.click()
                    opt = WebDriverWait(driver, 6).until(
                        EC.element_to_be_clickable((
                            By.XPATH,
                            "//*[(@role='option' or self::li or self::div) and contains(., 'חנויות')]"
                        ))
                    )
                    opt.click()
                except Exception:
                    pass
            time.sleep(0.8)
        except Exception:
            pass

                                                        
        try:
            btn = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((
                    By.XPATH, "//button[contains(normalize-space(.),'חיפוש') or contains(.,'Search')]"
                ))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            btn.click()
        except Exception:
            for xp in ["//button[contains(., 'חיפוש')]",
                       "//button[contains(., 'Search')]",
                       "//button[contains(., 'סנן') or contains(., 'הצג')]"]:
                try:
                    driver.find_element(By.XPATH, xp).click()
                    break
                except Exception:
                    continue
        time.sleep(1.2)

                                                            
        try:
            WebDriverWait(driver, 12).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "//a[contains(.,'הורדה') or contains(@href,'download') or contains(@href,'.gz') or contains(@href,'.xml')]"
                ))
            )
        except Exception:
            pass

        self.scroll_and_check(driver)

                                                                                                
        stores_links = []
        try:
            download_anchors = driver.find_elements(
                By.XPATH,
                "//a[contains(.,'הורדה') or contains(@href,'download') or contains(@href,'Download') or contains(@href,'.gz') or contains(@href,'.xml')]"
            )
            for a in download_anchors:
                url = a.get_attribute("href") or ""
                                                                     
                try:
                    row = a.find_element(By.XPATH, "./ancestor::tr[1]")
                    row_text = (row.text or "").lower()
                except Exception:
                    row = None
                    row_text = (a.text or "").lower()

                                     
                if not any(tok in row_text for tok in ["חנויות", "store", "stores", "storesfull", "branch", "branches", "סניפ", "חנות"]):
                    continue

                                                                            
                filename_guess = ""
                m = re.search(r"(\S+\.(?:gz|xml))", row_text)
                if m:
                    filename_guess = m.group(1).lower()
                else:
                    filename_guess = (url.split("/")[-1] or "stores").lower()

                stores_links.append({
                    "url": url,
                    "text": (a.text or "")[:50],
                    "type": "stores",
                    "filename": filename_guess,
                    "element": a,                                                          
                })
        except Exception:
            pass

                                                                                          
        links_after = self.find_all_download_links(driver) or []
        merged = list(links_initial)

        seen = {l.get('url') for l in merged if l.get('url')}
        for l in stores_links + links_after:
            u = l.get('url')
            if u and u not in seen:
                seen.add(u)
                merged.append(l)

        return merged

                                                                
                   
                                                                

                                                                   
    def _extract_timestamp_from_filename(self, fname: str):
        """
        Files end with -YYYYMMDDHHMM(.gz) or sometimes -YYYYMMDDHHMMSS(.gz).
        Return a datetime for sorting; None if not found.
        """
        if not fname:
            return None
        fname = fname.lower()
        m = re.search(r'(\d{14}|\d{12})(?=\.gz$)', fname)
        if not m:
            return None
        s = m.group(1)
        try:
            if len(s) == 14:
                return datetime.strptime(s, "%Y%m%d%H%M%S")
            return datetime.strptime(s, "%Y%m%d%H%M")
        except Exception:
            return None

    def _derive_stores_link_from_samples(self, links: list[dict]) -> str | None:
        """Heuristic: derive a Stores link by transforming a known Price/Promo URL."""
        for l in links:
            url = l.get("url") or ""
            if not url:
                continue
            cand = (
                url
                .replace("PriceFull", "StoresFull")
                .replace("pricefull", "storesfull")
                .replace("Price", "Stores")
                .replace("price", "stores")
                .replace("PromoFull", "StoresFull")
                .replace("promofull", "storesfull")
                .replace("Promo", "Stores")
                .replace("promo", "stores")
            )
            if cand != url:
                return cand
        return None

    def _probe_stores_url(self, url: str) -> bool:
        """Return True if URL seems to be a valid stores file (gzip or xml)."""
        try:
            r = self.session.get(url, timeout=15, allow_redirects=True)
            ct = (r.headers.get("Content-Type") or "").lower()
            if r.status_code != 200 or not r.content:
                return False
            data = r.content
            if self._is_gzip_magic(data[:4]) or "gzip" in ct:
                return True
            head = data[:64].decode("utf-8", errors="ignore")
            return ("xml" in ct) or ("<" in head)
        except Exception:
            return False

    def organize_and_download_files(self, driver, download_links, supermarket_name):
        """
        Download up to max_br price + promo files, validate, and return file infos.
        """
        downloaded = []

        price_files   = [l for l in download_links if l['type'] == 'price']
        promo_files   = [l for l in download_links if l['type'] == 'promo']
        stores_files  = [l for l in download_links if l['type'] == 'stores']
        unknown_files = [l for l in download_links if l['type'] == 'unknown']

                                                                     
        def _sort_newest_first(files):
            return sorted(
                files,
                key=lambda l: (self._extract_timestamp_from_filename(l.get('filename', '')) or datetime.min),
                reverse=True
            )

        price_files = _sort_newest_first(price_files)
        promo_files = _sort_newest_first(promo_files)

        logger.info(f"File type breakdown for {supermarket_name}:")
        logger.info(f"  - Price files found: {len(price_files)}")
        logger.info(f"  - Promo files found: {len(promo_files)}")
        logger.info(f"  - Stores files found: {len(stores_files)}")
        logger.info(f"  - Unknown files found: {len(unknown_files)}")

        max_br = self.config.get('max_branches', 2)
                                                                                                
                                                                                  
        store_names_by_index = []

                                                                  
        for i, link in enumerate(price_files[:max_br], start=1):
            branch_num = i
            branch = f"branch_{branch_num}"
            filename   = f"pricesFull_{self.ts}_b{branch_num}.gz"
            path = self.download_file_with_validation(driver, link, filename)
            if path:
                downloaded.append({"path": path, "branch": branch, "type": "pricesFull"})

                                                                  
        for i, link in enumerate(promo_files[:max_br], start=1):
            branch_num = i
            branch     = f"branch_{branch_num}"
            filename   = f"promoFull_{self.ts}_b{branch_num}.gz"
            path = self.download_file_with_validation(driver, link, filename)
            if path:
                downloaded.append({"path": path, "branch": branch, "type": "promoFull"})

                                                                                                
        try:
            if supermarket_name == 'shufersal':
                need_price = max_br - sum(1 for d in downloaded if d['type'] == 'pricesFull')
                need_promo = max_br - sum(1 for d in downloaded if d['type'] == 'promoFull')
                if need_price > 0:
                    self.nudge_select_category(driver, ["PriceFull", "Price", "מחיר"])                  
                    extra = self._click_download_buttons(driver, button_terms=["להורדה", "הורדה", "Download"], out_prefix="pricesFull", count=need_price)
                    for idx, p in enumerate(extra, start=1):
                        if store_names_by_index and idx <= len(store_names_by_index):
                            branch_slug = self._slugify(store_names_by_index[idx-1])
                        else:
                            branch_slug = f"branch_{idx}"
                        downloaded.append({"path": p, "branch": branch_slug, "type": "pricesFull"})
                if need_promo > 0:
                    self.nudge_select_category(driver, ["PromoFull", "Promo", "מבצע"])                  
                    extra = self._click_download_buttons(driver, button_terms=["להורדה", "הורדה", "Download"], out_prefix="promoFull", count=need_promo)
                    for idx, p in enumerate(extra, start=1):
                        if store_names_by_index and idx <= len(store_names_by_index):
                            branch_slug = self._slugify(store_names_by_index[idx-1])
                        else:
                            branch_slug = f"branch_{idx}"
                        downloaded.append({"path": p, "branch": branch_slug, "type": "promoFull"})
        except Exception:
            pass

                                             
                                                                                
        if not stores_files:
            try:
                xml_candidates = [l for l in download_links if (l.get('filename') or '').lower().endswith('.xml')]
                if xml_candidates:
                                                               
                    stores_files = sorted(
                        xml_candidates,
                        key=lambda l: (self._extract_timestamp_from_filename(l.get('filename', '')) or datetime.min),
                        reverse=True
                    )[:1]
            except Exception:
                pass

        if stores_files:
                                                                          
            def _is_real_file(l):
                url = (l.get('url') or '').lower()
                fn = (l.get('filename') or '').lower()
                return (fn.endswith('.gz') or fn.endswith('.xml') or '.gz' in url or '.xml' in url)
            preferred = [l for l in stores_files if _is_real_file(l)]
            chosen_pool = preferred if preferred else stores_files
            s_link = _sort_newest_first(chosen_pool)[0]

                                                                                                                          
            url_l = (s_link.get('url') or '').strip().lower()
            if not _is_real_file(s_link) or url_l.startswith('javascript:'):
                try:
                    self._activate_stores_view(driver, s_link)
                                                                                                  
                    refreshed = self.find_all_download_links(driver)
                    stores_ref = [l for l in refreshed if l.get('type') == 'stores']
                    preferred = [l for l in stores_ref if _is_real_file(l)] or stores_ref
                                                                                          
                    if not preferred:
                        preferred = [l for l in refreshed if (l.get('filename','').lower().endswith('.xml') or '.xml' in (l.get('url','').lower()))]
                    if preferred:
                        s_link = _sort_newest_first(preferred)[0]
                                                                                           
                    if not _is_real_file(s_link):
                        cand = self._derive_stores_link_from_samples(price_files + promo_files)
                        if cand and self._probe_stores_url(cand):
                            s_link = {"url": cand, "filename": cand.split("/")[-1]}
                except Exception:
                    pass
            fname = (s_link.get('filename') or '').lower()
            if fname.endswith('.xml'):
                s_filename = f"storesFull_{self.ts}.xml"
            elif fname.endswith('.gz'):
                s_filename = f"storesFull_{self.ts}.gz"
            else:
                                                                                               
                s_filename = f"storesFull_{self.ts}.gz"
            s_branch = "stores"
            s_path = self.download_file_with_validation(driver, s_link, s_filename)
            if s_path:
                downloaded.append({"path": s_path, "branch": s_branch, "type": "storesFull"})
                                                           
                try:
                                                        
                    raw_b = (gzip.decompress(open(s_path, 'rb').read()) if str(s_path).lower().endswith('.gz') else open(s_path, 'rb').read())
                    records = self._parse_stores_xml(raw_b)
                    entries = []
                    if records:
                        for r in records:
                            entries.append({
                                'name': (r.get('name') or '').strip(),
                                'address': (r.get('address') or '').strip() or None,
                                'city': (r.get('city') or '').strip() or None,
                            })
                    else:
                        logger.warning(f"Stores parse produced 0 records for {supermarket_name}; uploading empty mapping")
                    self._upload_branch_map_json(supermarket_name, entries)
                    self._upload_combined_branch_map(supermarket_name, entries)
                                                    
                    if not entries:
                        self._upload_stores_debug(supermarket_name, s_link.get('url') or s_path.name, raw_b, 0)
                except Exception as e:
                    logger.warning(f"Stores parse/upload failed: {e}")
        else:
                                                                
            cand = self._derive_stores_link_from_samples(price_files + promo_files)
            if cand and self._probe_stores_url(cand):
                logger.info(f"Derived Stores link candidate: {cand}")
                                                                                         
                s_link = {"url": cand, "filename": cand.split("/")[-1]}
                fname = s_link["filename"].lower()
                if fname.endswith('.xml'):
                    s_filename = f"storesFull_{self.ts}.xml"
                else:
                    s_filename = f"storesFull_{self.ts}.gz"
                s_branch = "stores"
                s_path = self.download_file_with_validation(driver, s_link, s_filename)
                if s_path:
                    downloaded.append({"path": s_path, "branch": s_branch, "type": "storesFull"})
                    try:
                        raw_b = (gzip.decompress(open(s_path, 'rb').read()) if str(s_path).lower().endswith('.gz') else open(s_path, 'rb').read())
                        records = self._parse_stores_xml(raw_b)
                        entries = []
                        if records:
                            entries = [{
                                'name': (r.get('name') or '').strip(),
                                'address': (r.get('address') or '').strip() or None,
                                'city': (r.get('city') or '').strip() or None,
                            } for r in records]
                        else:
                            logger.warning(f"Stores parse produced 0 records (derived) for {supermarket_name}; uploading empty mapping")
                        self._upload_branch_map_json(supermarket_name, entries)
                        self._upload_combined_branch_map(supermarket_name, entries)
                        if not entries:
                            self._upload_stores_debug(supermarket_name, s_link.get('url') or s_path.name, raw_b, 0)
                    except Exception as e:
                        logger.warning(f"Stores parse/upload failed: {e}")

                                                                                            
        if len(downloaded) < max_br * 2 and unknown_files:
            from collections import defaultdict
            per_branch_type_count = defaultdict(int)

            unknown_files = _sort_newest_first(unknown_files)

            price_downloaded = sum(1 for d in downloaded if d['type'] == 'pricesFull')
            promo_downloaded = sum(1 for d in downloaded if d['type'] == 'promoFull')

            for link in unknown_files[:max_br * 2]:
                fname = (link.get("filename") or "").lower()

                if "promo" in fname and promo_downloaded < max_br:
                    inferred_type = "promoFull"
                    branch_num = promo_downloaded + 1
                    promo_downloaded += 1
                elif price_downloaded < max_br:
                    inferred_type = "pricesFull"
                    branch_num = price_downloaded + 1
                    price_downloaded += 1
                elif promo_downloaded < max_br:
                    inferred_type = "promoFull"
                    branch_num = promo_downloaded + 1
                    promo_downloaded += 1
                else:
                    continue                        

                branch = f"branch_{branch_num}"
                per_branch_type_count[(inferred_type, branch_num)] += 1
                seq = per_branch_type_count[(inferred_type, branch_num)]
                filename = f"{inferred_type}_{self.ts}_b{branch_num}_{seq}.gz"
                path = self.download_file_with_validation(driver, link, filename)
                if path:
                    downloaded.append({"path": path, "branch": branch, "type": inferred_type})

        return downloaded

    def upload_to_s3(self, file_info, supermarket_name):
        """
        Upload following the exact pattern expected by the extractor:
        providers/<provider>/<branch>/<file>
        """
        file_path: Path = file_info["path"]
        branch = file_info.get("branch", "branch_1")
        key = f"providers/{supermarket_name}/{branch}/{file_path.name}"
        if self.local_mode:
                                                        
            dest = Path(self.bucket) / key.replace("/", os.sep)
            dest.parent.mkdir(parents=True, exist_ok=True)
            try:
                                                                                   
                size = file_path.stat().st_size if file_path.exists() else 0
                if size <= 0:
                    logger.error(f"File is empty; skipping save: {file_path.name}")
                    return False
                data = file_path.read_bytes()
                dest.write_bytes(data)
                logger.info(f"Saved locally: {dest}")
                return True
            except Exception as e:
                logger.error(f"Failed to save locally: {e}")
                return False
            finally:
                try:
                    file_path.unlink(missing_ok=True)
                except Exception:
                    pass
        else:
            try:
                                                                                   
                size = file_path.stat().st_size if file_path.exists() else 0
                if size <= 0:
                    logger.error(f"File is empty; skipping upload: {file_path.name}")
                    return False
                with open(file_path, "rb") as f:
                    ct = (
                        "application/gzip" if file_path.suffix.lower() == ".gz" else
                        ("application/xml" if file_path.suffix.lower() == ".xml" else "application/octet-stream")
                    )
                    self.s3.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=f.read(),
                        ContentType=ct,
                    )
                logger.info(f"Uploaded to s3://{self.bucket}/{key}")
                return True
            except Exception as e:
                logger.error(f"Failed to upload to S3: {e}")
                return False
            finally:
                                                          
                try:
                    file_path.unlink(missing_ok=True)
                except Exception:
                    pass

    def crawl_supermarket(self, driver, supermarket):
        name = supermarket["name"]
        display = supermarket.get("display_name", name)
        url = supermarket["url"]

        logger.info("\n" + "=" * 60)
        logger.info(f"Processing: {display}")
        logger.info(f"URL: {url}")

                                                           
        try:
            driver.delete_all_cookies()
        except Exception:
            pass

        site_type = self.detect_site_type(driver, url)

        if site_type == "login" or "username" in supermarket:
            self.handle_login(driver, supermarket)

                                                   
        got_links = self.wait_for_gz_links(driver, timeout=20)
        if not got_links:
            got_links = self.scroll_and_check(driver)
        if not got_links:
            got_links = self.click_nav_candidates_to_find_links(driver)
        if not got_links:
            self.dump_debug_html(driver, tag=f"{name}_no_links")
            logger.warning(f"No download links detected for {name} at {url}. Dumped HTML for debugging.")

                              
        if name == "victory":
                                    
            links_main = self.handle_victory_optimized(driver)
                                                                                                      
            if not any(l.get('type') == 'stores' for l in (links_main or [])):
                self.nudge_select_category(driver, [
                    "StoresFull", "Stores", "store", "branches",
                    "Storesfull",                 
                    "סניפים", "סניף", "חנויות", "חנות"
                ])
                links_stores = self.find_all_download_links(driver)
                seen = {l.get('url') for l in (links_main or []) if l.get('url')}
                links = list(links_main or [])
                for l in (links_stores or []):
                    u = l.get('url')
                    if u and u not in seen:
                        seen.add(u)
                        links.append(l)
            else:
                links = links_main
        elif name == "carrefour":
            self.handle_carrefour_filters(driver)
            links_main = self.find_all_download_links(driver)
            self.nudge_carrefour_stores(driver)
            links_stores = self.find_all_download_links(driver)
            seen = {l.get('url') for l in (links_main or []) if l.get('url')}
            links = list(links_main or [])
            for l in (links_stores or []):
                u = l.get('url')
                if u and u not in seen:
                    seen.add(u)
                    links.append(l)
        elif name == "shufersal":
            links_main = self.find_all_download_links(driver)
            self.nudge_carrefour_stores(driver)
            links_stores = self.find_all_download_links(driver)
            seen = {l.get('url') for l in (links_main or []) if l.get('url')}
            links = list(links_main or [])
            for l in (links_stores or []):
                u = l.get('url')
                if u and u not in seen:
                    seen.add(u)
                    links.append(l)
        elif name == "hazi-hinam":
            links = self.handle_hazihinam(driver)
        elif name == "super-pharm":
                                                      
            links_main = self.find_all_download_links(driver)
            self.nudge_select_category(driver, ["PriceFull", "Price", "מחיר"])
            links_price = self.find_all_download_links(driver)
            self.nudge_select_category(driver, ["PromoFull", "Promo", "מבצע"])
            links_promo = self.find_all_download_links(driver)
            self.nudge_select_category(driver, ["StoresFull", "Stores", "סניפים", "חנויות"])
            self.nudge_carrefour_stores(driver)
            links_stores = self.find_all_download_links(driver)
            seen = {l.get('url') for l in (links_main or []) if l.get('url')}
            links = list(links_main or [])
            for group in (links_price, links_promo, links_stores):
                for l in (group or []):
                    u = l.get('url')
                    if u and u not in seen:
                        seen.add(u)
                        links.append(l)
        else:
            links = self.find_all_download_links(driver)

                                                                                    
        if not any(l.get('type') == 'stores' for l in links):
            stores_url = supermarket.get('stores_url') if isinstance(supermarket, dict) else None
            if stores_url:
                try:
                    logger.info(f"Navigating to configured stores_url: {stores_url}")
                    driver.get(stores_url)
                    self.wait_for_gz_links(driver, timeout=10)
                    more_links = self.find_all_download_links(driver)
                    if more_links:
                        existing = {l.get('url') for l in links}
                        links.extend([l for l in more_links if l.get('url') not in existing])
                except Exception as e:
                    logger.warning(f"stores_url navigation failed: {e}")

        if not links:
            self.dump_debug_html(driver, tag=f"{name}_post_find_all")
            logger.info(f"Successfully uploaded 0 files for {display}")
            return 0

        downloaded = self.organize_and_download_files(driver, links, name)

        uploaded = 0
        for info in downloaded:
            if self.upload_to_s3(info, name):
                uploaded += 1

        logger.info(f"Successfully uploaded {uploaded} files for {display}")
        return uploaded

    def run(self, supermarket_filter=None):
        logger.info("Starting Universal Supermarket Crawler")
        logger.info(f"S3 bucket: {self.bucket}")

        supermarkets = self.config["supermarkets"]
        logger.info(f"Available supermarkets in config: {[s['name'] for s in supermarkets]}")

        if supermarket_filter:
            logger.info(f"Filter requested for: {supermarket_filter}")
            supermarkets = [s for s in supermarkets if s["name"] in supermarket_filter]
            if not supermarkets:
                logger.warning(f"No supermarkets match filter: {supermarket_filter}")
                return

        driver = self.setup_driver()
        results = {}
        try:
            for s in supermarkets:
                uploaded = self.crawl_supermarket(driver, s)
                results[s["name"]] = uploaded
                time.sleep(1)
        finally:
            try:
                driver.quit()
            except Exception:
                pass
                       
            for f in self.download_dir.glob("*"):
                try:
                    f.unlink()
                except Exception:
                    pass

            logger.info("\n" + "=" * 60)
            logger.info("CRAWLER SUMMARY")
            logger.info("=" * 60)
            total = 0
            for name, count in results.items():
                logger.info(f"{name}: {count} files")
                total += count
            logger.info(f"\nTotal files uploaded: {total}")
            logger.info(f"S3 bucket: {self.bucket}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python crawler.py <s3-bucket> [--config config.json] [--only name1,name2]")
        sys.exit(1)
    bucket = sys.argv[1]
    config_file = None
    only = None
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--config" and i + 1 < len(sys.argv):
            config_file = sys.argv[i + 1]
            i += 1
        elif sys.argv[i] == "--only" and i + 1 < len(sys.argv):
            only = sys.argv[i + 1].split(",")
            i += 1
        i += 1

    crawler = UniversalSupermarketCrawler(bucket, config_file)
    crawler.run(supermarket_filter=only)


if __name__ == "__main__":
    main()

