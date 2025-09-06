import json, os, re, time
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException

from managers.driver_manager import DriverManager
from managers.file_manager import FileManager
from managers.s3_manager import S3Manager
from utils.date import parse_date
from utils.branch_utils import branch_id



class Crawler:
    def __init__(self):
        # managers
        self.driver_manager = DriverManager()
        self.driver = self.driver_manager.get_chromedriver()
        self.file_manager = FileManager()
        self.s3 = S3Manager()

        self._req_sess = None
        with open("config.json", "r", encoding="utf-8") as file:
            self.config = json.load(file)

    def run(self):
        """Fetch and process data from the target source."""
        for superMarket in self.config[os.getenv("CONFIG_KEY", "superMarkets")]:
            print(f"Crawling superMarket: {superMarket['name']}")
            try:
                soup = self.driver_manager.get_html_parser(superMarket["url"])

                # Optional login
                if superMarket.get("username") and superMarket["username"] != "none":
                    username_input = self.driver.find_element(By.NAME, "username")
                    username_input.send_keys(superMarket["username"])
                
                    button = self.driver.find_element("id", "login-button")
                    button.click()
                    time.sleep(2)
                    soup = BeautifulSoup(self.driver.page_source, "html.parser")

                branch_payloads = self.extract_data_multi(soup, superMarket, max_branches=5)
                if not branch_payloads:
                    print(f"No complete (price+promo) branches for {superMarket['name']}")
                    continue

                # Init session once per supermarket page
                if self._req_sess is None:
                    self._req_sess = self.driver_manager.build_session()
                self.driver_manager.sync_cookies(self._req_sess, url=self.driver.current_url)

                for bp in branch_payloads:
                    self.save_files_for_branch(bp, superMarket)
                    time.sleep(2)  
                time.sleep(8) 
            except Exception as e:
                print(f"Error crawling {superMarket['name']}: {e}")

    def extract_data_multi(self, soup, superMarket, max_branches):
        if not soup:
            return []

        table_body = soup.select_one(superMarket["table"])
        if not table_body:
            return []

        rows = table_body.select(superMarket["table-row"])

        latest_price_by_branch = {}  
        latest_promo_by_branch = {}  

        for row in rows:
            name_el = row.select_one(superMarket["name-row"])
            if not name_el:
                continue
            fname = name_el.get_text(strip=True)

            # Price rows
            if fname.startswith(superMarket["file-price-name"]):
                branch, dt = self._row_branch_and_dt(row, superMarket)
                if not branch or not dt:
                    continue
                cur = latest_price_by_branch.get(branch)
                if (cur is None) or (dt > cur[1]):
                    latest_price_by_branch[branch] = (row, dt)

            # Promo rows
            elif fname.startswith(superMarket["file-promo-name"]):
                branch, dt = self._row_branch_and_dt(row, superMarket)
                if not branch or not dt:
                    continue
                cur = latest_promo_by_branch.get(branch)
                if (cur is None) or (dt > cur[1]):
                    latest_promo_by_branch[branch] = (row, dt)

        # Branches that have both
        candidate_branches = set(latest_price_by_branch.keys()) & set(latest_promo_by_branch.keys())
        if not candidate_branches:
            return []

        # Rank by recency 
        ranked = []
        for b in candidate_branches:
            p_row, p_dt = latest_price_by_branch[b]
            r_row, r_dt = latest_promo_by_branch[b]
            recency_key = max(p_dt, r_dt)
            ranked.append((recency_key, b, p_row, p_dt, r_row, r_dt))

        ranked.sort(key=lambda x: x[0], reverse=True)

        result = []
        for _, b, p_row, p_dt, r_row, r_dt in ranked[:max_branches]:
            result.append({
                "branch": b,
                "price": p_row,
                "price_dt": p_dt,
                "promo": r_row,
                "promo_dt": r_dt
            })
        return result

    def _row_branch_and_dt(self, row, superMarket):
        branch = self.get_branch(row, superMarket)
        dt_el = row.select_one(superMarket["date"])
        dt = parse_date(dt_el.text.strip()) if dt_el else None
        return branch, dt

    def get_branch(self, row, superMarket):
        sel = superMarket.get("branch")
        if sel and sel != "none":
            el = row.select_one(sel)
            if el:
                val = el.get_text(strip=True)
                if val:
                    return val

        raw = ""
        name_sel = superMarket.get("name-row")
        if name_sel:
            name_el = row.select_one(name_sel)
            if name_el:
                raw = name_el.get_text(strip=True)

        base = os.path.basename(raw)

        m = re.search(r'-(\d+)-\d{12}\.', base)
        if m:
            return m.group(1)

        parts = base.split('-')
        if len(parts) >= 3 and parts[1].isdigit():
            return parts[1]

        return None


    def save_files_for_branch(self, bp, superMarket):
        """
        bp = {"branch": ..., "price": <tr>, "promo": <tr>, "price_dt": dt, "promo_dt": dt}
        """
        price_row = bp["price"]
        promo_row = bp["promo"]
        branch = bp["branch"]

        if not price_row or not promo_row:
            print(f"Skipping branch {branch}: missing price/promo row")
            return

        # Resolve Selenium elements by ID
        price_row_id = price_row.get("id")
        promo_row_id = promo_row.get("id")
        if not price_row_id or not promo_row_id:
            print(f"Branch {branch}: Missing row IDs; skipping")
            return

        sel_price_row = self.driver.find_element(By.ID, price_row_id)
        sel_promo_row = self.driver.find_element(By.ID, promo_row_id)

        more_info_selector = superMarket.get("more-info")
        if more_info_selector and more_info_selector != "none":
            print(f"[{superMarket['name']}] Opening details for branch {branch}")
            try:
                sel_promo_row.find_element(By.CSS_SELECTOR, more_info_selector).click()
            except Exception:
                pass
            try:
                sel_price_row.find_element(By.CSS_SELECTOR, more_info_selector).click()
            except Exception:
                pass
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, superMarket["download-button"]))
            )

        price_ts = bp["price_dt"].strftime("%Y%m%d_%H%M%S")
        promo_ts = bp["promo_dt"].strftime("%Y%m%d_%H%M%S")

        superMarket_name = superMarket.get("name", "default")
        branch_fs = branch if branch.isdigit() else branch_id(branch)

        # Re-sync cookies 
        self.driver_manager.sync_cookies(self._req_sess, url=self.driver.current_url)

        download_buttons = []
        for row_el in (sel_price_row, sel_promo_row):
            btns = row_el.find_elements(By.CSS_SELECTOR, superMarket["download-button"])
            if not btns:
                try:
                    details_row = row_el.find_element(By.XPATH, "following-sibling::tr[1]")
                    btns = details_row.find_elements(By.CSS_SELECTOR, superMarket["download-button"])
                except NoSuchElementException:
                    btns = []
            download_buttons.extend(btns)

        for btn in download_buttons:
            onclick = btn.get_attribute("onclick") or ""
            m = re.search(r"Download\('([^']+)'\)", onclick)
            raw = m.group(1) if m else (btn.get_attribute("href") or "")
            if not raw:
                continue

            raw_link = urljoin(self.driver.current_url, f"/Download/{raw.lstrip('/')}") if m \
                    else urljoin(self.driver.current_url, raw)
            base_lower = os.path.basename(urlparse(raw_link).path).lower()
            ext = os.path.splitext(base_lower)[1] or ""

            if base_lower.startswith("price"):
                prefix, ts = "price", price_ts
            elif base_lower.startswith("promo"):
                prefix, ts = "promo", promo_ts
            else:
                prefix, ts = "file", price_ts

            filename = f"{prefix}_{ts}{ext}"

            out_path = self.file_manager.download_to_branch(
                raw_link,
                superMarket=superMarket_name,
                branch=branch_fs,
                filename=filename,
                session=self._req_sess,
                verify_cert=False if "publishedprices.co.il" in raw_link else True,
                referer=self.driver.current_url,
            )

            if not out_path and m and "/" not in raw:
                alt_link = urljoin(self.driver.current_url, f"/file/d/{raw}")
                print(f"Retrying via {alt_link} …")
                out_path = self.file_manager.download_to_branch(
                    alt_link,
                    superMarket=superMarket_name,
                    branch=branch_fs,
                    filename=filename,
                    session=self._req_sess,
                    verify_cert=False if "publishedprices.co.il" in alt_link else True,
                    referer=self.driver.current_url,
                )

            if not out_path:
                print(f"{filename} not available, skipping.")
                continue

            # Upload to S3 
            s3_key = f"{superMarket_name}/{branch_fs}/{filename}".replace("\\", "/")
            self.s3.upload_file_from_path(out_path, s3_key)
