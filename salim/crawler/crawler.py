import json
import os
import re
import time
import traceback
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.common.exceptions import NoSuchElementException, TimeoutException

from utils.browser_utils import (
    get_chromedriver,
    get_html_parser,
    session_from_driver,
    sanitize_path_component,
)
from utils.time_date_utils import parse_date
from utils import download_file_from_link
from s3.upload_to_s3 import upload_file_to_s3
from consts import TOP_K_BRANCHES

class Crawler:
    def __init__(self):
        self.driver = get_chromedriver()
        with open("config.json", "r", encoding="utf-8") as file:
            self.config = json.load(file)
        self._req_sess = None

    def crawl(self):
        for provider in self.config["providers"]:
            self._req_sess = None
            print(f"Crawling provider: {provider['name']}")

            try:
                _ = get_html_parser(self.driver, provider["url"])

                if provider.get("username") and provider["username"] != "none":
                    username_input = self.driver.find_element(By.NAME, "username")
                    username_input.send_keys(provider["username"])
                    if provider.get("password") and provider["password"] != "none":
                        password_input = self.driver.find_element(By.NAME, "password")
                        password_input.send_keys(provider["password"])
                    button = self.driver.find_element("id", provider.get("login-button-id", "login-button"))
                    button.click()
                    time.sleep(2)

                time.sleep(1)
                self._req_sess = session_from_driver(self.driver)

                selections = self.extract_top_k(provider, k=TOP_K_BRANCHES)

                if not selections:
                    print(f"No data found for provider {provider['name']}")
                    continue

                for data in selections:
                    saved_files, branch_dir = self.save_file(data, provider)
                    if saved_files:
                        self.upload_file(saved_files, branch_dir, provider)

            except Exception as e:
                print(f"Error crawling {provider['name']}: {type(e).__name__}: {e}")
                traceback.print_exc()

    def switch_mode_and_wait(self, provider, mode_value):
        if not mode_value or mode_value == "none":
            return
        if not (provider.get("options-select-selector") and provider["options-select-selector"] != "none"):
            return

        select_el = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, provider["options-select-selector"]))
        )
        sel = Select(select_el)

        old_body = None
        try:
            old_body = self.driver.find_element(By.CSS_SELECTOR, provider["table-body-selector"])
        except Exception:
            pass

        sel.select_by_value(mode_value)
        try:
            self.driver.execute_script(
                "arguments[0].dispatchEvent(new Event('change', {bubbles:true}))", select_el
            )
        except Exception:
            pass

        if old_body is not None:
            try:
                WebDriverWait(self.driver, 15).until(EC.staleness_of(old_body))
            except Exception:
                pass

        time.sleep(0.3)

    def get_table_body_and_rows(self, provider, wait_timeout=20):
        css = provider.get("table-body-selector", "tbody")

        try:
            WebDriverWait(self.driver, wait_timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css))
            )
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            tb = soup.select_one(css)
            if tb:
                rows = tb.select(provider.get("table-row", "tr")) or tb.select("tr")
                return tb, rows, soup
            raise TimeoutException(f"tbody '{css}' not found in parsed HTML")
        except TimeoutException:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            candidates = soup.select("table tbody")
            if not candidates:
                raise
            best = max(candidates, key=lambda el: len(el.select("tr")))
            rows = best.select(provider.get("table-row", "tr")) or best.select("tr")
            print(f"[fallback] using largest <tbody> with {len(rows)} rows for {provider['name']}")
            return best, rows, soup

    def extract_top_k(self, provider, k=5):
        if provider.get("options-select-selector") and provider["options-select-selector"] != "none":
            if provider.get("option-price") and provider["option-price"] != "none":
                self.switch_mode_and_wait(provider, provider["option-price"])

        _, rows, _ = self.get_table_body_and_rows(provider)
        print(f"[debug] {provider['name']} price mode rows: {len(rows)}")

        latest_price_by_branch = {}
        for row in rows:
            name_el = row.select_one(provider["name-selector"])
            if not name_el:
                continue
            col_text = name_el.get_text(strip=True)
            if not col_text.startswith(provider["file-price-name"]):
                continue

            date_el = row.select_one(provider["date-selector"])
            if not date_el:
                continue
            dt = parse_date(date_el.text.strip())
            if not dt:
                continue

            branch = self.get_branch(row, provider)
            if not branch:
                continue

            prev = latest_price_by_branch.get(branch)
            if (prev is None) or (dt > prev["dt"]):
                latest_price_by_branch[branch] = {"row": row, "dt": dt}

        if not latest_price_by_branch:
            return []

        ordered = sorted(
            latest_price_by_branch.items(),
            key=lambda kv: kv[1]["dt"],
            reverse=True
        )[:k]
        selected_branches = [b for b, _ in ordered]

        latest_promo_by_branch = {}
        if provider.get("options-select-selector") and provider["options-select-selector"] != "none":
            if provider.get("option-promo") and provider["option-promo"] != "none":
                self.switch_mode_and_wait(provider, provider["option-promo"])
                _, promo_rows, _ = self.get_table_body_and_rows(provider)
            else:
                promo_rows = rows
        else:
            promo_rows = rows

        print(f"[debug] {provider['name']} promo mode rows: {len(promo_rows)}")

        for row in promo_rows:
            name_el = row.select_one(provider["name-selector"])
            if not name_el:
                continue
            col_text = name_el.get_text(strip=True)
            if not col_text.startswith(provider["file-promo-name"]):
                continue

            branch = self.get_branch(row, provider)
            if branch not in selected_branches:
                continue

            date_el = row.select_one(provider["date-selector"])
            if not date_el:
                continue
            dt = parse_date(date_el.text.strip())
            if not dt:
                continue

            prev = latest_promo_by_branch.get(branch)
            if (prev is None) or (dt > prev["dt"]):
                latest_promo_by_branch[branch] = {"row": row, "dt": dt}

        result = []
        for branch, pdata in ordered:
            price_row = pdata["row"]
            promo_row = latest_promo_by_branch.get(branch, {}).get("row")
            result.append({"price": price_row, "promo": promo_row, "branch": branch})
        return result

    def get_branch(self, row, provider):
        sel = provider.get("branch-selector")
        if sel and sel != "none":
            el = row.select_one(sel)
            if el:
                val = el.get_text(strip=True)
                if val:
                    return val

        raw = ""
        name_sel = provider.get("name-selector")
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

    def refind_row_in_mode(self, provider, kind, row_id, wait_secs=12):
        if kind == "price":
            self.switch_mode_and_wait(provider, provider.get("option-price"))
        else:
            self.switch_mode_and_wait(provider, provider.get("option-promo"))

        if not row_id:
            return None
        try:
            return WebDriverWait(self.driver, wait_secs).until(
                EC.presence_of_element_located((By.ID, row_id))
            )
        except TimeoutException:
            return None

    def save_file(self, data, provider):
        if not data["price"]:
            print(f"No price row for provider {provider['name']}")
            return [], None

        price_row_id = data["price"].get("id")
        promo_row_id = data["promo"].get("id") if data["promo"] else None

        price_dt = parse_date(data["price"].select_one(provider["date-selector"]).text.strip())
        price_ts = price_dt.strftime("%Y%m%d_%H%M%S") if price_dt else time.strftime("%Y%m%d_%H%M%S")

        promo_ts = None
        if data["promo"]:
            promo_dt = parse_date(data["promo"].select_one(provider["date-selector"]).text.strip())
            promo_ts = promo_dt.strftime("%Y%m%d_%H%M%S") if promo_dt else None

        provider_name = provider.get("name", "default")
        branch_raw = data.get("branch", "default")
        branch_dir = sanitize_path_component(branch_raw)
        download_dir = os.path.join("providers", provider_name, branch_dir)
        os.makedirs(download_dir, exist_ok=True)

        more_info_selector = provider.get("more-info-selector")
        need_more_info = bool(more_info_selector and more_info_selector != "none")

        def collect_buttons(row_el):
            if not row_el:
                return []
            btns = row_el.find_elements(By.CSS_SELECTOR, provider["download-button"])
            if not btns:
                try:
                    details_row = row_el.find_element(By.XPATH, "following-sibling::tr[1]")
                    btns = details_row.find_elements(By.CSS_SELECTOR, provider["download-button"])
                except NoSuchElementException:
                    btns = []
            return btns

        def download_buttons(buttons, filename_prefix, ts):
            saved = []
            for btn in buttons:
                onclick = btn.get_attribute("onclick") or ""
                href = btn.get_attribute("href") or ""

                raw = ""
                if onclick:
                    m = re.search(r"Download\('([^']+)'\)", onclick)
                    raw = m.group(1) if m else ""
                elif href:
                    raw = href
                if not raw:
                    continue

                if onclick:
                    raw_link = urljoin(self.driver.current_url, f"/Download/{raw.lstrip('/')}")
                else:
                    raw_link = urljoin(self.driver.current_url, raw)

                print(f"Download Link: {raw_link}")
                filename = f"{filename_prefix}_{ts}"
                parsed = urlparse(raw_link)
                ext = os.path.splitext(os.path.basename(parsed.path))[1] or ".gz"

                print(f"Downloading {filename}{ext}…")
                out = download_file_from_link(
                    raw_link, download_dir, filename + ext,
                    session=self._req_sess, verify_cert=False
                )
                if not out and onclick:
                    alt = urljoin(self.driver.current_url, f"/file/d/{raw.lstrip('/')}")
                    print(f"Retry with: {alt}")
                    out = download_file_from_link(
                        alt, download_dir, filename + ext,
                        session=self._req_sess, verify_cert=False
                    )
                if out:
                    saved.append(filename + ext)
            return saved

        saved_files = []

        price_row_el = self.refind_row_in_mode(provider, "price", price_row_id)
        if not price_row_el:
            print(f"[warn] price row not found in live DOM (id={price_row_id})")
        else:
            if need_more_info:
                print("Opening More info")
                try:
                    price_row_el.find_element(By.CSS_SELECTOR, more_info_selector).click()
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, provider["download-button"]))
                    )
                except NoSuchElementException:
                    pass
            buttons = collect_buttons(price_row_el)
            saved_files += download_buttons(buttons, "price", price_ts)

        if promo_row_id and promo_ts:
            promo_row_el = self.refind_row_in_mode(provider, "promo", promo_row_id)
            if not promo_row_el:
                print(f"[warn] promo row not found in live DOM (id={promo_row_id})")
            else:
                if need_more_info:
                    print("Opening More info")
                    try:
                        promo_row_el.find_element(By.CSS_SELECTOR, more_info_selector).click()
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, provider["download-button"]))
                        )
                    except NoSuchElementException:
                        pass
                buttons = collect_buttons(promo_row_el)
                saved_files += download_buttons(buttons, "promo", promo_ts)

        if saved_files:
            for f in saved_files:
                print(f"Downloaded to {os.path.join(download_dir, f)}")
        else:
            print(f"[info] No files downloaded for branch {branch_raw}")

        return saved_files, branch_dir

    def upload_file(self, saved_files, branch_dir, provider):
        if not saved_files:
            return
        s3_prefix = os.path.join(provider["name"], branch_dir).replace("\\", "/")
        for file in saved_files:
            upload_file_to_s3(s3_prefix, file)