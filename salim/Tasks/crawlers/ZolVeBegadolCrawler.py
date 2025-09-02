

from selenium.webdriver.common.by import By
from Base import CrawlerBase
from upload_to_s3 import upload_file_to_s3
import requests
import os
import re
from datetime import datetime
import io, gzip, zipfile





class ZolVeBegadolCrawler(CrawlerBase):

    def to_gz_bytes(self,raw: bytes) -> bytes:
        """Return real .gz bytes from raw download:
        - pass-through if already gzip
        - if ZIP: pick first *.xml (or *.gz) entry; if xml → gzip it; if gz → pass-through
        - else: gzip the raw bytes
        """
        # gzip magic
        if raw[:2] == b"\x1f\x8b":
            return raw
        # zip magic
        if raw[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                names = [n for n in zf.namelist() if not n.endswith("/")]
                if not names:
                    raise ValueError("zip archive empty")
                chosen = next((n for n in names if n.lower().endswith(".xml")), None) \
                        or next((n for n in names if n.lower().endswith(".gz")), None) \
                        or names[0]
                inner = zf.read(chosen)
                if chosen.lower().endswith(".gz") or inner[:2] == b"\x1f\x8b":
                    return inner
                out = io.BytesIO()
                with gzip.GzipFile(fileobj=out, mode="wb") as gz:
                    gz.write(inner)
                return out.getvalue()
        # plain xml/other → gzip it
        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode="wb") as gz:
            gz.write(raw)
        return out.getvalue()


    def download_file(self, file_entry):
        # Request actual file URL
        print(f"Requesting file from JSON API: {file_entry['url']}")
        response = requests.get(file_entry["url"])
        response.raise_for_status()
        json_data = response.json()
        real_url = json_data[0]["SPath"]

        # Inline path construction logic here
        timestamp = file_entry["ts"]  # guaranteed 12 digits

        # timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        folder = os.path.join("providers", self.provider_name, file_entry["branch"])
        os.makedirs(folder, exist_ok=True)
        filename = f"{file_entry['type']}_{timestamp}.gz"
        local_path = os.path.join(folder, filename)

        print(f"Downloading actual file from: {real_url}")
        with requests.get(real_url, stream=True) as r:
            r.raise_for_status()
            raw_bytes = r.content  # get the raw response bytes

        # 🔑 Normalize → always real .gz
        gz_bytes = self.to_gz_bytes(raw_bytes)

        # Save to disk (optional, for debugging)
        with open(local_path, "wb") as f:
            f.write(gz_bytes)

        print(f"Saved normalized gzip to: {local_path}")
        upload_file_to_s3(self.provider_name, file_entry["branch"], local_path)


    def extract_file_links(self):
        rows = self.driver.find_elements(By.XPATH, "//tr[starts-with(@id, 'tr')]")
        found = {"pricesFull": None, "promoFull": None}

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 6:
                continue

            branch = cols[1].text.strip()
            # Keep only the first number found in the branch name
            m = re.search(r"\d+", branch)
            branch = m.group(0) if m else branch

            try:
                button = cols[5].find_element(By.TAG_NAME, "button")
                onclick_value = button.get_attribute("onclick")
                filename = onclick_value.split("'")[1]
                ts = CrawlerBase.last_token_ts12(filename)  # works for both "31/08/2025 20:32" and "20:32 31/08/2025"

            except Exception as e:
                print(f"Failed to extract button/filename: {e}")
                continue

            
            api_url = f"https://zolvebegadol.binaprojects.com/Download.aspx?FileNm={filename}"
            file_type = "pricesFull" if filename.lower().startswith("price") else "promoFull"

            if file_type == "pricesFull" and found["pricesFull"] is None:
                found["pricesFull"] = {"url": api_url, "branch": branch, "type": "pricesFull", "ts": ts}
            elif file_type == "promoFull" and found["promoFull"] is None:
                found["promoFull"] = {"url": api_url, "branch": branch, "type": "promoFull", "ts": ts}

            if all(found.values()):
                break

        return [v for v in found.values() if v]
