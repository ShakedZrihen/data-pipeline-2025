
# from selenium.webdriver.common.by import By
# from Base import *  # adjust import if needed
# import requests
# import os
# from datetime import datetime

# class SuperSapirCrawler(CrawlerBase):

#     def download_file(self, file_entry):
#         # Request actual file URL
#         print(f"Requesting file from JSON API: {file_entry['url']}")
#         response = requests.get(file_entry["url"])
#         response.raise_for_status()
#         json_data = response.json()
#         real_url = json_data[0]["SPath"]

#         # Inline path construction logic here
#         timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
#         folder = os.path.join("providers", self.provider_name, file_entry["branch"])
#         os.makedirs(folder, exist_ok=True)
#         filename = f"{file_entry['type']}_{timestamp}.gz"
#         local_path = os.path.join(folder, filename)

#         print(f"Downloading actual file from: {real_url}")
#         with requests.get(real_url, stream=True) as r:
#             r.raise_for_status()
#             with open(local_path, "wb") as f:
#                 for chunk in r.iter_content(chunk_size=8192):
#                     f.write(chunk)
#         print(f"Downloaded to: {local_path}")
#         upload_file_to_s3(self.provider_name, file_entry["branch"], local_path)


#     def extract_file_links(self):
#         rows = self.driver.find_elements(By.XPATH, "//tr[starts-with(@id, 'tr')]")
#         found = {"pricesFull": None, "promoFull": None}

#         for row in rows:
#             cols = row.find_elements(By.TAG_NAME, "td")
#             if len(cols) < 6:
#                 continue

#             branch = cols[2].text.strip()

#             try:
#                 button = cols[6].find_element(By.TAG_NAME, "button")
#                 onclick_value = button.get_attribute("onclick")
#                 filename = onclick_value.split("'")[1]
#             except Exception as e:
#                 print(f"Failed to extract button/filename: {e}")
#                 continue

#             api_url = f"https://supersapir.binaprojects.com/Download.aspx?FileNm={filename}"
#             file_type = "pricesFull" if filename.lower().startswith("price") else "promoFull"

#             if file_type == "pricesFull" and found["pricesFull"] is None:
#                 found["pricesFull"] = {"url": api_url, "branch": branch, "type": "pricesFull"}
#             elif file_type == "promoFull" and found["promoFull"] is None:
#                 found["promoFull"] = {"url": api_url, "branch": branch, "type": "promoFull"}

#             if all(found.values()):
#                 break

#         return [v for v in found.values() if v]

from selenium.webdriver.common.by import By
from Base import *  # adjust import if needed
import requests
import os
import re
from datetime import datetime

class SuperSapirCrawler(CrawlerBase):

    def download_file(self, file_entry):
        # Request actual file URL
        print(f"Requesting file from JSON API: {file_entry['url']}")
        response = requests.get(file_entry["url"])
        response.raise_for_status()
        json_data = response.json()
        real_url = json_data[0]["SPath"]

        # Inline path construction logic here
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        folder = os.path.join("providers", self.provider_name, file_entry["branch"])
        os.makedirs(folder, exist_ok=True)
        filename = f"{file_entry['type']}_{timestamp}.gz"
        local_path = os.path.join(folder, filename)

        print(f"Downloading actual file from: {real_url}")
        with requests.get(real_url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Downloaded to: {local_path}")
        upload_file_to_s3(self.provider_name, file_entry["branch"], local_path)

    def extract_file_links(self):
        rows = self.driver.find_elements(By.XPATH, "//tr[starts-with(@id, 'tr')]")
        found = {"pricesFull": None, "promoFull": None}

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 6:
                continue

            branch = cols[2].text.strip()
            # ✅ Keep only the first number found in the branch name
            m = re.search(r"\d+", branch)
            branch = m.group(0) if m else branch

            try:
                button = cols[6].find_element(By.TAG_NAME, "button")
                onclick_value = button.get_attribute("onclick")
                filename = onclick_value.split("'")[1]
            except Exception as e:
                print(f"Failed to extract button/filename: {e}")
                continue

            api_url = f"https://supersapir.binaprojects.com/Download.aspx?FileNm={filename}"
            file_type = "pricesFull" if filename.lower().startswith("price") else "promoFull"

            if file_type == "pricesFull" and found["pricesFull"] is None:
                found["pricesFull"] = {"url": api_url, "branch": branch, "type": "pricesFull"}
            elif file_type == "promoFull" and found["promoFull"] is None:
                found["promoFull"] = {"url": api_url, "branch": branch, "type": "promoFull"}

            if all(found.values()):
                break

        return [v for v in found.values() if v]
