import os
import time
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_file_links(driver):
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "tbody.context.allow-dropdown-overflow tr"))
    )
    rows = driver.find_elements(By.CSS_SELECTOR, "tbody.context.allow-dropdown-overflow tr")
    file_map = []
    for row in rows:
        try:
            link_el = row.find_element(By.CSS_SELECTOR, "a")
            text = link_el.text
            href = link_el.get_attribute("href")
            if "PriceFull" in text and ".gz" in text:
                ts = datetime.strptime(text.split("-")[-1].replace(".gz", "")[:10], "%Y%m%d%H")
                file_map.append((href, ts))
        except:
            continue
    return file_map

def download_files(driver, latest_files, download_dir, move_callback):
    for link in latest_files:
        filename = os.path.basename(link)
        try:
            el = driver.find_element(By.LINK_TEXT, filename)
            el.click()
            time.sleep(2)

            full_path = os.path.join(download_dir, filename)
            while not os.path.exists(full_path) or any(f.endswith(".crdownload") for f in os.listdir(download_dir)):
                time.sleep(1)

            parts = filename.split("-")
            branch_id = parts[1] if len(parts) >= 3 else "unknown"
            move_callback(full_path, branch_id)

        except Exception as e:
            print(f"{filename}: {e}")


