import os
import time
import platform
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from datetime import datetime
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import shutil

def init_chrome_options(supermarket: str) -> Options:
    chrome_options = Options()

    # --- עדכני את הנתיב לבסיס הפרויקט אצלך ---
    base_dir = r'C:\Users\Daniella Elbaz\Desktop\שנה ג סמסטר קיץ\סדנת פייתון\data-pipeline-2025\salim\app\finalCrawler'
    download_dir = os.path.join(base_dir, 'providers', supermarket, 'temp')
    os.makedirs(download_dir, exist_ok=True)

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }

    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")

    return chrome_options

def get_chromedriver_path() -> str:
    try:
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            print("Detected macOS ARM64, using specific chromedriver...")
            from webdriver_manager.core.os_manager import ChromeType
            driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        else:
            driver_path = ChromeDriverManager().install()

        print(f"Chrome driver path: {driver_path}")
        return driver_path
    except Exception as e:
        print(f"Error with webdriver-manager: {e}")
        print("Falling back to system chromedriver...")
        return "chromedriver"

def crawler(username: str):
    # --- עדכני את הנתיב לבסיס הפרויקט אצלך ---
    base_dir = r'C:\Users\Daniella Elbaz\Desktop\שנה ג סמסטר קיץ\סדנת פייתון\data-pipeline-2025\salim\app\finalCrawler'
    chrome_options = init_chrome_options(username)
    download_dir = os.path.join(base_dir, 'providers', username, 'temp')

    url = "https://url.publishedprices.co.il/login"  # הכתובת של פורטל הקבצים

    try:
        chromedriver_path = get_chromedriver_path()
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        driver = webdriver.Chrome(options=chrome_options)

    try:
        print(f"Opening {url}")
        driver.get(url)
        time.sleep(2)

        # התחברות: שם משתמש = שם הסופר, סיסמה ריקה (כמו בדוגמה שלך)
        driver.find_element(By.NAME, "username").send_keys(username)
        driver.find_element(By.NAME, "password").send_keys("")
        driver.find_element(By.NAME, "password").send_keys(Keys.RETURN)
        time.sleep(3)

        # המתנה לטעינת טבלת הקבצים
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tbody.context.allow-dropdown-overflow tr"))
        )

        rows = driver.find_elements(By.CSS_SELECTOR, "tbody.context.allow-dropdown-overflow tr")

        file_map = []
        for row in rows:
            try:
                link_el = row.find_element(By.CSS_SELECTOR, "a")
                text = link_el.text
                href = link_el.get_attribute("href")
                # מחפשים קבצי PriceFull עם סיומת .gz ושולפים את השעה מהשם
                if "PriceFull" in text and ".gz" in text:
                    timestamp_str = text.split("-")[-1].replace(".gz", "")[:10]  # YYYYMMDDHH
                    timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H")
                    file_map.append((href, timestamp))
            except Exception:
                continue

        if not file_map:
            print("No files found.")
            driver.quit()
            return

        latest_hour = max(file_map, key=lambda x: x[1])[1]
        latest_files = [link for link, ts in file_map if ts == latest_hour]

        print(f"\nFound {len(latest_files)} files from {latest_hour.strftime('%Y-%m-%d %H:00')}:")
        for link in latest_files:
            print(link)
            filename = os.path.basename(link)
            try:
                # קליק לפי טקסט קישור = שם הקובץ
                el = driver.find_element(By.LINK_TEXT, filename)
                el.click()
                print(f"Downloading: {filename}")
                time.sleep(2)

                full_path = os.path.join(download_dir, filename)
                # ממתינים עד שההורדה מסתיימת (אין crdownload)
                while not os.path.exists(full_path):
                    time.sleep(1)
                while any(f.endswith(".crdownload") for f in os.listdir(download_dir)):
                    time.sleep(1)

                # חילוץ מזהה סניף מהשם
                parts = filename.split("-")
                branch_id = parts[1] if len(parts) >= 3 else "unknown"

                final_dir = os.path.join(base_dir, 'providers', username, branch_id)
                os.makedirs(final_dir, exist_ok=True)

                final_path = os.path.join(final_dir, filename)
                os.rename(full_path, final_path)
                print(f"Saved to: {final_path}")

            except Exception as e:
                print(f"{filename}: {e}")

        # ניקוי temp
        temp_dir = os.path.join(base_dir, 'providers', username, 'temp')
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                print(f"Could not delete temp folder: {e}")

    finally:
        driver.quit()
        print("Chrome driver closed.")

if __name__ == "__main__":
    crawler("yohananof")
    time.sleep(1)
    crawler("Keshet")
    time.sleep(1)
    crawler("osherad")
