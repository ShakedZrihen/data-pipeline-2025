import sys, os, glob, requests, json, time, urllib3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from crawlers.base import CrawlerBase

PROVIDER_URL = "https://prices.carrefour.co.il/"
PROVIDER_NAME = "carrefour"
DOWNLOAD_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "local_files", "carrefour"
)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
class CarrefourCrawler(CrawlerBase):
    def init_chrome_options(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        prefs = {
            "profile.default_content_settings.popups": 0,
            "download.default_directory": os.path.abspath(DOWNLOAD_DIR),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "plugins.always_open_pdf_externally": True,
            "download.restrictions": 0
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        )
        return chrome_options

    def __init__(self):
        super().__init__()
        self.base_url = PROVIDER_URL
        self.collected_files = []
        
    def get_driver(self):
        chrome_options = self.init_chrome_options()
        chromedriver_path = self.get_chromedriver_path()
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    
    def download_file(self, url, file_path):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': self.base_url,
            }
            response = requests.get(url, headers=headers, stream=True, verify=False)
            response.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True
        except Exception as e:
            print(f"Failed to download {url}: {e}")
            return False

    def get_page_source(self, provider_url):
        driver = self.get_driver()
        driver.get(provider_url)
        time.sleep(3)
        page_html = driver.page_source
        driver.quit()
        return page_html
    
    def download_files_from_html(self, html):
        driver = self.get_driver()
        driver.get(self.base_url)
        
        selected_branch_values = ["0116", "0009", "1295", "1147", "0063","0329", "4420", "0752", "1171", "1139"]

        for category in ["pricefull", "promofull"]:
            try:
                Select(driver.find_element(By.ID, "cat_filter")).select_by_value(category)
            except Exception as e:
                continue
            for branch_value in selected_branch_values:
                try:
                    Select(driver.find_element(By.ID, "branch_filter")).select_by_value(branch_value)
                    time.sleep(5)
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".filesDiv .fileDiv"))
                    )
                    file_divs = driver.find_elements(By.CSS_SELECTOR, ".filesDiv .fileDiv")
                    valid_file_divs = [div for div in file_divs if div.find_elements(By.CSS_SELECTOR, "a.downloadBtn")]
                    if not valid_file_divs:
                        continue
                    valid_file_divs[0].find_element(By.CSS_SELECTOR, "a.downloadBtn").click()
                    time.sleep(10)
                    files = glob.glob(os.path.join(DOWNLOAD_DIR, "*"))
                    if not files:
                        continue
                    latest_file = max(files, key=os.path.getctime)
                    for f in os.listdir(DOWNLOAD_DIR):
                        if f.endswith(".tmp") or "(1)" in f:
                            os.remove(os.path.join(DOWNLOAD_DIR, f))
                    file_type = "prices" if category.lower().startswith("price") else "promos"
                    file_info = {
                        "file_path": os.path.abspath(latest_file),
                        "branch": f"{PROVIDER_NAME}_{branch_value.strip()}",
                        "file_type": file_type
                    }
                    self.collected_files.append(file_info)
                except Exception as e:
                    print(f"Error selecting branch {branch_value} or downloading: {e}")
                    continue
        self.save_file_info(self.collected_files)
        driver.quit()
        return DOWNLOAD_DIR

    def save_file_info(self, files_info):
        base_provider_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "local_files", "carrefour")
        with open(os.path.join(base_provider_dir, "file_info.json"), "w", encoding="utf-8") as f:
            json.dump(files_info, f, indent=4, ensure_ascii=False)
        return base_provider_dir

    def run(self):
        return super().run(self.base_url)
    
if __name__ == "__main__":
    crawler = CarrefourCrawler()
    crawler.run()