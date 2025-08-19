import time
from typing import List
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def init_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--start-maximized")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def scroll_page_to_end(driver: webdriver.Chrome) -> None:
    last_height = 0
    while True:
        driver.execute_script("window.scrollBy(0, 800);")
        time.sleep(0.4)
        current_height = driver.execute_script("return document.body.scrollHeight")
        if current_height == last_height:
            break
        last_height = current_height
        
def wait_for_files(driver: webdriver.Chrome) -> None:
    print("Loading file list...")
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.XPATH, "//a[contains(@href,'.pdf') or contains(@href,'.xlsx') "
                       "or contains(@href,'.csv') or contains(@href,'.gz')]")
        )
    )
    time.sleep(1.5)

def extract_file_links(driver: webdriver.Chrome) -> List[str]:
    print("Searching for downloadable files...")
    try:
        links = driver.find_elements(
            By.XPATH,
            "//a[contains(@href,'.pdf') or contains(@href,'.xlsx') "
            "or contains(@href,'.csv') or contains(@href,'.gz')]"
        )
        return list({link.get_attribute("href") for link in links if link.get_attribute("href")})
    except Exception as e:
        print(f"Error extracting links: {e}")
        return []
