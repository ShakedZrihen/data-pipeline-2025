from base_crawler import crawl
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def login_yohananof(driver):
    driver.get("https://url.publishedprices.co.il/login")

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "username"))
    ).send_keys("yohananof")

    driver.find_element(By.ID, "login-button").click()

    WebDriverWait(driver, 10).until(EC.url_contains("/file"))
    print("Logged in as Yohananof")
    driver.get("https://url.publishedprices.co.il/file")



if __name__ == "__main__":
    crawl(
        start_url="https://www.gov.il/he/pages/cpfta_prices_regulations/",
        download_base_url="https://url.publishedprices.co.il/",
        login_function=login_yohananof
    )