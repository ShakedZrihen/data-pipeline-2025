from base_crawler import crawl
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def login_tivtaam(driver):
    driver.get("https://url.publishedprices.co.il/login")
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.ID, "username"))
    ).send_keys("osherad")
    driver.find_element(By.ID, "login-button").click()
    WebDriverWait(driver, 30).until(EC.url_contains("/file"))
    print("Logged in as osherad")
    driver.get("https://url.publishedprices.co.il/file")

if __name__ == "__main__":
    crawl(
        start_url="https://url.publishedprices.co.il/file",
        login_function=login_tivtaam
    )