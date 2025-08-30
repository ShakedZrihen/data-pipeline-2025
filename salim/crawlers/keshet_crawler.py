from base_crawler import crawl
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def login_keshet(driver):
    driver.get("https://url.publishedprices.co.il/login")
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "username"))
    ).send_keys("Keshet")
    driver.find_element(By.ID, "login-button").click()
    WebDriverWait(driver, 10).until(EC.url_contains("/file"))
    print("Logged in as Keshet")
    driver.get("https://url.publishedprices.co.il/file")

if __name__ == "__main__":
    crawl(
        start_url="https://url.publishedprices.co.il/file",
        login_function=login_keshet
    )