from typing import Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium_helpers import init_driver, wait_for_files, extract_file_links, scroll_page_to_end
from settings import GOV_URL
from file_handlers import select_recent_files, process_downloads

def navigate_to_provider(driver: webdriver.Chrome, provider_name: str) -> None:
    print(f"Accessing page for {provider_name}...")
    driver.get(GOV_URL)
    scroll_page_to_end(driver)
    xpath = f"//tr[td[contains(normalize-space(.), '{provider_name}')]]//a[contains(., 'לצפייה במחירים')]"
    link = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, xpath)))
    driver.execute_script("arguments[0].click()", link)
    print("Navigated to prices page.")

def perform_login(driver: webdriver.Chrome, username: str, password: str) -> None:
    def _submit_login_form():
        driver.find_element(By.NAME, "username").send_keys(username)
        driver.find_element(By.NAME, "password").send_keys(password)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()
        print("Login form submitted.")

    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "username")))
        print("Found login form in main document.")
        _submit_login_form()
        return
    except TimeoutException:
        pass

    for frame in driver.find_elements(By.TAG_NAME, "iframe"):
        driver.switch_to.frame(frame)
        try:
            if driver.find_elements(By.NAME, "username"):
                print("Found login form in iframe.")
                _submit_login_form()
                driver.switch_to.default_content()
                return
        finally:
            driver.switch_to.default_content()

    print("ℹNo login form required.")

def handle_provider(provider_name: str, config: Dict[str, str]) -> None:
    driver = init_driver()
    try:
        navigate_to_provider(driver, provider_name)
        perform_login(driver, config["username"], config["password"])
        wait_for_files(driver)
        links = extract_file_links(driver)
        print(f"Discovered {len(links)} files.")
        selected_urls = select_recent_files(links)
        print(f"Selected {selected_urls} files for download.")
        process_downloads(selected_urls, config["folder"])
    finally:
        print("Closing browser.")
        try:
            driver.quit()
        except Exception:
            pass