import time
from driver_setup import init_chrome_options, start_driver
from file_manager import get_download_dir, ensure_dir, move_file, delete_temp_dir
from downloader import get_file_links, download_files

def crawl_supermarket(supermarket, base_dir):
    download_dir = get_download_dir(base_dir, supermarket)
    ensure_dir(download_dir)

    chrome_options = init_chrome_options(download_dir)
    driver = start_driver(chrome_options)

    try:
        driver.get("https://url.publishedprices.co.il/login")
        time.sleep(2)

        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        driver.find_element(By.NAME, "username").send_keys(supermarket)
        driver.find_element(By.NAME, "password").send_keys("")
        driver.find_element(By.NAME, "password").send_keys(Keys.RETURN)
        time.sleep(3)

        file_map = get_file_links(driver)
        if not file_map:
            print("No files found.")
            return

        latest_hour = max(file_map, key=lambda x: x[1])[1]
        latest_files = [link for link, ts in file_map if ts == latest_hour]
        print(f"\n{len(latest_files)} files from {latest_hour.strftime('%Y-%m-%d %H:00')}:")

        def move_cb(file_path, branch_id):
            move_file(file_path, supermarket, branch_id, base_dir)

        download_files(driver, latest_files, download_dir, move_cb)

    finally:
        delete_temp_dir(download_dir)
        driver.quit()
        print(f"{supermarket}: Done.")
