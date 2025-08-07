# main.py
from config import DOWNLOAD_DIR, TARGET_STORES, logger
from driver import make_driver
from utils import ensure_dir
from gov_crawler import GovCrawler
import time

def main():
    ensure_dir(DOWNLOAD_DIR)
    driver = make_driver()
    crawler = GovCrawler(driver)
    try:
        for frag, folder in TARGET_STORES:
            try:
                crawler.process_store(frag, folder)
            except Exception as e:
                logger.exception(f"Store {frag} failed: {e}")
            time.sleep(0.9)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    logger.info("All done.")

if __name__ == "__main__":
    main()
