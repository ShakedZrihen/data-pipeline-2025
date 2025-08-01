import sys
import os

# מוסיפים את ה-root ל-PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from salim.app.crawlers.providers.carrefour import CarrefourCrawler
from salim.app.crawlers.providers.hazi_hinam import HaziHinamCrawler
# from salim.app.crawlers.providers.yohananof import YohananofCrawler

def run_all_crawlers():
    crawlers = [
        (CarrefourCrawler, "https://prices.carrefour.co.il/"),
        (HaziHinamCrawler, "https://shop.hazi-hinam.co.il/Prices"),
    ]

    for CrawlerClass, url in crawlers:
        try:
            print(f"\nRunning crawler for {CrawlerClass.__name__} ({url})...")
            crawler = CrawlerClass(url)
            crawler.run(url)
            print(f"Finished {CrawlerClass.__name__}\n")
        except Exception as e:
            print(f"Error running {CrawlerClass.__name__}: {e}")

if __name__ == "__main__":
    run_all_crawlers()
