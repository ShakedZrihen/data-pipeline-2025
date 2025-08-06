import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from salim.app.crawlers.providers.hazi_hinam import HaziHinamCrawler
from salim.app.crawlers.providers.yohananof import YohananofCrawler
from salim.app.crawlers.providers.tiv_taam import TivTaamCrawler
from salim.app.crawlers.providers.rami_levi import RamiLeviCrawler
from salim.app.crawlers.providers.osherad import OsherAdCrawler
from salim.app.crawlers.providers.carrefour import CarrefourCrawler

def run_all_crawlers():
    crawlers = [
        (HaziHinamCrawler, "https://shop.hazi-hinam.co.il/Prices"),
        (YohananofCrawler, "https://url.publishedprices.co.il/file"),
        (TivTaamCrawler, "https://url.publishedprices.co.il/login"),
        (RamiLeviCrawler, "https://url.publishedprices.co.il/login"),
        (OsherAdCrawler, "https://url.publishedprices.co.il/login"),
        (CarrefourCrawler, "https://prices.carrefour.co.il/"),
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
