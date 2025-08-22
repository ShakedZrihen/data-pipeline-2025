import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from salim.crawlers.providers.carrefour import CarrefourCrawler
from salim.crawlers.providers.cerberus import CerberusCrawler

SUPERMARKETS = {
    "yohananof": "yohananof",
    "tiv_taam": "tivtaam",
    "rami_levy": "ramilevi",
    "osher_ad": "osherad",
    "keshet_tamim": "keshet",
}

def run_all_crawlers():
    carrefourCrawler = CarrefourCrawler()
    print(f"\nRunning carrefour crawler...")
    carrefourCrawler.run()
    
    print(f"\nRunning cerberus crawlers for...")
    for market, uname in SUPERMARKETS.items():
        print(f"=== Running crawler for {market} ({uname}) ===")
        cerberusCrawler = CerberusCrawler(supermarket=market, username=uname)
        cerberusCrawler.run()

if __name__ == "__main__":
    run_all_crawlers()
