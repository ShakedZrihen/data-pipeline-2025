import time
import os
from supermarket_crawler import crawl_supermarket

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    for market in ["yohananof", "Keshet", "RamiLevi"]:
        crawl_supermarket(market, base_dir)
        time.sleep(1)
