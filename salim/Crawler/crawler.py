# Crawler/crawler.py
import os
from datetime import datetime
from providers.yohananof import run as run_yohananof

def main():
    os.makedirs("out", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M")

    print("\n=== Yohananof ===")
    y_prices, y_promos = run_yohananof(ts)
    print("saved:", y_prices)
    print("saved:", y_promos)

if __name__ == "__main__":
    main()
