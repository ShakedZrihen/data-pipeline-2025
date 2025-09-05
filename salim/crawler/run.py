import os, time, traceback
from crawler import Crawler

INTERVAL_MIN = int(os.getenv("CRAWL_EVERY_MINUTES", "60"))

def run_once():
    c = Crawler()
    try:
        c.run()
    finally:
        try:
            if c.driver:
                c.driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    print(f"[crawler] starting loop, interval={INTERVAL_MIN} minutes")
    while True:
        start = time.time()
        try:
            run_once()
        except Exception as e:
            print(f"[crawler] uncaught error: {e}")
            traceback.print_exc()
        elapsed = time.time() - start
        to_sleep = max(0, INTERVAL_MIN * 60 - elapsed)
        print(f"[crawler] sleeping {int(to_sleep)}s")
        time.sleep(to_sleep)
