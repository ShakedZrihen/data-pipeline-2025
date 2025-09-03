import os, time, traceback
from crawler import Crawler

def _to_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

if __name__ == "__main__":
    print("[BOOT] starting crawler.run")
    interval = _to_int("CRAWL_INTERVAL_SEC", 3600)
    jitter    = _to_int("CRAWL_JITTER_SEC", 15)

    while True:
        try:
            print(f"[INFO] crawler tick (interval={interval}s)")
            Crawler().crawl() 
            print("[RESULT] crawler cycle done")
        except Exception as e:
            print(f"[ERROR] crawler cycle failed: {type(e).__name__}: {e}")
            traceback.print_exc()

        # sleep with tiny jitter
        nap = interval
        if jitter > 0:
            import random
            nap += random.randint(0, jitter)
        print(f"[INFO] sleeping {nap}s")
        time.sleep(nap)