import os, time, traceback
from extractor import Extractor

def _to_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

if __name__ == "__main__":
    print("[BOOT] starting extractor.run")
    interval = _to_int("EXTRACT_INTERVAL_SEC", 3600)
    jitter   = _to_int("EXTRACT_JITTER_SEC", 15)
    startup  = _to_int("STARTUP_DELAY_SEC", 5)

    time.sleep(startup)

    while True:
        try:
            print(f"[INFO] extractor tick (interval={interval}s)")
            Extractor().run()
            print("[RESULT] extractor cycle done")
        except Exception as e:
            print(f"[ERROR] extractor cycle failed: {type(e).__name__}: {e}")
            traceback.print_exc()

        nap = interval
        if jitter > 0:
            import random
            nap += random.randint(0, jitter)
        print(f"[INFO] sleeping {nap}s")
        time.sleep(nap)