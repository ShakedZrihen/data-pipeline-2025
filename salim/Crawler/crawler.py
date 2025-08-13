# main_crawler.py - גרסה מעודכנת
import os
from datetime import datetime, timezone
from providers.yohananof import run as run_yohananof
from providers.keshet import run as run_keshet
from providers.ramilevi import run as run_ramilevi
# אין צורך ב-extractor הישן כאן, כי משימה 2 תטפל בזה

def main():
    os.makedirs("out", exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")

    # --- הרצת כל הקראולרים ---
    # כל פונקציית run אחראית עכשיו ליצור את התיקיות שלה בעצמה
    print("\n=== Yohananof ===")
    run_yohananof(ts)

    print("\n=== Keshet ===")
    run_keshet(ts)

    print("\n=== RamiLevi ===")
    run_ramilevi(ts)

    print("\n\n✅ All crawlers finished. Check the 'out' folder for the new structure.")

if __name__ == "__main__":
    main()