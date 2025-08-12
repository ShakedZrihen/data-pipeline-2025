import os
from datetime import datetime, timezone
from providers.yohananof import run as run_yohananof
from providers.keshet import run as run_keshet
from providers.ramilevi import run as run_ramilevi
from providers.extractor import run_extractor

def main():
    os.makedirs("out", exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")

    # --- Yohananof ---
    print("\n=== Yohananof ===")
    y_prices, y_promos = run_yohananof(ts)
    y_folder = os.path.join("out", f"yohananof_{ts}")
    print("saved:", y_prices)
    print("saved:", y_promos)
    if os.path.exists(y_folder):
        output_file_path = os.path.join("out", f"yohananof_extracted_data_{ts}.json")
        run_extractor(y_folder, output_file_path)

    # --- Keshet ---
    print("\n=== Keshet ===")
    k_prices, k_promos = run_keshet(ts)
    k_folder = os.path.join("out", f"keshet_{ts}")
    print("saved:", k_prices)
    print("saved:", k_promos)
    if os.path.exists(k_folder):
        output_file_path = os.path.join("out", f"keshet_extracted_data_{ts}.json")
        run_extractor(k_folder, output_file_path)

    # --- RamiLevi ---
    print("\n=== RamiLevi ===")
    r_prices, r_promos = run_ramilevi(ts)
    r_folder = os.path.join("out", f"ramilevi_{ts}")
    print("saved:", r_prices)
    print("saved:", r_promos)
    if os.path.exists(r_folder):
        output_file_path = os.path.join("out", f"ramilevi_extracted_data_{ts}.json")
        run_extractor(r_folder, output_file_path)

if __name__ == "__main__":
    main()