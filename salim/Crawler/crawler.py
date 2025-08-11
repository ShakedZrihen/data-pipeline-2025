import os
from datetime import datetime
from providers.yohananof import run as run_yohananof
from providers.keshet import run as run_keshet
from providers.ramilevi import run as run_ramilevi

def main():
    os.makedirs("out", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M")

    print("\n=== Yohananof ===")
    y_prices, y_promos = run_yohananof(ts)
    print("saved:", y_prices)
    print("saved:", y_promos)

    print("\n=== Keshet ===")
    k_prices, k_promos = run_keshet(ts)
    print("saved:", k_prices)
    print("saved:", k_promos)

    print("\n=== RamiLevi ===")
    r_prices, r_promos = run_ramilevi(ts)
    print("saved:", r_prices)
    print("saved:", r_promos)

if __name__ == "__main__":
    main()
