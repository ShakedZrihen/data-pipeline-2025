import os
import glob
import json
from pathlib import Path

from config import S3_SIMULATOR_ROOT
from handler import process_local_file

OUTBOX_DIR = Path(__file__).parent / "outbox"
ITEMS_SAMPLE_PATH = Path(__file__).parent / "items_sample.json"


def is_target(fname: str) -> bool:

    n = fname.lower()
    if not n.endswith(".gz"):
        return False
    n_upper = fname.upper()
    return ("PRICEFULL" in n_upper) or ("PROMOFULL" in n_upper)

def write_items_sample(outbox_dir: str = str(Path(__file__).parent / "outbox"),
                       output_path: str = str(Path(__file__).parent / "items_sample.json"),
                       per_file: int = 10) -> int:
    rows = []
    for fp in sorted(glob.glob(os.path.join(outbox_dir, "*.json"))):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)

            full_items = data.get("items", []) or []
            row = {
                "provider":     data.get("provider"),
                "branch":       data.get("branch"),
                "type":         data.get("type"),
                "timestamp":    data.get("timestamp"),
                "supermarket":  data.get("supermarket"),
                "items_total":  len(full_items),
                "items_sample": full_items[:per_file],
                "outbox_path":  os.path.abspath(fp),
            }
            rows.append(row)
        except Exception as e:
            print(f"[WARN] failed reading {fp}: {e}")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    return len(rows)


def main():
    root = Path(S3_SIMULATOR_ROOT).resolve() / "providers"
    if not root.exists():
        print("No S3-simulator root found at:", root)
        return

    paths = sorted(root.rglob("*.gz"))
    if not paths:
        print("No .gz files under:", root)
        return

    OUTBOX_DIR.mkdir(parents=True, exist_ok=True)

    processed = 0
    for path in paths:
        fname = path.name
        if not is_target(fname):
            continue

        try:
            print("Processing:", path)
            _ = process_local_file(str(path))
            processed += 1
        except Exception as e:
            print("ERROR processing", path, "->", e)

    print(f"[OK] generated {processed} outbox JSON file(s).")


if __name__ == "__main__":
    main()
    wrote = write_items_sample()
    print(f"[OK] wrote items_sample.json with {wrote} entries (10 items per file).")
