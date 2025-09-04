import os, glob,json
from config import S3_SIMULATOR_ROOT
from handler import process_local_file
from pathlib import Path
def write_items_sample(outbox_dir: str = str(Path(__file__).parent / "outbox"),
                       output_path: str = str(Path(__file__).parent / "items_sample.json"),
                       per_file: int = 10) -> int:
    rows = []
    for fp in sorted(glob.glob(os.path.join(outbox_dir, "*.json"))):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            rows.append({
                "provider":   data.get("provider"),
                "branch":     data.get("branch"),
                "type":       data.get("type"),
                "timestamp":  data.get("timestamp"),
                "items_total": len(data.get("items", [])),
                "items_sample": data.get("items", [])[:per_file],
                "outbox_path": os.path.abspath(fp),
            })
        except Exception as e:
            print(f"[WARN] failed reading {fp}: {e}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    return len(rows)
def is_target(fname: str) -> bool:
    n = fname.lower()
    return n.endswith(".gz") and ("price" in n or "promo" in n)

def main():
    root = os.path.join(S3_SIMULATOR_ROOT, "providers")
    paths = sorted(glob.glob(os.path.join(root, "**", "*.gz"), recursive=True))
    if not paths:
        print("No .gz files under:", root)
        return

    for path in paths:
        if not is_target(os.path.basename(path)):
            continue
        print("Processing:", path)
        try:
            process_local_file(path)
        except Exception as e:
            print("ERROR processing", path, "->", e)

if __name__ == "__main__":
    main()
    wrote = write_items_sample()
    print(f"[OK] wrote items_sample.json with {wrote} entries (10 items per file).")
