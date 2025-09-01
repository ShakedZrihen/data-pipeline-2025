import os, glob
from config import S3_SIMULATOR_ROOT
from handler import process_local_file

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
