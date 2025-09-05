import os, sys, gzip, json, argparse
from pathlib import Path
from typing import Iterator, Dict, Any
from utils.ingest_runner import ingest_payload_to_db

def _iter_files(base: Path, patterns: list[str]) -> Iterator[Path]:
    if not patterns:
        patterns = ["**/*.json", "**/*.json.gz"]
    for pat in patterns:
        for p in base.glob(pat):
            if p.is_file():
                yield p

def _read_json(path: Path) -> Dict[str, Any]:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    else:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

def main():
    ap = argparse.ArgumentParser(description="Ingest saved payload files into Postgres")
    ap.add_argument("--dir", default=os.getenv("MERGED_DIR", "./out"),
                    help="Base directory to scan (default: ./out or $MERGED_DIR)")
    ap.add_argument("--pattern", action="append",
                    help="Glob pattern(s) to include (default: **/*.json and **/*.json.gz)")
    ap.add_argument("--stop-on-error", action="store_true",
                    help="Stop on first file error (default: continue)")
    args = ap.parse_args()

    base = Path(args.dir).resolve()
    if not base.exists():
        print(f"Base directory not found: {base}", file=sys.stderr)
        sys.exit(2)

    files = sorted(_iter_files(base, args.pattern or []))
    print(f"Found {len(files)} files under {base}")

    ok = 0; bad = 0
    for i, path in enumerate(files, 1):
        try:
            payload = _read_json(path)
            ingest_payload_to_db(payload)
            ok += 1
            if i % 100 == 0:
                print(f"Processed {i}/{len(files)} …")
        except Exception as e:
            bad += 1
            msg = f"[{i}/{len(files)}] {path}: {e}"
            if args.stop_on_error:
                raise
            else:
                print("WARN:", msg, file=sys.stderr)

    print(f"Done. OK={ok} BAD={bad}")

if __name__ == "__main__":
    main()
