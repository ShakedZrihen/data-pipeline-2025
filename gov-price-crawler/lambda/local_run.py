# local_run.py
# Process local downloads (providers\downloads\...) into normalized JSON results
# Optional: also send the envelopes to SQS using QUEUE_URL

from __future__ import annotations

import os
import re
import gzip
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime, timezone

from parser import parse_to_items
from local_saver import save_result_copy

# ---------- timestamp helpers ----------
def _plausible_ymdhm(s: str) -> bool:
    if not (s.isdigit() and len(s) == 12):
        return False
    y, m, d, hh, mm = int(s[:4]), int(s[4:6]), int(s[6:8]), int(s[8:10]), int(s[10:12])
    return (2010 <= y <= 2099) and (1 <= m <= 12) and (1 <= d <= 31) and (0 <= hh <= 23) and (0 <= mm <= 59)

def normalize_timestamp(ts: str) -> str:
    try:
        if _plausible_ymdhm(ts):
            return datetime.strptime(ts, "%Y%m%d%H%M").replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y%m%d-%H%M", "%Y-%m-%d_%H%M", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(ts, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                continue
        iso = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
        dt = datetime.fromisoformat(iso)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def looks_like_html(text: str) -> bool:
    s = text.lstrip().lower()
    return s.startswith("<!doctype html") or s.startswith("<html") or "<html" in s[:2048]

# ---------- IO helpers ----------
def _decode_bytes(raw: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "windows-1255", "iso-8859-8"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")

def read_text(path: Path) -> str:
    data = path.read_bytes()
    if len(data) >= 2 and data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return _decode_bytes(data)

# ---------- core processing ----------
PATTERN = re.compile(r"(?P<type>pricesFull|promoFull)_(?P<ts>\d{12})\.gz$", re.I)

def process_one(file_path: Path, provider: str, branch: str, send: bool = False, queue_url: Optional[str] = None) -> Tuple[int, int]:
    m = PATTERN.search(file_path.name)
    if not m:
        print(f"skip (name not matching): {file_path}")
        return (0, 0)

    text = read_text(file_path)
    if looks_like_html(text):
        print(f"skip HTML: {file_path}")
        return (0, 0)

    ftype = m.group("type")
    ts_raw = m.group("ts")
    ts_iso = normalize_timestamp(ts_raw)

    items = parse_to_items(text)
    envelope = {"provider": provider, "branch": branch, "type": ftype, "timestamp": ts_iso, "items": items}
    save_result_copy(envelope)

    sent = 0
    if send:
        from sqs_producer import send_envelope
        sent = send_envelope(envelope, queue_url or os.getenv("QUEUE_URL", ""))

    print(f"OK {provider}/{branch} {ftype}: items={len(items)} sent={sent} ts='{ts_raw}' -> '{ts_iso}'")
    return (len(items), sent)

def bulk_process(base_dir: Path, send: bool = False, queue_url: Optional[str] = None) -> Tuple[int, int, int]:
    files = 0
    total_items = 0
    total_msgs = 0

    if not base_dir.exists():
        raise FileNotFoundError(f"Downloads folder not found: {base_dir}")

    for provider_dir in sorted(p for p in base_dir.iterdir() if p.is_dir()):
        provider = provider_dir.name
        for branch_dir in sorted(b for b in provider_dir.iterdir() if b.is_dir()):
            branch = branch_dir.name
            for f in sorted(branch_dir.glob("*.gz")):
                if PATTERN.search(f.name):
                    files += 1
                    items_cnt, msgs = process_one(f, provider, branch, send=send, queue_url=queue_url)
                    total_items += items_cnt
                    total_msgs += msgs

    print(f"done. files={files} items={total_items} messages={total_msgs}")
    return (files, total_items, total_msgs)

# ---------- auto-detect downloads dir ----------
def find_downloads_dir(cli_dir: str | None) -> Path:
    if cli_dir:
        p = Path(cli_dir).resolve()
        if p.exists():
            print(f"Using downloads dir (CLI): {p}")
            return p
        raise FileNotFoundError(f"--dir path not found: {p}")

    cwd = Path.cwd().resolve()
    here = Path(__file__).resolve().parent

    candidates = [
        cwd / "providers" / "downloads",                # running from repo root
        here.parent / "providers" / "downloads",        # running from lambda/
        here.parent.parent / "providers" / "downloads", # one level higher (just in case)
    ]
    print("Candidate downloads paths:")
    for c in candidates:
        print(" -", c)

    for c in candidates:
        if c.exists():
            print("Using downloads dir:", c)
            return c

    raise FileNotFoundError(
        "Downloads folder not found. Tried:\n  " + "\n  ".join(str(c) for c in candidates) +
        "\nTip: run with --dir ..\\providers\\downloads"
    )

# ---------- entrypoint ----------
if __name__ == "__main__":
    import argparse, sys
    ap = argparse.ArgumentParser(description="Process local providers\\downloads into normalized JSON (and optionally send to SQS).")
    ap.add_argument("--dir", help="Root of local downloads tree (auto-detected if omitted)")
    ap.add_argument("--send", action="store_true", help="Also send to SQS using QUEUE_URL env var")
    ap.add_argument("--queue", help="Explicit SQS queue URL (overrides QUEUE_URL env var)")
    ap.add_argument("--file", help="Process a single file (requires --provider and --branch)")
    ap.add_argument("--provider", help="Provider name (for --file)")
    ap.add_argument("--branch", help="Branch name (for --file)")
    args = ap.parse_args()

    # allow imports when run from repo root or from lambda/
    sys.path.append(str(Path(__file__).parent.resolve()))

    if args.file:
        if not (args.provider and args.branch):
            raise SystemExit("--file requires --provider and --branch")
        process_one(Path(args.file), args.provider, args.branch, send=args.send, queue_url=args.queue)
    else:
        base_dir = find_downloads_dir(args.dir)
        bulk_process(base_dir, send=args.send, queue_url=args.queue)
