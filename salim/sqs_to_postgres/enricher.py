# enricher.py
import os
import sys
import json
import uuid
import tempfile
import shutil
from typing import Tuple, Optional

import boto3
from botocore.exceptions import ClientError
import psycopg2

from utils import iter_sqs_batches  # generator that yields up to N msgs per poll

# ── json_loader: do NOT modify it, just import and call ────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_LOADER_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "postgresql"))
if JSON_LOADER_DIR not in sys.path:
    sys.path.append(JSON_LOADER_DIR)
import json_loader as JL  # PG_DSN, Cache, load_prices_file, load_promos_file

# ── Config ────────────────────────────────────────────────────────────────────
WORK_DIR               = os.getenv("WORK_DIR", os.getcwd())
SQS_QUEUE_NAME         = os.getenv("SQS_QUEUE_NAME", "test-queue")
SQS_ENDPOINT_URL       = os.getenv("SQS_ENDPOINT_URL", "http://localhost:4567")
AWS_REGION             = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY_ID      = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY  = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
SQS_WAIT_TIME          = int(os.getenv("SQS_WAIT_TIME", "5"))
SQS_MAX_PER_POLL       = int(os.getenv("SQS_MAX_PER_POLL", "10"))

# NEW: if true, never try promo when we think it’s a price
STRICT_PRICE_ONLY      = os.getenv("STRICT_PRICE_ONLY", "1").lower() in ("1","true","yes","y")

# ── SQS helpers ────────────────────────────────────────────────────────────────
def _make_sqs_client():
    return boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

def _ensure_queue_and_get_url(sqs_client, queue_name: str):
    try:
        return sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except sqs_client.exceptions.QueueDoesNotExist:
        attrs = {}
        if queue_name.endswith(".fifo"):
            attrs["FifoQueue"] = "true"
            attrs["ContentBasedDeduplication"] = "true"
        resp = sqs_client.create_queue(QueueName=queue_name, Attributes=attrs)
        return resp["QueueUrl"]

# ── Classification ────────────────────────────────────────────────────────────
def _detect_doc_type_from_obj(obj: dict) -> Optional[str]:
    """Return 'price' or 'promo' if clear; else None."""
    def lowers(d: dict): return {str(k).lower(): v for k, v in d.items()}
    o = lowers(obj)

    # top-level type: pricesFull / promoFull
    t = o.get("type")
    if isinstance(t, str):
        tl = t.lower()
        if "price" in tl: return "price"
        if "promo" in tl: return "promo"

    # find items list
    items = None
    for k in ("items", "data", "entries", "rows", "lines"):
        if isinstance(obj.get(k), list) and obj[k]:
            items = obj[k]; break
    if items is None:
        for v in o.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                items = v; break

    # quick hints
    if "promotions" in o or "promotionid" in o: return "promo"
    if "prices" in o: return "price"

    # scan fields
    if isinstance(items, list) and items:
        price_like = {"price","saleprice","regularprice","unitprice","currentprice","itemprice","finalprice"}
        promo_like = {"discountprice","discountrate","discount_rate","minqty","min_qty","promotionid","promotion_id","products"}
        ph = pr = 0
        for it in items[:50]:
            if not isinstance(it, dict): continue
            lit = lowers(it)
            if any(k in lit for k in price_like): ph += 1
            if any(k in lit for k in promo_like) or isinstance(lit.get("products"), list): pr += 1
        if ph and not pr: return "price"
        if pr and not ph: return "promo"

    return None

def _parse_message_to_doc_and_type(msg) -> Tuple[Optional[dict], Optional[str]]:
    """Handle JSON body, double-encoded JSON, or a file path body."""
    body = (msg.get("Body") or "").strip()
    val = body
    for _ in range(2):
        if isinstance(val, str) and val.lstrip().startswith("{"):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, dict):
                    return parsed, _detect_doc_type_from_obj(parsed)
                if isinstance(parsed, str):
                    val = parsed; continue
            except Exception:
                break
    # file path fallback
    path = body.strip('"').strip("'")
    if path:
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if isinstance(obj, dict):
                return obj, _detect_doc_type_from_obj(obj)
        except Exception as e:
            print(f"[SKIP] Could not read JSON from path {path!r}: {e!r}")
    return None, None

# ── Temp file (json_loader expects a path) ─────────────────────────────────────
def _write_temp_json(doc: dict, kind: str, temp_root: str) -> str:
    os.makedirs(temp_root, exist_ok=True)
    f = os.path.join(temp_root, f"{kind}_chunk_{uuid.uuid4().hex}.json")
    with open(f, "w", encoding="utf-8") as out:
        json.dump(doc, out, ensure_ascii=False)
    return f

# ── Logging helper for DB errors ──────────────────────────────────────────────
def _log_db_error(i: int, *errs: Exception):
    def describe(e: Exception) -> str:
        base = f"{e.__class__.__name__}: {e}"
        diag = getattr(e, "diag", None)
        diag_bits = []
        if diag:
            for attr in ("message_primary","message_detail","schema_name","table_name","constraint_name","column_name","context"):
                v = getattr(diag, attr, None)
                if v: diag_bits.append(f"{attr}={v}")
        return base + (" | " + " ; ".join(diag_bits) if diag_bits else "")
    print(f"[SKIP] Message #{i}: DB error → " + " | ".join(describe(e) for e in errs if e))

# ── Main ──────────────────────────────────────────────────────────────────────
def enricher():
    print("[BOOT] starting json_loader.py")
    print("[INFO] connecting to database")
    print("[INFO] SQS config:", f"name={SQS_QUEUE_NAME}, endpoint={SQS_ENDPOINT_URL}, region={AWS_REGION}",
          f"(STRICT_PRICE_ONLY={int(STRICT_PRICE_ONLY)})")

    sqs = _make_sqs_client()
    queue_url = _ensure_queue_and_get_url(sqs, SQS_QUEUE_NAME)

    temp_root = tempfile.mkdtemp(prefix="sqs_chunks_", dir=WORK_DIR)
    print(f"[BOOT] temp dir: {temp_root}")

    total_products = 0
    total_links = 0
    batch_idx = 0

    try:
        with psycopg2.connect(JL.PG_DSN) as conn:
            conn.autocommit = False
            cache = JL.Cache()

            for batch in iter_sqs_batches(
                queue_name=SQS_QUEUE_NAME,
                endpoint_url=SQS_ENDPOINT_URL,
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                wait_time_seconds=SQS_WAIT_TIME,
                max_number_per_poll=SQS_MAX_PER_POLL,
                max_empty_polls=1,
            ):
                batch_idx += 1
                print(f"\n[Batch #{batch_idx}] received {len(batch)} message(s)")

                delete_entries = []
                temp_files_to_remove: list[str] = []
                processed_msgs = 0

                with conn.cursor() as cur:
                    for i, msg in enumerate(batch, 1):
                        receipt_handle = msg.get("ReceiptHandle")
                        savepoint = f"msg_{i}"

                        try:
                            doc, kind_hint = _parse_message_to_doc_and_type(msg)
                            if not isinstance(doc, dict):
                                print(f"[SKIP] Message #{i}: not a JSON object; leaving in queue")
                                continue

                            # Force price-only when we’re confident
                            if STRICT_PRICE_ONLY and (kind_hint == "price"):
                                strict_price = True
                            else:
                                strict_price = False

                            tmp_path = _write_temp_json(doc, (kind_hint or "unknown"), temp_root)
                            temp_files_to_remove.append(tmp_path)

                            cur.execute(f"SAVEPOINT {savepoint};")

                            def _load(which: str) -> int:
                                return JL.load_prices_file(cur, cache, tmp_path) if which == "price" \
                                       else JL.load_promos_file(cur, cache, tmp_path)

                            loaded_kind, added_count = None, 0

                            # Helper: try exactly one kind; if 0 or error → rollback & signal failure
                            def _try_one(kind: str) -> Tuple[bool, int]:
                                try:
                                    cnt = _load(kind)
                                    if cnt and cnt > 0:
                                        return True, cnt
                                    print(f"[INFO] Message #{i}: '{kind}' returned 0 rows")
                                    cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint};")
                                    return False, 0
                                except Exception as e:
                                    cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint};")
                                    _log_db_error(i, e)
                                    return False, 0

                            if strict_price:
                                ok, cnt = _try_one("price")
                                if ok:
                                    loaded_kind, added_count = "price", cnt
                                else:
                                    # keep in SQS for inspection/retry; don’t try promo
                                    continue

                            else:
                                # Not strict: use hint if any; otherwise price → promo
                                if kind_hint == "promo":
                                    ok, cnt = _try_one("promo")
                                    if ok:
                                        loaded_kind, added_count = "promo", cnt
                                    else:
                                        # try price as fallback
                                        ok2, cnt2 = _try_one("price")
                                        if ok2:
                                            loaded_kind, added_count = "price", cnt2
                                        else:
                                            continue
                                elif kind_hint == "price":
                                    ok, cnt = _try_one("price")
                                    if ok:
                                        loaded_kind, added_count = "price", cnt
                                    else:
                                        # try promo only if not strict
                                        ok2, cnt2 = _try_one("promo")
                                        if ok2:
                                            loaded_kind, added_count = "promo", cnt2
                                        else:
                                            continue
                                else:
                                    # unknown → try price then promo
                                    ok, cnt = _try_one("price")
                                    if ok:
                                        loaded_kind, added_count = "price", cnt
                                    else:
                                        ok2, cnt2 = _try_one("promo")
                                        if ok2:
                                            loaded_kind, added_count = "promo", cnt2
                                        else:
                                            continue

                            # success for this message
                            cur.execute(f"RELEASE SAVEPOINT {savepoint};")
                            if loaded_kind == "price":
                                total_products += added_count
                            else:
                                total_links += added_count

                            if receipt_handle:
                                delete_entries.append({"Id": str(i), "ReceiptHandle": receipt_handle})
                            processed_msgs += 1

                        except Exception as e:
                            try: cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint};")
                            except Exception: pass
                            _log_db_error(i, e)

                    print("[INFO] committing transaction for this batch")
                    conn.commit()

                if delete_entries:
                    try:
                        for off in range(0, len(delete_entries), 10):
                            chunk = delete_entries[off:off+10]
                            resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=chunk)
                            if resp.get("Failed"): print(f"[WARN] delete_message_batch partial failures: {resp['Failed']}")
                    except ClientError as e:
                        print(f"[WARN] delete_message_batch error: {e}")

                for fp in temp_files_to_remove:
                    try: os.remove(fp)
                    except OSError: pass

                print(f"[STATS] totals so far → products: {total_products}, promo_links: {total_links}, processed msgs: {processed_msgs}")

    finally:
        try: shutil.rmtree(temp_root, ignore_errors=True)
        except Exception: pass

    print("[DONE] enricher finished.")


if __name__ == "__main__":
    enricher()
