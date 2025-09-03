import os
import sys
import json
import uuid
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Tuple, Set, Union

import boto3
from botocore.exceptions import ClientError
import psycopg2
from psycopg2 import Error as PGError
from psycopg2 import extensions as _pgx

from utils import iter_sqs_batches

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_LOADER_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "postgresql"))
if JSON_LOADER_DIR not in sys.path:
    sys.path.append(JSON_LOADER_DIR)
import json_loader as JL

try:
    from enrich_stores import enrich_dir as ENRICH_DIR
except Exception:
    ENRICH_DIR = None

WORK_DIR               = os.getenv("WORK_DIR", os.getcwd())

SQS_QUEUE_NAME         = os.getenv("SQS_QUEUE_NAME", "test-queue")
SQS_ENDPOINT_URL       = os.getenv("SQS_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION             = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY_ID      = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY  = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

SQS_WAIT_TIME          = int(os.getenv("SQS_WAIT_TIME", "5"))
SQS_MAX_PER_POLL       = int(os.getenv("SQS_MAX_PER_POLL", "10"))

STRICT_PRICE_ONLY      = os.getenv("STRICT_PRICE_ONLY", "1").lower() in ("1","true","yes","y")
PRICE_FIRST            = os.getenv("PRICE_FIRST", "1").lower() in ("1","true","yes","y")

STORES_DIR                 = os.getenv("STORES_DIR", os.path.join(WORK_DIR, "stores"))
ENRICH_OVERWRITE           = os.getenv("ENRICH_OVERWRITE", "0").lower() in ("1","true","yes","y")
ENRICH_NORMALIZE_BRANCH    = os.getenv("ENRICH_NORMALIZE_BRANCH", "1").lower() in ("1","true","yes","y")

def _make_sqs_client():
    return boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

def _ensure_queue_and_get_url(sqs_client, queue_name: str) -> str:
    try:
        return sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except sqs_client.exceptions.QueueDoesNotExist:
        attrs = {}
        if queue_name.endswith(".fifo"):
            attrs["FifoQueue"] = "true"
            attrs["ContentBasedDeduplication"] = "true"
        resp = sqs_client.create_queue(QueueName=queue_name, Attributes=attrs)
        return resp["QueueUrl"]

def _extract_body(msg: Dict[str, Any]) -> Optional[str]:
    body = msg.get("Body")
    if not body:
        return None
    try:
        maybe = json.loads(body)
        if isinstance(maybe, dict) and isinstance(maybe.get("Message"), str):
            return maybe["Message"]
        return body
    except Exception:
        return body

def _load_doc_from_any(body: str) -> Tuple[Optional[dict], Optional[str], Optional[str]]:
    s = body.strip().strip('"').strip("'")

    if os.path.exists(s) and s.lower().endswith(".json"):
        try:
            with open(s, "r", encoding="utf-8") as f:
                parsed = json.load(f)
            return _coerce_to_object(parsed), _guess_kind_from_any(parsed, s), s
        except Exception:
            pass

    try:
        parsed = json.loads(s)
        return _coerce_to_object(parsed), _guess_kind_from_any(parsed, s), None
    except Exception:
        return None, None, None

def _coerce_to_object(parsed: Any) -> dict:
    if isinstance(parsed, dict):
        return parsed
    if isinstance(parsed, list):
        return {"items": parsed}
    return {"data": parsed}

def _guess_kind_from_any(data: Any, origin_hint: Optional[str] = None) -> Optional[str]:
    if isinstance(data, dict) and isinstance(data.get("type"), str):
        t = data["type"].lower()
        if "promo" in t: return "promo"
        if "price" in t: return "price"
    if isinstance(origin_hint, str):
        low = origin_hint.lower()
        if "promo" in low: return "promo"
        if "price" in low: return "price"
    if isinstance(data, dict):
        keys = {k.lower() for k in data.keys()}
        if {"promos","promotions","promotion","campaigns","deals"} & keys: return "promo"
        if {"prices","price","items","products","rows","entries"} & keys: return "price"
    if isinstance(data, list) and data:
        if isinstance(data[0], dict) and ("promo" in "".join(data[0].keys()).lower()):
            return "promo"
        return "price"
    return None

def _write_temp_json(doc: dict, kind: str, temp_root: str) -> str:
    os.makedirs(temp_root, exist_ok=True)
    path = os.path.join(temp_root, f"{kind}_chunk_{uuid.uuid4().hex}.json")
    with open(path, "w", encoding="utf-8") as out:
        json.dump(doc, out, ensure_ascii=False)
    return path

def _log_db_error(msg_index: int, e: Exception) -> None:
    etype = type(e).__name__
    if isinstance(e, PGError):
        code = getattr(e, "pgcode", None)
        diag  = getattr(e, "diag", None)
        detail = getattr(diag, "message_detail", None) if diag else None
        ctx    = getattr(diag, "context", None) if diag else None
        print(f"[ERROR] Message #{msg_index}: {etype} (sqlstate={code}) {e}")
        if detail: print(f"[ERROR]   detail: {detail}")
        if ctx:    print(f"[ERROR]   context: {ctx}")
    else:
        print(f"[ERROR] Message #{msg_index}: {etype}: {e}")

def _clear_if_aborted(conn: psycopg2.extensions.connection) -> None:
    if conn.get_transaction_status() == _pgx.TRANSACTION_STATUS_INERROR:
        print("[WARN] Connection in aborted state → rolling back")
        conn.rollback()

def _auto_find_stores_dir() -> Optional[str]:
    from os.path import abspath, join, isdir
    candidates = [
        STORES_DIR,
        join(WORK_DIR, "stores"),
        join(BASE_DIR, "stores"),
        abspath(join(BASE_DIR, "..", "stores")),
        abspath(join(BASE_DIR, "..", "..", "stores")),
    ]
    for p in candidates:
        if p and isdir(p): return p
    return None

def _debug_json_shape(path: str) -> None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            keys = list(data.keys())
            print(f"[DEBUG] JSON top-level keys ({len(keys)}): {keys[:12]}")
            for k in keys[:12]:
                v = data[k]
                if isinstance(v, list):
                    print(f"[DEBUG]   '{k}': list(len={len(v)})")
                elif isinstance(v, dict):
                    print(f"[DEBUG]   '{k}': dict(keys={list(v.keys())[:8]})")
                else:
                    print(f"[DEBUG]   '{k}': {type(v).__name__}")
        elif isinstance(data, list):
            print(f"[DEBUG] JSON is a list(len={len(data)}) of {type(data[0]).__name__ if data else 'unknown'}")
        else:
            print(f"[DEBUG] JSON is {type(data).__name__}")
    except Exception as e:
        print(f"[DEBUG] failed to inspect JSON shape: {e}")

PROMO_HINT_KEYS = {
    "start","start_at","startdate",
    "end","end_at","enddate",
    "minqty","min_qty",
    "discount","discountrate","discount_rate","discountedprice","discount_price",
    "products","items"
}
ID_KEYS_STRICT = {"promotionid","promotion_id"}
ID_KEY_GENERIC = "id"

def _as_norm_id_text(x: Union[str,int]) -> str:
    return str(x).strip()

def _looks_like_promo_dict(d: Dict[str, Any]) -> bool:
    kl = {k.lower() for k in d.keys()}
    return bool(PROMO_HINT_KEYS & kl)

def _scan_promo_ids_any(obj: Any, acc: Set[str]) -> None:
    if isinstance(obj, dict):
        kl = {k.lower() for k in obj.keys()}
        for k, v in obj.items():
            klow = k.lower()
            if klow in ID_KEYS_STRICT or ("promotion" in klow and "id" in klow):
                acc.add(_as_norm_id_text(v))
        if ID_KEY_GENERIC in kl and _looks_like_promo_dict(obj):
            acc.add(_as_norm_id_text(obj.get("id")))
        for v in obj.values():
            _scan_promo_ids_any(v, acc)
    elif isinstance(obj, list):
        for v in obj:
            _scan_promo_ids_any(v, acc)

def _extract_promo_ids_from_path(tmp_path: str) -> Set[str]:
    try:
        with open(tmp_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        ids: Set[str] = set()
        _scan_promo_ids_any(data, ids)
        return ids
    except Exception:
        return set()

def _filter_out_promos_by_ids_inplace(tmp_path: str, drop_ids_text: Set[str]) -> int:
    """
    Remove any dict that has a promo-looking id (promotionId/promotion_id/id in promo-ish dict)
    whose normalized text value is in drop_ids_text. Returns count removed.
    """
    def _walk(obj: Any) -> Tuple[Any, int]:
        removed = 0
        if isinstance(obj, dict):
            for k, v in obj.items():
                kl = k.lower()
                if kl in ID_KEYS_STRICT or ("promotion" in kl and "id" in kl):
                    if _as_norm_id_text(v) in drop_ids_text:
                        return None, 1
            if ID_KEY_GENERIC in {k.lower() for k in obj.keys()} and _looks_like_promo_dict(obj):
                if _as_norm_id_text(obj.get("id")) in drop_ids_text:
                    return None, 1
            newd = {}
            for k, v in obj.items():
                nv, r = _walk(v)
                removed += r
                if nv is not None:
                    newd[k] = nv
            return newd, removed
        elif isinstance(obj, list):
            newl = []
            for v in obj:
                nv, r = _walk(v)
                removed += r
                if nv is not None:
                    newl.append(nv)
            return newl, removed
        else:
            return obj, 0

    try:
        with open(tmp_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        filtered, removed = _walk(data)
        if removed > 0:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(filtered, f, ensure_ascii=False)
        return removed
    except Exception as e:
        print(f"[WARN] failed to filter promos by ids: {e}")
        return 0

def _db_existing_promo_ids_as_text(cur, ids_text: List[str]) -> Set[str]:
    """
    Return existing promo ids as TEXT, comparing promotion_id::text = ANY(%s).
    Works whether the column is TEXT or INT.
    """
    if not ids_text:
        return set()
    try:
        cur.execute("SELECT promotion_id::text FROM promos WHERE promotion_id::text = ANY(%s)", (ids_text,))
        return {str(r[0]) for r in cur.fetchall()}
    except Exception as e:
        print(f"[WARN] failed checking existing promo ids: {e}")
        return set()

def enricher():
    print("[BOOT] starting json_loader.py")
    print("[INFO] connecting to database")
    print("[INFO] SQS config:", f"name={SQS_QUEUE_NAME}, endpoint={SQS_ENDPOINT_URL}, region={AWS_REGION}",
          f"(STRICT_PRICE_ONLY={int(STRICT_PRICE_ONLY)}, PRICE_FIRST={int(PRICE_FIRST)})")

    detected = _auto_find_stores_dir()
    if detected and detected != STORES_DIR:
        print(f"[INFO] STORES_DIR auto-detected → {detected}")
        globals()["STORES_DIR"] = detected
    print(f"[BOOT] STORES_DIR={STORES_DIR} (exists={os.path.isdir(STORES_DIR)})")
    if ENRICH_DIR is None:
        print("[WARN] Could not import enrich_stores.enrich_dir; enrichment will be skipped.")

    sqs = _make_sqs_client()
    queue_url = _ensure_queue_and_get_url(sqs, SQS_QUEUE_NAME)

    temp_root = tempfile.mkdtemp(prefix="sqs_chunks_", dir=WORK_DIR)
    print(f"[BOOT] temp dir: {temp_root}")

    total_products = 0
    total_links = 0
    total_processed_msgs = 0
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
                if not batch:
                    break

                batch_idx += 1
                print(f"\n[Batch #{batch_idx}] received {len(batch)} message(s)")

                prepared: List[Dict[str, Any]] = []
                for msg in batch:
                    body = _extract_body(msg)
                    doc, kind_hint, original_path = _load_doc_from_any(body or "")
                    prepared.append({
                        "msg": msg,
                        "body": body,
                        "doc": doc,
                        "kind_hint": kind_hint,
                        "original_path": original_path,
                        "receipt_handle": msg.get("ReceiptHandle"),
                    })

                if PRICE_FIRST:
                    prepared.sort(key=lambda x: (x["kind_hint"] != "price", 0))

                delete_entries: List[Dict[str, str]] = []
                temp_files_to_remove: List[str] = []
                temp_dirs_to_remove: List[str] = []
                processed_msgs = 0

                for i, p in enumerate(prepared, 1):
                    body = p["body"]; doc = p["doc"]; kind_hint = p["kind_hint"]; original_path = p["original_path"]
                    receipt_handle = p["receipt_handle"]

                    _clear_if_aborted(conn)

                    with conn.cursor() as cur:
                        try:
                            if not body:
                                print(f"[SKIP] Message #{i}: empty body; leaving in queue")
                                continue
                            if not isinstance(doc, dict):
                                print(f"[SKIP] Message #{i}: body is not valid JSON; leaving in queue")
                                continue

                            strict_price = STRICT_PRICE_ONLY and (kind_hint == "price")

                            msg_dir = os.path.join(temp_root, f"msg_{i}_{uuid.uuid4().hex[:8]}")
                            os.makedirs(msg_dir, exist_ok=True)
                            temp_dirs_to_remove.append(msg_dir)

                            if original_path and os.path.exists(original_path):
                                fname = os.path.basename(original_path)
                                tmp_path = os.path.join(msg_dir, fname)
                                shutil.copy2(original_path, tmp_path)
                            else:
                                tmp_path = _write_temp_json(doc, (kind_hint or "unknown"), msg_dir)
                            temp_files_to_remove.append(tmp_path)

                            if ENRICH_DIR and os.path.isdir(STORES_DIR):
                                try:
                                    updated = ENRICH_DIR(
                                        normalize_dir=msg_dir,
                                        stores_dir=STORES_DIR,
                                        out_dir=None,
                                        overwrite=ENRICH_OVERWRITE,
                                        normalize_branch=ENRICH_NORMALIZE_BRANCH,
                                    )
                                    print(f"[INFO] Message #{i}: enriched {updated} file(s) via enrich_dir")
                                except Exception as e:
                                    print(f"[WARN] Message #{i}: enrich_dir failed: {e}")
                            else:
                                if ENRICH_DIR is None:
                                    print(f"[WARN] Message #{i}: enrichment skipped (import failed)")
                                else:
                                    print(f"[WARN] Message #{i}: enrichment skipped (STORES_DIR not found: {STORES_DIR})")

                            def _load_price() -> int:
                                return JL.load_prices_file(cur, cache, tmp_path)

                            def _pre_dedup_promos(cur) -> Tuple[int, int]:
                                """
                                Find promo IDs (as text), drop ones already in DB.
                                Returns (found_ids, filtered_count).
                                """
                                ids = _extract_promo_ids_from_path(tmp_path)
                                if not ids:
                                    print(f"[INFO] Message #{i}: promo de-dup → no promotion_id found")
                                    return 0, 0
                                existing = _db_existing_promo_ids_as_text(cur, list(ids))
                                filtered = 0
                                if existing:
                                    filtered = _filter_out_promos_by_ids_inplace(tmp_path, existing)
                                    if filtered:
                                        print(f"[INFO] Message #{i}: promo pre-dedup → found={len(ids)}, filtered_out={filtered}")
                                else:
                                    print(f"[INFO] Message #{i}: promo pre-dedup → found={len(ids)}, none already in DB")
                                return len(ids), filtered

                            def _load_promo_after_dedup() -> Tuple[bool, int, int]:
                                found, filtered = _pre_dedup_promos(cur)
                                if found > 0 and filtered == found:
                                    return True, 0, filtered
                                try:
                                    cnt = JL.load_promos_file(cur, cache, tmp_path)
                                    return True, cnt, filtered
                                except Exception as e:
                                    _log_db_error(i, e)
                                    return False, 0, filtered

                            loaded_kind: Optional[str] = None
                            added_count: int = 0

                            if strict_price or kind_hint == "price":
                                try:
                                    cnt = _load_price()
                                    if cnt and cnt > 0:
                                        loaded_kind, added_count = "price", cnt
                                    else:
                                        print(f"[INFO] Message #{i}: 'price' loader returned 0 rows")
                                        _debug_json_shape(tmp_path)
                                        conn.rollback()
                                except Exception as e:
                                    _log_db_error(i, e)
                                    conn.rollback()

                            elif kind_hint == "promo":
                                ok, cnt, filtered = _load_promo_after_dedup()
                                if ok and cnt and cnt > 0:
                                    loaded_kind, added_count = "promo", cnt
                                elif ok and (cnt == 0) and (filtered > 0):
                                    conn.commit()
                                    if receipt_handle:
                                        delete_entries.append({"Id": str(i), "ReceiptHandle": receipt_handle})
                                    processed_msgs += 1
                                    total_processed_msgs += 1
                                    print(f"[NOOP] Message #{i}: all promos already present (filtered {filtered}) → deleted from queue")
                                else:
                                    pass

                            else:
                                tried_price = False
                                try:
                                    cnt = _load_price()
                                    tried_price = True
                                    if cnt and cnt > 0:
                                        loaded_kind, added_count = "price", cnt
                                    else:
                                        conn.rollback()
                                except Exception as e:
                                    _log_db_error(i, e)
                                    conn.rollback()
                                if not loaded_kind:
                                    ok, cnt, _ = _load_promo_after_dedup()
                                    if ok and cnt and cnt > 0:
                                        loaded_kind, added_count = "promo", cnt

                            if loaded_kind and added_count > 0:
                                conn.commit()
                                if loaded_kind == "price":
                                    total_products += added_count
                                else:
                                    total_links += added_count
                                if receipt_handle:
                                    delete_entries.append({"Id": str(i), "ReceiptHandle": receipt_handle})
                                processed_msgs += 1
                                total_processed_msgs += 1
                            else:
                                _clear_if_aborted(conn)

                        except Exception as e:
                            _log_db_error(i, e)
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            _clear_if_aborted(conn)

                if delete_entries:
                    try:
                        for off in range(0, len(delete_entries), 10):
                            chunk = delete_entries[off : off + 10]
                            resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=chunk)
                            if resp.get("Failed"):
                                print(f"[WARN] delete_message_batch partial failures: {resp['Failed']}")
                    except ClientError as e:
                        print(f"[WARN] delete_message_batch error: {e}")

                for fp in temp_files_to_remove:
                    try: os.remove(fp)
                    except OSError: pass
                for d in temp_dirs_to_remove:
                    shutil.rmtree(d, ignore_errors=True)

                try: conn.commit()
                except Exception: pass

                print(f"[STATS] totals so far → products: {total_products}, promo_links: {total_links}, processed msgs: {total_processed_msgs}")

    finally:
        try:
            shutil.rmtree(temp_root, ignore_errors=True)
        except Exception:
            pass

    print("[DONE] enricher finished.")


if __name__ == "__main__":
    enricher()
