# # # # # # # # extractor/extractor.py
# # # # # # # import re
# # # # # # # from datetime import datetime, timezone
# # # # # # # import boto3

# # # # # # # from .config import ENDPOINT_URL, AWS_REGION, S3_BUCKET, SUPPORTED_TYPES
# # # # # # # from .xml_normalizer import parse_gz_xml_bytes, build_payload
# # # # # # # from .sqs_producer import send_json
# # # # # # # from .db_tracker import get_last_run, put_last_run

# # # # # # # s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=AWS_REGION)

# # # # # # # KEY_RE = re.compile(
# # # # # # #     r"^providers/(?P<provider>[^/]+)/(?P<branch>[^/]+)/(?P<filetype>pricesFull|promoFull)_(?P<timestamp>\d{14})\.gz$"
# # # # # # # )

# # # # # # # def _parse_key(key: str):
# # # # # # #     m = KEY_RE.match(key)
# # # # # # #     if not m:
# # # # # # #         return None
# # # # # # #     gd = m.groupdict()
# # # # # # #     ts_dt = datetime.strptime(gd["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
# # # # # # #     return gd["provider"], gd["branch"], gd["filetype"], ts_dt

# # # # # # # def _process_object(key: str):
# # # # # # #     parsed = _parse_key(key)
# # # # # # #     if not parsed:
# # # # # # #         print(f"Skip non-matching key: {key}")
# # # # # # #         return
# # # # # # #     provider, branch, file_type, ts_dt = parsed
# # # # # # #     if file_type not in SUPPORTED_TYPES:
# # # # # # #         print(f"Unsupported type: {file_type}")
# # # # # # #         return

# # # # # # #     body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
# # # # # # #     root = parse_gz_xml_bytes(body)
# # # # # # #     payload = build_payload(file_type, root, provider, branch, ts_dt)
# # # # # # #     send_json(payload)
# # # # # # #     put_last_run(provider, branch, file_type, payload["timestamp"])
# # # # # # #     print(f"Processed {key} → {len(payload.get('items', []))} items")

# # # # # # # def scan_and_process_new_objects():
# # # # # # #     """Scheduled mode: scan S3/prefix=providers/ and process keys newer than last_run per (provider,branch,type)."""
# # # # # # #     token = None
# # # # # # #     while True:
# # # # # # #         kwargs = {"Bucket": S3_BUCKET, "Prefix": "providers/"}
# # # # # # #         if token:
# # # # # # #             kwargs["ContinuationToken"] = token
# # # # # # #         resp = s3.list_objects_v2(**kwargs)
# # # # # # #         for obj in resp.get("Contents", []):
# # # # # # #             key = obj["Key"]
# # # # # # #             parsed = _parse_key(key)
# # # # # # #             if not parsed:
# # # # # # #                 continue
# # # # # # #             provider, branch, file_type, ts_dt = parsed
# # # # # # #             last = get_last_run(provider, branch, file_type)
# # # # # # #             last_dt = datetime.strptime(last, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) if last else None
# # # # # # #             if last_dt and ts_dt <= last_dt:
# # # # # # #                 continue
# # # # # # #             _process_object(key)
# # # # # # #         if resp.get("IsTruncated"):
# # # # # # #             token = resp.get("NextContinuationToken")
# # # # # # #         else:
# # # # # # #             break

# # # # # # # def handler(event, context):
# # # # # # #     # If triggered by a schedule (EventBridge) or manual invoke without records: do a backlog scan
# # # # # # #     if not event or event.get("source") == "aws.events" or "Records" not in event:
# # # # # # #         scan_and_process_new_objects()
# # # # # # #         return {"ok": True, "mode": "scheduled-scan"}

# # # # # # #     # S3 event path (kept for compatibility if you ever enable S3 notifications)
# # # # # # #     for rec in event.get("Records", []):
# # # # # # #         bucket = rec["s3"]["bucket"]["name"]
# # # # # # #         key = rec["s3"]["object"]["key"]
# # # # # # #         if bucket != S3_BUCKET:
# # # # # # #             print(f"Skip foreign bucket: {bucket}")
# # # # # # #             continue
# # # # # # #         _process_object(key)
# # # # # # #     return {"ok": True, "mode": "s3-event"}

# # # # # # # extractor/extractor.py
# # # # # # import re
# # # # # # from datetime import datetime, timezone
# # # # # # import boto3

# # # # # # from .config import ENDPOINT_URL, AWS_REGION, S3_BUCKET, SUPPORTED_TYPES
# # # # # # from .xml_normalizer import parse_gz_xml_bytes, build_payload
# # # # # # from .sqs_producer import send_json
# # # # # # from .db_tracker import put_last_run

# # # # # # s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=AWS_REGION)

# # # # # # KEY_RE = re.compile(
# # # # # #     r"^providers/(?P<provider>[^/]+)/(?P<branch>[^/]+)/(?P<filetype>pricesFull|promoFull)_(?P<timestamp>\d{14})\.gz$"
# # # # # # )

# # # # # # def _parse_key(key: str):
# # # # # #     m = KEY_RE.match(key)
# # # # # #     if not m:
# # # # # #         return None
# # # # # #     gd = m.groupdict()
# # # # # #     ts_dt = datetime.strptime(gd["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
# # # # # #     return gd["provider"], gd["branch"], gd["filetype"], ts_dt

# # # # # # def _process_object(key: str):
# # # # # #     parsed = _parse_key(key)
# # # # # #     if not parsed:
# # # # # #         print(f"Skip non-matching key: {key}")
# # # # # #         return
# # # # # #     provider, branch, file_type, ts_dt = parsed
# # # # # #     if file_type not in SUPPORTED_TYPES:
# # # # # #         print(f"Unsupported type: {file_type}")
# # # # # #         return

# # # # # #     try:
# # # # # #         body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
# # # # # #         root = parse_gz_xml_bytes(body)
# # # # # #         payload = build_payload(file_type, root, provider, branch, ts_dt)
# # # # # #         send_json(payload)
# # # # # #         put_last_run(provider, branch, file_type, payload["timestamp"])
# # # # # #         print(f"Processed {key} → {len(payload.get('items', []))} items")
# # # # # #     except Exception as e:
# # # # # #         # Fault tolerant: log and continue
# # # # # #         print(f"ERROR processing {key}: {e}")

# # # # # # def handler(event, context):
# # # # # #     # Expect S3 ObjectCreated events
# # # # # #     recs = event.get("Records", [])
# # # # # #     for rec in recs:
# # # # # #         bucket = rec["s3"]["bucket"]["name"]
# # # # # #         key = rec["s3"]["object"]["key"]
# # # # # #         if bucket != S3_BUCKET:
# # # # # #             print(f"Skip event from bucket={bucket} (configured={S3_BUCKET})")
# # # # # #             continue
# # # # # #         _process_object(key)
# # # # # #     return {"ok": True, "processed": len(recs)}
# # # # # # extractor/extractor.py
# # # # # import re
# # # # # from datetime import datetime, timezone
# # # # # import boto3

# # # # # from .config import ENDPOINT_URL, AWS_REGION, S3_BUCKET, SUPPORTED_TYPES
# # # # # from .xml_normalizer import parse_gz_xml_bytes, build_payload
# # # # # from .sqs_producer import send_json
# # # # # from .db_tracker import put_last_run
# # # # # from .log_utils import log, log_exception


# # # # # s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=AWS_REGION)

# # # # # KEY_RE = re.compile(
# # # # #     r"^providers/(?P<provider>[^/]+)/(?P<branch>[^/]+)/(?P<filetype>pricesFull|promoFull)_(?P<timestamp>\d{14})\.gz$"
# # # # # )

# # # # # def _parse_key(key: str):
# # # # #     m = KEY_RE.match(key)
# # # # #     if not m:
# # # # #         return None
# # # # #     gd = m.groupdict()
# # # # #     ts_dt = datetime.strptime(gd["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
# # # # #     return gd["provider"], gd["branch"], gd["filetype"], ts_dt

# # # # # def _process_object(key: str):
# # # # #     parsed = _parse_key(key)
# # # # #     if not parsed:
# # # # #         log("skip_non_matching_key", key=key)
# # # # #         return
# # # # #     provider, branch, file_type, ts_dt = parsed
# # # # #     if file_type not in SUPPORTED_TYPES:
# # # # #         log("skip_unsupported_type", key=key, file_type=file_type)
# # # # #         return

# # # # #     try:
# # # # #         log("s3_get_object_start", bucket=S3_BUCKET, key=key)
# # # # #         body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
# # # # #         log("s3_get_object_ok", size=len(body))

# # # # #         root = parse_gz_xml_bytes(body)
# # # # #         payload = build_payload(file_type, root, provider, branch, ts_dt)
# # # # #         items = payload.get("items", [])
# # # # #         log("payload_built", provider=provider, branch=branch, type=file_type, timestamp=payload["timestamp"], items=len(items))

# # # # #         send_json(payload)
# # # # #         log("sqs_send_ok", items=len(items))

# # # # #         put_last_run(provider, branch, file_type, payload["timestamp"])
# # # # #         log("last_run_saved", pk=f"{provider}#{branch}#{file_type}", ts=payload["timestamp"])

# # # # #         log("processed_ok", key=key, items=len(items))
# # # # #     except Exception as e:
# # # # #         log_exception("process_failed", e, key=key)

# # # # # def handler(event, context):
# # # # #     mode = "s3-event" if event and "Records" in event else "unknown"
# # # # #     log("handler_start", mode=mode)
# # # # #     recs = event.get("Records", []) if event else []
# # # # #     for rec in recs:
# # # # #         bucket = rec["s3"]["bucket"]["name"]
# # # # #         key = rec["s3"]["object"]["key"]
# # # # #         if bucket != S3_BUCKET:
# # # # #             log("skip_foreign_bucket", event_bucket=bucket, configured=S3_BUCKET)
# # # # #             continue
# # # # #         _process_object(key)
# # # # #     log("handler_done", processed=len(recs))
# # # # #     return {"ok": True, "processed": len(recs)}

# # # # # extractor/extractor.py
# # # # import re
# # # # from datetime import datetime, timezone
# # # # import boto3

# # # # from .config import ENDPOINT_URL, AWS_REGION, S3_BUCKET, SUPPORTED_TYPES
# # # # from .xml_normalizer import parse_gz_xml_bytes, build_payload
# # # # from .sqs_producer import send_json
# # # # from .db_tracker import put_last_run
# # # # from .log_utils import log, log_exception
# # # # from .s3_writer import put_payload_json   # <-- NEW

# # # # s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=AWS_REGION)

# # # # KEY_RE = re.compile(
# # # #     r"^providers/(?P<provider>[^/]+)/(?P<branch>[^/]+)/(?P<filetype>pricesFull|promoFull)_(?P<timestamp>\d{14})\.gz$"
# # # # )

# # # # def _parse_key(key: str):
# # # #     m = KEY_RE.match(key)
# # # #     if not m:
# # # #         return None
# # # #     gd = m.groupdict()
# # # #     ts_dt = datetime.strptime(gd["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
# # # #     return gd["provider"], gd["branch"], gd["filetype"], ts_dt

# # # # def _process_object(key: str):
# # # #     parsed = _parse_key(key)
# # # #     if not parsed:
# # # #         log("skip_non_matching_key", key=key)
# # # #         return
# # # #     provider, branch, file_type, ts_dt = parsed
# # # #     if file_type not in SUPPORTED_TYPES:
# # # #         log("skip_unsupported_type", key=key, file_type=file_type)
# # # #         return

# # # #     try:
# # # #         log("s3_get_object_start", bucket=S3_BUCKET, key=key)
# # # #         body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
# # # #         log("s3_get_object_ok", size=len(body))

# # # #         root = parse_gz_xml_bytes(body)
# # # #         payload = build_payload(file_type, root, provider, branch, ts_dt)

# # # #         # --- NEW: write normalized JSON to S3 under Json/<provider>/<branch>/... ---
# # # #         json_key = put_payload_json(payload)
# # # #         log("json_saved", json_key=json_key)

# # # #         # Continue the original flow (SQS + last run)
# # # #         items = payload.get("items", [])
# # # #         send_json(payload)
# # # #         log("sqs_send_ok", items=len(items))

# # # #         put_last_run(provider, branch, file_type, payload["timestamp"])
# # # #         log("last_run_saved", pk=f"{provider}#{branch}#{file_type}", ts=payload["timestamp"])

# # # #         log("processed_ok", key=key, items=len(items))
# # # #     except Exception as e:
# # # #         log_exception("process_failed", e, key=key)

# # # # def handler(event, context):
# # # #     mode = "s3-event" if event and "Records" in event else "unknown"
# # # #     log("handler_start", mode=mode)
# # # #     recs = event.get("Records", []) if event else []
# # # #     for rec in recs:
# # # #         bucket = rec["s3"]["bucket"]["name"]
# # # #         key = rec["s3"]["object"]["key"]
# # # #         if bucket != S3_BUCKET:
# # # #             log("skip_foreign_bucket", event_bucket=bucket, configured=S3_BUCKET)
# # # #             continue
# # # #         _process_object(key)
# # # #     log("handler_done", processed=len(recs))
# # # #     return {"ok": True, "processed": len(recs)}

# # # # extractor/lambda/lambda_extractor/extractor.py
# # # import re
# # # import json
# # # from datetime import datetime, timezone
# # # import boto3

# # # from .config import ENDPOINT_URL, AWS_REGION, S3_BUCKET, SUPPORTED_TYPES
# # # from .xml_normalizer import parse_gz_xml_bytes, build_payload
# # # from .sqs_producer import send_json
# # # from .db_tracker import put_last_run
# # # from .s3_writer import write_payload_json
# # # from .log_utils import log, log_exception

# # # # S3 client (LocalStack or AWS based on ENDPOINT_URL)
# # # s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=AWS_REGION)

# # # # Expected object key format:
# # # # providers/<provider>/<branch>/(pricesFull|promoFull)_YYYYMMDDhhmmss.gz
# # # KEY_RE = re.compile(
# # #     r"^providers/(?P<provider>[^/]+)/(?P<branch>[^/]+)/(?P<filetype>pricesFull|promoFull)_(?P<timestamp>\d{14})\.gz$"
# # # )


# # # def _parse_key(key: str):
# # #     """Return (provider, branch, file_type, ts_dt) or None if not matching."""
# # #     m = KEY_RE.match(key)
# # #     if not m:
# # #         return None
# # #     gd = m.groupdict()
# # #     ts_dt = datetime.strptime(gd["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
# # #     return gd["provider"], gd["branch"], gd["filetype"], ts_dt


# # # def _process_object(key: str):
# # #     parsed = _parse_key(key)
# # #     if not parsed:
# # #         log("skip_non_matching_key", key=key)
# # #         return
# # #     provider, branch, file_type, ts_dt = parsed
# # #     if file_type not in SUPPORTED_TYPES:
# # #         log("skip_unsupported_type", key=key, file_type=file_type)
# # #         return

# # #     try:
# # #         # Fetch gz payload
# # #         log("s3_get_object_start", bucket=S3_BUCKET, key=key)
# # #         body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
# # #         log("s3_get_object_ok", size=len(body))

# # #         # Parse + normalize into our JSON payload
# # #         root = parse_gz_xml_bytes(body)
# # #         payload = build_payload(file_type, root, provider, branch, ts_dt)
# # #         items = payload.get("items", [])
# # #         log(
# # #             "payload_built",
# # #             provider=provider,
# # #             branch=branch,
# # #             type=file_type,
# # #             timestamp=payload.get("timestamp"),
# # #             items=len(items),
# # #         )

# # #         # Write normalized JSON to S3 under Json/<provider>/<branch>/<type>_<ts>.json
# # #         out_key = write_payload_json(payload)
# # #         log("json_written", key=out_key, approx_bytes=len(json.dumps(payload, ensure_ascii=False)))

# # #         # Send message to SQS
# # #         send_json(payload)
# # #         log("sqs_send_ok", items=len(items))

# # #         # Track last run in DynamoDB
# # #         put_last_run(provider, branch, file_type, payload["timestamp"])
# # #         log("last_run_saved", pk=f"{provider}#{branch}#{file_type}", ts=payload["timestamp"])

# # #         log("processed_ok", key=key, items=len(items))
# # #     except Exception as e:
# # #         # Fault tolerant: log and continue
# # #         log_exception("process_failed", e, key=key)


# # # def handler(event, context):
# # #     """Lambda entrypoint: expects S3 ObjectCreated events."""
# # #     mode = "s3-event" if event and "Records" in event else "unknown"
# # #     log("handler_start", mode=mode)

# # #     recs = event.get("Records", []) if event else []
# # #     for rec in recs:
# # #         bucket = rec["s3"]["bucket"]["name"]
# # #         key = rec["s3"]["object"]["key"]
# # #         if bucket != S3_BUCKET:
# # #             log("skip_foreign_bucket", event_bucket=bucket, configured=S3_BUCKET)
# # #             continue
# # #         _process_object(key)

# # #     log("handler_done", processed=len(recs))
# # #     return {"ok": True, "processed": len(recs)}

# # # extractor/extractor.py
# # import re
# # import json
# # import traceback
# # from datetime import datetime, timezone
# # import boto3

# # from .config import ENDPOINT_URL, AWS_REGION, S3_BUCKET, SUPPORTED_TYPES
# # from .xml_normalizer import parse_gz_xml_bytes, build_payload
# # from .sqs_producer import send_json
# # from .db_tracker import put_last_run
# # from .s3_writer import write_json_s3  # <-- JSON writer to S3

# # s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=AWS_REGION)

# # KEY_RE = re.compile(
# #     r"^providers/(?P<provider>[^/]+)/(?P<branch>[^/]+)/(?P<filetype>pricesFull|promoFull)_(?P<timestamp>\d{14})\.gz$"
# # )

# # def _parse_key(key: str):
# #     m = KEY_RE.match(key)
# #     if not m:
# #         return None
# #     gd = m.groupdict()
# #     ts_dt = datetime.strptime(gd["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
# #     return gd["provider"], gd["branch"], gd["filetype"], ts_dt

# # def _debug_write(prefix: str, key: str, msg: str, extra: dict | None = None):
# #     """
# #     Always try to write a small debug marker into S3 so we can see progress without CloudWatch.
# #     Creates objects under: Json/_debug/<ISO8601>_<sanitized-key>.json
# #     """
# #     # Fetch OUTPUT_JSON_PREFIX env (default "Json/")
# #     import os
# #     out_prefix = os.environ.get("OUTPUT_JSON_PREFIX", "Json/")
# #     if not out_prefix.endswith("/"):
# #         out_prefix += "/"

# #     # sanitize key for path safety
# #     safe_key = key.replace("/", "_")
# #     ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
# #     dbg_obj_key = f"{out_prefix}_debug/{prefix}_{ts}_{safe_key}.json"
# #     payload = {"message": msg, "key": key, "extra": (extra or {})}
# #     try:
# #         s3.put_object(
# #             Bucket=S3_BUCKET,
# #             Key=dbg_obj_key,
# #             Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
# #             ContentType="application/json; charset=utf-8",
# #         )
# #     except Exception:
# #         # If even debug write fails, we can’t do much more.
# #         pass

# # def _process_object(key: str):
# #     parsed = _parse_key(key)
# #     if not parsed:
# #         _debug_write("skip_non_matching", key, "Key did not match expected pattern")
# #         return

# #     provider, branch, file_type, ts_dt = parsed
# #     if file_type not in SUPPORTED_TYPES:
# #         _debug_write("skip_unsupported", key, f"Unsupported type: {file_type}")
# #         return

# #     try:
# #         _debug_write("fetch_start", key, "Fetching object from S3", {"bucket": S3_BUCKET})
# #         body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
# #         _debug_write("fetch_ok", key, "Fetched bytes", {"size": len(body)})

# #         root = parse_gz_xml_bytes(body)
# #         payload = build_payload(file_type, root, provider, branch, ts_dt)

# #         # 1) Write normalized JSON to S3 (under Json/<original-key>.json)
# #         write_json_s3(
# #             payload,
# #             bucket=S3_BUCKET,
# #             original_key=key,  # function will derive Json/<...>.json
# #         )
# #         _debug_write("json_written", key, "Normalized JSON written to S3", {"items": len(payload.get("items", []))})

# #         # 2) Send to SQS (optional, part of assignment)
# #         send_json(payload)
# #         _debug_write("sqs_sent", key, "Message sent to SQS", {"items": len(payload.get("items", []))})

# #         # 3) Track last run
# #         put_last_run(provider, branch, file_type, payload["timestamp"])
# #         _debug_write("last_run_saved", key, "Saved last run", {"timestamp": payload["timestamp"]})

# #     except Exception as e:
# #         _debug_write(
# #             "process_failed",
# #             key,
# #             "Exception during processing",
# #             {"error": str(e), "trace": traceback.format_exc()},
# #         )

# # def handler(event, context):
# #     recs = event.get("Records", []) if event else []
# #     _debug_write("handler_start", f"records:{len(recs)}", "Start handling event")
# #     for rec in recs:
# #         bucket = rec["s3"]["bucket"]["name"]
# #         obj_key = rec["s3"]["object"]["key"]
# #         if bucket != S3_BUCKET:
# #             _debug_write("skip_foreign_bucket", obj_key, f"event bucket={bucket}, expected={S3_BUCKET}")
# #             continue
# #         _process_object(obj_key)
# #     _debug_write("handler_done", f"records:{len(recs)}", "Done")
# #     return {"ok": True, "processed": len(recs)}

# # extractor/extractor.py
# import re
# import json
# import traceback
# from datetime import datetime, timezone
# import boto3
# import os

# # Robust imports: package (relative) first, then flat (absolute) fallback
# try:
#     from .config import ENDPOINT_URL, AWS_REGION, S3_BUCKET, SUPPORTED_TYPES
#     from .xml_normalizer import parse_gz_xml_bytes, build_payload
#     from .sqs_producer import send_json
#     from .db_tracker import put_last_run
#     from .s3_writer import write_json_s3
# except ImportError:
#     from config import ENDPOINT_URL, AWS_REGION, S3_BUCKET, SUPPORTED_TYPES
#     from xml_normalizer import parse_gz_xml_bytes, build_payload
#     from sqs_producer import send_json
#     from db_tracker import put_last_run
#     from s3_writer import write_json_s3

# s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=AWS_REGION)

# KEY_RE = re.compile(
#     r"^providers/(?P<provider>[^/]+)/(?P<branch>[^/]+)/(?P<filetype>pricesFull|promoFull)_(?P<timestamp>\d{14})\.gz$"
# )

# def _parse_key(key: str):
#     m = KEY_RE.match(key)
#     if not m:
#         return None
#     gd = m.groupdict()
#     ts_dt = datetime.strptime(gd["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
#     return gd["provider"], gd["branch"], gd["filetype"], ts_dt

# def _out_prefix() -> str:
#     # honor env var; default to "Json/"
#     p = os.environ.get("OUTPUT_JSON_PREFIX", "Json/")
#     return p if p.endswith("/") else (p + "/")

# def _debug_write(tag: str, key: str, msg: str, extra: dict | None = None):
#     """Write a small debug blob so you can see progress without CloudWatch."""
#     safe_key = key.replace("/", "_")
#     ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
#     dbg_key = f"{_out_prefix()}_debug/{tag}_{ts}_{safe_key}.json"
#     payload = {"message": msg, "key": key, "extra": (extra or {})}
#     try:
#         s3.put_object(
#             Bucket=S3_BUCKET,
#             Key=dbg_key,
#             Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
#             ContentType="application/json; charset=utf-8",
#         )
#     except Exception:
#         # if even debug fails, ignore
#         pass

# def _process_object(key: str):
#     parsed = _parse_key(key)
#     if not parsed:
#         _debug_write("skip_non_matching", key, "Key did not match expected pattern")
#         return

#     provider, branch, file_type, ts_dt = parsed

#     if file_type not in SUPPORTED_TYPES:
#         _debug_write("skip_unsupported", key, f"Unsupported type: {file_type}")
#         return

#     try:
#         _debug_write("fetch_start", key, "Fetching object from S3", {"bucket": S3_BUCKET})
#         body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
#         _debug_write("fetch_ok", key, "Fetched bytes", {"size": len(body)})

#         root = parse_gz_xml_bytes(body)
#         payload = build_payload(file_type, root, provider, branch, ts_dt)

#         # Write normalized JSON to S3 under Json/<key>.json
#         write_json_s3(payload, bucket=S3_BUCKET, original_key=key)
#         _debug_write("json_written", key, "Normalized JSON written", {"items": len(payload.get("items", []))})

#         # Send to SQS (assignment requirement)
#         send_json(payload)
#         _debug_write("sqs_sent", key, "Message sent to SQS", {"items": len(payload.get("items", []))})

#         # Track last run
#         put_last_run(provider, branch, file_type, payload["timestamp"])
#         _debug_write("last_run_saved", key, "Saved last run", {"timestamp": payload["timestamp"]})

#     except Exception as e:
#         _debug_write("process_failed", key, "Exception during processing", {"error": str(e), "trace": traceback.format_exc()})

# def handler(event, context):
#     recs = event.get("Records", []) if event else []
#     _debug_write("handler_start", f"records:{len(recs)}", "Start handling")
#     for rec in recs:
#         bucket = rec["s3"]["bucket"]["name"]
#         obj_key = rec["s3"]["object"]["key"]
#         if bucket != S3_BUCKET:
#             _debug_write("skip_foreign_bucket", obj_key, f"event bucket={bucket}, expected={S3_BUCKET}")
#             continue
#         _process_object(obj_key)
#     _debug_write("handler_done", f"records:{len(recs)}", "Done")
#     return {"ok": True, "processed": len(recs)}

# extractor/extractor.py
import os
import re
from datetime import datetime, timezone
import boto3

from config import ENDPOINT_URL, AWS_REGION, S3_BUCKET, SUPPORTED_TYPES
from xml_normalizer import parse_gz_xml_bytes, build_payload
from sqs_producer import send_json
from db_tracker import put_last_run
from log_utils import log, log_exception
from s3_writer import write_json_to_s3   # <-- DIRECT JSON WRITE

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=AWS_REGION)

KEY_RE = re.compile(
    r"^providers/(?P<provider>[^/]+)/(?P<branch>[^/]+)/(?P<filetype>pricesFull|promoFull)_(?P<timestamp>\d{14})\.gz$"
)

def _parse_key(key: str):
    m = KEY_RE.match(key)
    if not m:
        return None
    gd = m.groupdict()
    ts_dt = datetime.strptime(gd["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return gd["provider"], gd["branch"], gd["filetype"], ts_dt

def _process_object(key: str):
    parsed = _parse_key(key)
    if not parsed:
        log("skip_non_matching_key", key=key)
        return
    provider, branch, file_type, ts_dt = parsed
    if file_type not in SUPPORTED_TYPES:
        log("skip_unsupported_type", key=key, file_type=file_type)
        return

    try:
        log("s3_get_object_start", bucket=S3_BUCKET, key=key)
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        log("s3_get_object_ok", size=len(body))

        root = parse_gz_xml_bytes(body)
        payload = build_payload(file_type, root, provider, branch, ts_dt)
        items = payload.get("items", [])
        log("payload_built", provider=provider, branch=branch, type=file_type, timestamp=payload["timestamp"], items=len(items))

        # --- send to SQS as before
        try:
            send_json(payload)
            log("sqs_send_ok", items=len(items))
        except Exception as e:
            log_exception("sqs_send_failed", e)

        # --- DIRECT JSON WRITE (always attempt; harmless if duplicate)
        try:
            write_json_to_s3(payload)
            log("direct_json_write_ok", provider=provider, branch=branch, type=file_type)
        except Exception as e:
            log_exception("direct_json_write_failed", e)

        put_last_run(provider, branch, file_type, payload["timestamp"])
        log("last_run_saved", pk=f"{provider}#{branch}#{file_type}", ts=payload["timestamp"])

        log("processed_ok", key=key, items=len(items))
    except Exception as e:
        log_exception("process_failed", e, key=key)

def handler(event, context):
    mode = "s3-event" if event and "Records" in event else "unknown"
    log("handler_start", mode=mode)
    recs = event.get("Records", []) if event else []
    for rec in recs:
        bucket = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]
        if bucket != S3_BUCKET:
            log("skip_foreign_bucket", event_bucket=bucket, configured=S3_BUCKET)
            continue
        _process_object(key)
    log("handler_done", processed=len(recs))
    return {"ok": True, "processed": len(recs)}
