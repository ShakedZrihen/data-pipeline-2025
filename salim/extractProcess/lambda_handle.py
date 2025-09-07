import json
from .extract import process_s3_object_to_json
from .queue_rabbit import emit_event_full_json
from .state_mongo import update_last_run

def lambda_handler(event, context=None):
    if 'Records' not in event:
        return {'statusCode': 400, 'body': json.dumps("No Records key")}
    outputs = []
    for rec in event['Records']:
        bucket = rec['s3']['bucket']['name']
        key = rec['s3']['object']['key']
        try:
            doc = process_s3_object_to_json(bucket, key)
            if not doc:
                outputs.append({"key": key, "ok": False, "reason": "invalid format"})
                continue

            try:
                emit_event_full_json(doc)
            except Exception as e:
                print(f"[Queue] Failed to emit event for {key}: {e}")

            try:
                update_last_run(
                    doc.get("provider") or "",
                    doc.get("branch") or "",
                    doc.get("type") or "",
                    doc.get("timestamp") or "",
                )
            except Exception as e:
                print(f"[State] Failed to update last_run for {key}: {e}")

            print(f"Processed {key}")
        except Exception as e:
            print(f"Error processing {key}: {e}")
            outputs.append({"key": key, "ok": False, "reason": str(e)})

    # cause i want to debug
    total = len(outputs)
    success = sum(1 for o in outputs if o.get("ok"))
    failed = total - success
    print(f"[SUMMARY] Lambda finished: total={total}, success={success}, failed={failed}")

    return {'statusCode': 200, 'body': json.dumps(outputs, ensure_ascii=False)}
