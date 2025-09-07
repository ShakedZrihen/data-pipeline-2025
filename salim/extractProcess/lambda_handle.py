import json
from .extract import process_s3_object_to_json
from .queue_rabbit import emit_event_full_json
from .state_mongo import update_last_run
from .s3io import s3


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
            try:
                s3.delete_object(Bucket=bucket, Key=key)
                print(f"[S3] Deleted from bucket after processing: {key}")
            except Exception as de:
                print(f"[S3] Failed to delete {key}: {de}")

        except Exception as e:
            print(f"Error processing {key}: {e}")
            outputs.append({"key": key, "ok": False, "reason": str(e)})

    return {'statusCode': 200, 'body': json.dumps(outputs, ensure_ascii=False)}
