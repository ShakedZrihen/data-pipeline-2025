import json

def lambda_handler(event, context):
    for rec in event.get("Records", []):
        body = rec.get("body")
        try:
            payload = json.loads(body)
        except Exception:
            payload = body
        print(" Received from SQS:", json.dumps(payload, ensure_ascii=False))
    return {"ok": True, "count": len(event.get("Records", []))}
