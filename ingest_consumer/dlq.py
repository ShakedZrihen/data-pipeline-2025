import os, json, boto3, traceback

_sqs = None
def _sqs_client():
    global _sqs
    if _sqs:
        return _sqs
    _sqs = boto3.client("sqs", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
    return _sqs

def _safe_json_loads(s: str):
    try:
        return json.loads(s)
    except Exception:
        return s

def send_to_dlq(message_body, error: Exception, queue_name: str = "extractor-results"):
    url = os.getenv("SQS_DLQ_URL")
    if not url:
        return

    if isinstance(message_body, str):
        original = _safe_json_loads(message_body)
    else:
        original = message_body

    body = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        "traceback": traceback.format_exc(),
        "source_queue": queue_name,
        "original_body": original,
    }
    _sqs_client().send_message(
        QueueUrl=url,
        MessageBody=json.dumps(body, ensure_ascii=False)
    )
