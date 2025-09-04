import os, json, logging, boto3
from botocore.config import Config
log = logging.getLogger(__name__)

def _sqs_client():
    region = os.getenv("AWS_REGION", "us-east-1")
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    cfg = Config(retries={'max_attempts': 3, 'mode': 'standard'})
    return boto3.client("sqs", region_name=region, endpoint_url=endpoint, config=cfg)
def send_message(payload, outbox_path=None):
    queue_url = os.getenv("OUTPUT_QUEUE_URL")
    if not queue_url:
        raise ValueError("Missing OUTPUT_QUEUE_URL")

    sqs = boto3.client("sqs", endpoint_url=os.getenv("AWS_ENDPOINT_URL"), region_name=os.getenv("AWS_REGION"))

    payload["items_sample"] = payload.get("items", [])[:10]
    payload["items_total"] = len(payload.get("items", []))
    if outbox_path:
        payload["outbox_path"] = outbox_path

        # ✅ שורה חשובה – שומרת את הקובץ על הדיסק!
        with open("items_sample.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    message_body = json.dumps(payload, ensure_ascii=False)
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body
    )


def _sqs():
    region = os.getenv("AWS_REGION", "us-east-1")
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    cfg = Config(retries={'max_attempts': 3, 'mode': 'standard'})
    return boto3.client("sqs", region_name=region, endpoint_url=endpoint, config=cfg)

def _json_size_bytes(obj) -> int:
    """חישוב גודל JSON בבייטים עם תמיכה נכונה בעברית"""
    return len(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))

def _shrink_to_bytes(obj: dict, max_bytes: int) -> dict:
    """
    קיצוץ אובייקט ל-SQS עם שמירה על עברית תקינה
    """
    if _json_size_bytes(obj) <= max_bytes:
        return obj

    items = list(obj.get("items_sample", []))
    trunc = 160
    while items:
        for it in items:
            name = it.get("product", "")
            if isinstance(name, str) and len(name) > trunc:
                # וידוא שהחיתוך לא יפגע באמצע תו עברי
                truncated = name[:trunc]
                # אם התו האחרון הוא תו עברי חלקי, נחתוך עוד אחד
                if truncated and ord(truncated[-1]) >= 0x0590 and ord(truncated[-1]) <= 0x05FF:
                    # מצא את הגבול הבטוח לחיתוך
                    safe_end = trunc
                    while safe_end > 0 and ord(name[safe_end - 1]) >= 0x0590:
                        safe_end -= 1
                    if safe_end > 0:
                        truncated = name[:safe_end]
                it["product"] = truncated + "…"

        tmp = dict(obj, items_sample=items)
        if _json_size_bytes(tmp) <= max_bytes:
            return tmp

        new_len = max(10, int(len(items) * 0.9))
        if new_len == len(items):
            new_len = len(items) - 1
        items = items[:new_len]

        if trunc > 60:
            trunc = int(trunc * 0.9)

    # במקרה קיצוני - נוקה את הרשימה
    obj["items_sample"] = []
    return obj
