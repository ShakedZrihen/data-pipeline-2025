from typing import Optional
import boto3
import os
import json
import uuid
from typing import Any, Tuple 
from botocore.exceptions import ClientError
from consts import *

def make_sqs_client():
    return boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

def ensure_queue_and_get_url(sqs_client, queue_name: str) -> str:
    try:
        return sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except sqs_client.exceptions.QueueDoesNotExist:
        attrs = {}
        if queue_name.endswith(".fifo"):
            attrs["FifoQueue"] = "true"
            attrs["ContentBasedDeduplication"] = "true"
        resp = sqs_client.create_queue(QueueName=queue_name, Attributes=attrs)
        return resp["QueueUrl"]


def iter_sqs_batches(
    queue_name: str = "test-queue",
    endpoint_url: str = "http://localhost:4567",
    region_name: str = "us-east-1",
    aws_access_key_id: str = "test",
    aws_secret_access_key: str = "test",
    wait_time_seconds: int = 5,
    max_number_per_poll: int = 10,
    max_empty_polls: int = 1
):
    sqs_client = boto3.client(
        "sqs",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )
    queue_url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]

    empty_polls = 0
    while True:
        try:
            resp = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_number_per_poll,
                WaitTimeSeconds=wait_time_seconds,
                MessageAttributeNames=["All"],
                AttributeNames=["All"],
            )
        except ClientError as e:
            print(f"[SQS] receive error: {e}")
            break

        batch = resp.get("Messages", []) or []
        if not batch:
            empty_polls += 1
            if empty_polls >= max_empty_polls:
                break
            continue

        yield batch

def write_temp_json(doc: dict, kind: str, temp_root: str) -> str:
    os.makedirs(temp_root, exist_ok=True)
    path = os.path.join(temp_root, f"{kind}_chunk_{uuid.uuid4().hex}.json")
    with open(path, "w", encoding="utf-8") as out:
        json.dump(doc, out, ensure_ascii=False)
    return path

def load_doc_from_any(body: str) -> Tuple[Optional[dict], Optional[str], Optional[str]]:
    s = body.strip().strip('"').strip("'")

    if os.path.exists(s) and s.lower().endswith(".json"):
        try:
            with open(s, "r", encoding="utf-8") as f:
                parsed = json.load(f)
            return coerce_to_object(parsed), guess_kind_from_any(parsed, s), s
        except Exception:
            pass

    try:
        parsed = json.loads(s)
        return coerce_to_object(parsed), guess_kind_from_any(parsed, s), None
    except Exception:
        return None, None, None

def coerce_to_object(parsed: Any) -> dict:
    if isinstance(parsed, dict):
        return parsed
    if isinstance(parsed, list):
        return {"items": parsed}
    return {"data": parsed}

def guess_kind_from_any(data: Any, origin_hint: Optional[str] = None) -> Optional[str]:
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

def auto_find_stores_dir() -> Optional[str]:
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