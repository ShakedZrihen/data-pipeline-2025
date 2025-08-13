# state_store.py
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
ddb = boto3.resource("dynamodb")

def _pk(meta: Dict[str, str]) -> str:
    return f"{meta['provider']}#{meta['branch']}#{meta['type']}"

def already_processed(table_name: str, meta: Dict[str, str], key: str, etag: Optional[str]) -> bool:
    table = ddb.Table(table_name)
    try:
        res = table.get_item(Key={"pk": _pk(meta)})
        item = res.get("Item")
        if not item: return False
        return (item.get("last_s3_key") == key) and (etag and item.get("last_s3_etag") == etag)
    except ClientError as e:
        logger.warning("DynamoDB get_item failed: %s", e)
        return False

def update_last_processed(table_name: str, meta: Dict[str, str], key: str, etag: Optional[str]) -> None:
    table = ddb.Table(table_name)
    try:
        table.put_item(
            Item={
                "pk": _pk(meta),
                "last_timestamp": meta["timestamp"],
                "last_s3_key": key,
                "last_s3_etag": etag or "",
                "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )
    except ClientError as e:
        logger.warning("DynamoDB put_item failed: %s", e)
