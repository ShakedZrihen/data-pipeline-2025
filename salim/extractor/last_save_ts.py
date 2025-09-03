# extractor/last_run_store.py
from __future__ import annotations
import os
import time
from datetime import datetime, timezone
from typing import Optional, Iterable

import boto3
from botocore.exceptions import ClientError

ISO_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_FMT)

def _to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime(ISO_FMT)

def _from_iso(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)

class LastRunStore:
    """
    Saves & loads the last *successful* extraction time per (provider, branch, job).
    """

    def __init__(
        self,
        table_name: str | None = None,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        create_if_missing: bool = True,
        wait_for_active: bool = True,
    ):
        self.table_name = table_name or os.getenv("DDB_TABLE_NAME", "pipeline_runs")
        self.endpoint_url = endpoint_url or os.getenv("DDB_ENDPOINT")
        self.region_name = region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")

        print(f"[BOOT][DDB] connecting… table={self.table_name}, endpoint={self.endpoint_url or 'default'}, region={self.region_name}")

        self._dynamodb = boto3.resource(
            "dynamodb",
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        )

        self._table = self._get_or_create_table(create_if_missing, wait_for_active)
        print(f"[BOOT][DDB] table ready: {self.table_name}")

    def get_last_success(
        self,
        provider: str,
        branch: str,
        job: str = "extractor",
    ) -> Optional[datetime]:
        """Return the last successful extraction time (UTC) or None."""
        pk = self._pk(provider, branch)
        sk = job
        try:
            resp = self._table.get_item(Key={"pk": pk, "sk": sk})
            item = resp.get("Item")
            if not item:
                print(f"[INFO][DDB] no last_success found for {pk}/{sk}")
                return None
            ts = item.get("last_success_at")
            if not ts:
                return None
            dt = _from_iso(ts)
            print(f"[INFO][DDB] last_success for {pk}/{sk} → {dt.isoformat()}")
            return dt
        except ClientError as e:
            print(f"[ERROR][DDB] get_last_success failed: {e}")
            return None

    def set_success(
        self,
        provider: str,
        branch: str,
        when: Optional[datetime] = None,
        job: str = "extractor",
        meta: Optional[dict] = None,
    ) -> bool:
        """
        Save a successful run at time 'when' (default now).
        Will only update if new time is strictly greater than existing (monotonic forward).
        Returns True if updated, False if skipped by condition.
        """
        pk = self._pk(provider, branch)
        sk = job
        iso = _to_iso(when or datetime.now(timezone.utc))
        meta = meta or {}

        print(f"[INFO][DDB] set_success {pk}/{sk} → {iso}")
        try:
            self._table.update_item(
                Key={"pk": pk, "sk": sk},
                UpdateExpression="SET last_success_at = :ts, updated_at = :now, meta = :meta",
                ConditionExpression="attribute_not_exists(last_success_at) OR last_success_at < :ts",
                ExpressionAttributeValues={
                    ":ts": iso,
                    ":now": _utc_now_iso(),
                    ":meta": meta,
                },
            )
            print("[RESULT][DDB] success timestamp advanced")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ConditionalCheckFailedException",):
                print("[RESULT][DDB] skipped (existing timestamp is newer or equal)")
                return False
            print(f"[ERROR][DDB] set_success failed: {e}")
            return False

    def set_failure(
        self,
        provider: str,
        branch: str,
        error_msg: str,
        job: str = "extractor",
    ) -> None:
        """Optional: record last failure time/message (does not affect last_success_at)."""
        pk = self._pk(provider, branch)
        sk = job
        now = _utc_now_iso()
        print(f"[WARN][DDB] set_failure {pk}/{sk} → {now} :: {error_msg[:200]}")
        try:
            self._table.update_item(
                Key={"pk": pk, "sk": sk},
                UpdateExpression="SET last_failure_at = :now, last_error = :err, updated_at = :now",
                ExpressionAttributeValues={
                    ":now": now,
                    ":err": error_msg[:1000],
                },
            )
        except ClientError as e:
            print(f"[ERROR][DDB] set_failure failed: {e}")

    @staticmethod
    def filter_s3_objects_since(
        objects: Iterable[dict],
        since: Optional[datetime],
    ) -> list[dict]:
        """
        Given an iterable of S3 objects (each with 'Key' and 'LastModified'),
        return only those strictly newer than 'since'.
        """
        if not since:
            return list(objects)
        since_utc = since.astimezone(timezone.utc)
        out = [o for o in objects if o.get("LastModified") and o["LastModified"].astimezone(timezone.utc) > since_utc]
        return out

    def _pk(self, provider: str, branch: str) -> str:
        return f"extractor#{provider}#{branch}"

    def _get_or_create_table(self, create_if_missing: bool, wait_for_active: bool):
        try:
            table = self._dynamodb.Table(self.table_name)
            table.load()
            return table
        except self._dynamodb.meta.client.exceptions.ResourceNotFoundException:
            if not create_if_missing:
                raise

        print(f"[BOOT][DDB] creating table {self.table_name}…")
        table = self._dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        if wait_for_active:
            while True:
                table.reload()
                status = table.table_status
                print(f"[BOOT][DDB] waiting for ACTIVE… ({status})")
                if status == "ACTIVE":
                    break
                time.sleep(0.8)
        return table
