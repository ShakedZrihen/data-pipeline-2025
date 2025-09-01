# extractor/db_state.py
import os
from datetime import datetime, timezone
import boto3

class LastRunStore:
    def __init__(self, table_name: str | None = None, region: str | None = None):
        self.table = boto3.resource("dynamodb", region_name=region or os.environ.get("AWS_REGION", "us-east-1")) \
                         .Table(table_name or os.environ["DDB_TABLE"])

    @staticmethod
    def pk(provider: str, branch: str, typ: str) -> str:
        return f"{provider}#{branch}#{typ}"

    def update(self, provider: str, branch: str, typ: str, ts_iso: str, s3_key: str):
        self.table.put_item(Item={
            "pk": self.pk(provider, branch, typ),
            "last_ts": ts_iso,
            "last_key": s3_key,
            "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        })
