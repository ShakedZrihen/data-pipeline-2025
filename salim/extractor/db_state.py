import boto3
from datetime import datetime, timezone

class LastRunStore:
    def __init__(self, table_name: str | None = None, region: str | None = None, endpoint_url: str | None = None):
        region = region 
        endpoint_url = endpoint_url  
        res_kwargs = {"region_name": region}
        if endpoint_url:
            res_kwargs["endpoint_url"] = endpoint_url                      
        self.table = boto3.resource("dynamodb", **res_kwargs).Table(table_name)
    @staticmethod
    def pk(provider: str, branch: str, typ: str) -> str:
        return f"{provider}#{branch}#{typ}"
    def update(self, provider: str, branch: str, typ: str, ts_iso: str, s3_key: str):

        pk = self.pk(provider, branch, typ)
        print("Updating DynamoDB:", pk)
        self.table.put_item(Item={
            "pk": pk,
            "last_ts": ts_iso,
            "last_key": s3_key,
            "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        })
        print("DynamoDB update complete:", pk)
