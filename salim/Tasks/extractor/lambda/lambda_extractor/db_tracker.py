# extractor/db_tracker.py
import boto3
from botocore.exceptions import ClientError
from config import ENDPOINT_URL, AWS_REGION, DDB_TABLE

def _ddb():
    return boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL, region_name=AWS_REGION)

def ensure_table():
    ddb = _ddb()
    try:
        ddb.create_table(
            TableName=DDB_TABLE,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.Table(DDB_TABLE).wait_until_exists()
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise

def put_last_run(provider, branch, file_type, ts_str):
    ensure_table()
    _ddb().Table(DDB_TABLE).put_item(
        Item={"pk": f"{provider}#{branch}#{file_type}", "last_run": ts_str}
    )
