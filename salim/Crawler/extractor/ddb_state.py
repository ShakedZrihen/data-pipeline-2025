def upsert_last_run(ddb_client, table_name: str, provider: str, branch: str, file_type: str, timestamp: str):
    key = f"{provider}#{branch}#{file_type}"
    ddb_client.put_item(
        TableName=table_name,
        Item={
            "ProviderBranchType": {"S": key},
            "LastRunTimestamp": {"S": timestamp}
        }
    )
