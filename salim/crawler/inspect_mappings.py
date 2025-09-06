
   

import argparse
import os
import sys
import json
import boto3


def list_provider_mapping_keys(s3, bucket: str):
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix='mappings/'):
        for obj in page.get('Contents', []) or []:
            k = obj['Key']
            if k.endswith('_stores.json') and '/debug/' not in k:
                keys.append(k)
    return keys


def fetch_mapping(s3, bucket: str, key: str):
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read()
    return json.loads(data.decode('utf-8'))


def preview_provider(provider: str, mapping: dict, limit: int = 10):
    stores = mapping.get('stores') if isinstance(mapping, dict) else None
    print(f"\nProvider: {provider}")
    if not isinstance(stores, list):
        print("  stores: <missing or not a list>")
        return
    print(f"  stores count: {len(stores)}")
    take = min(limit, len(stores))
    for i in range(take):
        st = stores[i] if isinstance(stores[i], dict) else {}
        name = (st.get('name') or '').strip()
        addr = (st.get('address') or '').strip()
        city = (st.get('city') or '').strip()
        print(f"  {i+1:>3}. name='{name}' | city='{city}' | address='{addr}'")


def main():
    ap = argparse.ArgumentParser(description='Inspect provider mappings in S3')
    ap.add_argument('--bucket', default=os.getenv('S3_BUCKET'), help='S3 bucket name')
    ap.add_argument('--providers', nargs='*', help='Specific providers to inspect (default: all found)')
    ap.add_argument('--limit', type=int, default=10, help='How many stores to preview per provider')
    args = ap.parse_args()

    if not args.bucket:
        print('Error: --bucket or S3_BUCKET must be set', file=sys.stderr)
        sys.exit(2)

    s3 = boto3.client('s3')
    keys = list_provider_mapping_keys(s3, args.bucket)
    if not keys:
        print(f'No provider mappings found under s3://{args.bucket}/mappings/')
        sys.exit(1)

    targets = []
    wanted = set((args.providers or []))
    for k in keys:
                                         
        base = os.path.basename(k)
        if not base.endswith('_stores.json'):
            continue
        provider = base[:-len('_stores.json')]
        if wanted and provider not in wanted:
            continue
        targets.append((provider, k))

    if not targets:
        print('No matching provider mappings found.')
        sys.exit(1)

    for provider, key in sorted(targets):
        try:
            mapping = fetch_mapping(s3, args.bucket, key)
            preview_provider(provider, mapping, limit=args.limit)
        except Exception as e:
            print(f"\nProvider: {provider}\n  Error fetching/parsing {key}: {e}")


if __name__ == '__main__':
    main()

