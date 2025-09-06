#!/usr/bin/env python3
# -*- coding: utf-8 -*-

\
\
\
\
\
\
\
\
\
\
   

import argparse
import os
import sys
from datetime import datetime, timezone

import boto3

                                  
from crawler import UniversalSupermarketCrawler


def pick_latest_stores_key(s3, bucket: str, provider: str) -> str | None:
    prefix = f"providers/{provider}/stores/"
    try:
        paginator = s3.get_paginator('list_objects_v2')
        latest = None
        latest_ts = None
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []) or []:
                key = obj['Key']
                ts = obj.get('LastModified')
                if not latest_ts or (ts and ts > latest_ts):
                    latest, latest_ts = key, ts
        return latest
    except Exception as e:
        print(f"Error listing S3: {e}")
        return None


def main():
    ap = argparse.ArgumentParser(description="Inspect latest Stores file for a provider")
    ap.add_argument('--provider', required=True, help='Provider name (e.g., victory, shufersal)')
    ap.add_argument('--bucket', default=os.getenv('S3_BUCKET'), help='S3 bucket (default from S3_BUCKET)')
    args = ap.parse_args()

    provider = args.provider.strip()
    bucket = (args.bucket or '').strip()
    if not bucket:
        print("Error: S3 bucket not provided (--bucket or S3_BUCKET).", file=sys.stderr)
        sys.exit(2)

    s3 = boto3.client('s3')
    key = pick_latest_stores_key(s3, bucket, provider)
    if not key:
        print(f"No stores files under s3://{bucket}/providers/{provider}/stores/")
        sys.exit(1)

    print(f"Latest stores key: s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read()
    if key.lower().endswith('.gz'):
        import gzip
        data = gzip.decompress(data)

                                                               
    crawler = UniversalSupermarketCrawler(bucket_name=bucket, config_file=None, local_mode=True)
    records = crawler._parse_stores_xml(data)                               
    if not records:
        print("Parsed 0 stores from XML. Please share a sample <Store> entry for tuning.")
        sys.exit(1)

    print(f"Parsed stores: {len(records)} (showing first 15)")
    limit = min(15, len(records))
    for i in range(limit):
        r = records[i]
        nm = (r.get('name') or '').strip()
        ad = (r.get('address') or '').strip()
        ct = (r.get('city') or '').strip()
        print(f"{i+1:>3}. name='{nm}' | address='{ad}' | city='{ct}'")

                                                                                       
    non_physical = []
    for i, r in enumerate(records, start=1):
        ad = (r.get('address') or '').strip().lower()
        ct = (r.get('city') or '').strip()
        if (not ct) or ad.startswith('http'):
            non_physical.append(i)
    if non_physical:
        print(f"\nLikely non-physical entries (by index): {non_physical[:15]}")
        print("Consider skipping these when mapping branch_<n> → store index.")


if __name__ == '__main__':
    main()

