import os, json, boto3, xml.etree.ElementTree as ET
import pg8000.native as pg
import re
from datetime import datetime

s3 = boto3.client("s3")

RAW_PREFIX  = os.environ.get("RAW_PREFIX", "raw/")
JSON_PREFIX = os.environ.get("JSON_PREFIX", "json/")
SQS_URL       = os.environ.get("SQS_URL")              
SQS_ENDPOINT  = os.environ.get("SQS_ENDPOINT")         
sqs = None

PGHOST=os.getenv("PGHOST"); 
PGPORT=int(os.getenv("PGPORT","5432"))
PGUSER=os.getenv("PGUSER"); 
PGPASSWORD=os.getenv("PGPASSWORD")
PGDATABASE=os.getenv("PGDATABASE")

if SQS_URL:
    sqs = boto3.client("sqs", endpoint_url=SQS_ENDPOINT) if SQS_ENDPOINT else boto3.client("sqs")

def _iso_from_compact(ts12: str) -> str:
    return datetime.strptime(ts12, "%Y%m%d%H%M").strftime("%Y-%m-%dT%H:%M:00Z")

def meta_from_key(key: str):
    parts = key.split("/")
    provider = parts[1] if len(parts) > 1 else "unknown"

    fname = parts[-1]
    m = re.search(r'(PriceFull|PromoFull)(\d+)-(\d+)-(\d{12})', fname, re.I)
    typ = "pricesFull" if (m and m.group(1).lower().startswith("price")) else ("promoFull" if m else "unknown")
    branch = m.group(3) if m else "unknown"
    ts_iso = _iso_from_compact(m.group(4)) if m else datetime.utcnow().strftime("%Y-%m-%dT%H:%M:00Z")
    return provider, branch, typ, ts_iso


def upsert_last_run_pg(provider:str, branch:str, typ:str, ts_iso:str):
    conn = pg.Connection(user=PGUSER, password=PGPASSWORD,
                         host=PGHOST, database=PGDATABASE, port=PGPORT)
    conn.run("""
      INSERT INTO last_run(provider,branch,type,last_ts)
      VALUES (:p,:b,:t,:ts)
      ON CONFLICT (provider,branch,type)
      DO UPDATE SET last_ts = GREATEST(last_run.last_ts, EXCLUDED.last_ts)
    """, p=provider, b=branch, t=typ, ts=ts_iso)

def elem_to_dict(elem):
    d = {}
    if elem.attrib:
        d["@attrs"] = dict(elem.attrib)
    children = list(elem)
    if children:
        groups = {}
        for c in children:
            groups.setdefault(c.tag, []).append(elem_to_dict(c))
        d.update({k: (v if len(v) > 1 else v[0]) for k, v in groups.items()})
    text = (elem.text or "").strip()
    if text:
        if d:
            d["#text"] = text
        else:
            return text
    return d

def handler(event, context):
    for rec in event.get("Records", []):
        b = rec["s3"]["bucket"]["name"]
        k = rec["s3"]["object"]["key"]

        if not k.lower().endswith(".xml"):
            continue
        if not k.startswith(RAW_PREFIX):
            continue

        obj = s3.get_object(Bucket=b, Key=k)
        xml_bytes = obj["Body"].read()

        root = ET.fromstring(xml_bytes)
        data = {root.tag: elem_to_dict(root)}
        json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")

        rel = k[len(RAW_PREFIX):]
        if rel.lower().endswith(".xml"):
            rel = rel[:-4] + ".json"
        out_key = f"{JSON_PREFIX}{rel}"

        s3.put_object(Bucket=b, Key=out_key, Body=json_bytes, ContentType="application/json")

        if sqs and SQS_URL:
            msg = {"bucket": b, "key": out_key}
            sqs.send_message(QueueUrl=SQS_URL, MessageBody=json.dumps(msg, ensure_ascii=False))
            try:
                provider, branch, typ, ts_iso = meta_from_key(k)
                if PGHOST and PGUSER and PGPASSWORD and PGDATABASE:
                    upsert_last_run_pg(provider, branch, typ, ts_iso)
            except Exception as e:
                print("PG upsert failed:", repr(e))
    return {"ok": True}
