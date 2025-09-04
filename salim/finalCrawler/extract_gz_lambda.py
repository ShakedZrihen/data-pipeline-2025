import os, io, gzip, boto3, traceback
s3 = boto3.client("s3")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/")

def handler(event, context):
    print("EVENT:", event) 
    for rec in event.get("Records", []):
        b = rec["s3"]["bucket"]["name"]
        k = rec["s3"]["object"]["key"]
        try:
            if not k.lower().endswith(".gz"):
                print(f"skip non-gz: {k}")
                continue

            print(f"downloading s3://{b}/{k}")
            obj = s3.get_object(Bucket=b, Key=k)
            gz_bytes = obj["Body"].read()

            with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes)) as gz:
                xml_bytes = gz.read()

            base = k[:-3]
            if not base.lower().endswith(".xml"):
                base += ".xml"
            out_key = f"{RAW_PREFIX}{base}"

            print(f"uploading s3://{b}/{out_key}  ({len(xml_bytes)} bytes)")
            s3.put_object(Bucket=b, Key=out_key, Body=xml_bytes, ContentType="application/xml")
        except Exception as e:
            print("ERROR:", e)
            traceback.print_exc()
    return {"ok": True}
