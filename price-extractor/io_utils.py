import boto3, gzip, zipfile, io, logging

s3 = boto3.client('s3')

GZIP_MAGIC = b'\x1f\x8b'
ZIP_MAGIC = b'PK'

def get_object_bytes(bucket: str, key: str) -> bytes:
    r = s3.get_object(Bucket=bucket, Key=key)
    return r['Body'].read()

def inflate_bytes(blob: bytes) -> bytes:
    """Return inner bytes if ZIP/GZIP; else raw."""
    try:
        if blob.startswith(GZIP_MAGIC):
            with gzip.GzipFile(fileobj=io.BytesIO(blob)) as gz:
                return gz.read()
        if blob.startswith(ZIP_MAGIC):
            with zipfile.ZipFile(io.BytesIO(blob)) as zf:
                names = zf.namelist()
                if not names:
                    return blob
                # Prefer XML-ish entries
                xml_candidates = [n for n in names if n.lower().endswith((".xml", ".txt"))]
                name = xml_candidates[0] if xml_candidates else names[0]
                return zf.read(name)
        return blob
    except Exception:
        logging.exception("Inflation failed")
        raise

def read_and_decompress_gz(bucket: str, key: str) -> bytes:
    """Kept for backward-compat with prior imports."""
    return inflate_bytes(get_object_bytes(bucket, key))
