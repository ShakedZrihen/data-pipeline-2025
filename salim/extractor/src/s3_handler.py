

from __future__ import annotations

import gzip
import io
import json
import logging
import re
from typing import Optional, Tuple
from urllib.parse import unquote_plus, unquote

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class S3Handler:
    """
    Small helper around boto3 S3 client for:
      - decoding and parsing S3 event keys in the exercise's format
      - opening a streaming gzip reader from S3
      - uploading small JSON/text artifacts back to S3

    Expected input object keys (URL-encoded by S3 event):
      providers/<provider>/<branch>/(pricesFull|promoFull)_<timestamp>[_bN[_...]].gz

    Returns from parse_s3_key():
      (provider, branch, file_type, timestamp_str, chunk_suffix)
    where chunk_suffix is something like "_b001" or None.
    """

    KEY_RE = re.compile(
        r"^providers/"
        r"(?P<provider>[^/]+)/"
        r"(?P<branch>[^/]+)/"
        r"(?P<type>pricesFull|promoFull)_"
        r"(?P<ts>[0-9T:\-\.]+Z?)"
        r"(?P<chunk>_b[0-9]+(?:_[^/]+)?)?"
        r"\.gz$"
    )

    def __init__(self, s3_client=None):
        self.s3 = s3_client or boto3.client("s3")

  

    @staticmethod
    def decoded_key(raw_key: str) -> str:
        """
        S3 event keys are URL-encoded and often use '+' for spaces.
        This mirrors the previous behavior used elsewhere in the project.
        (Note: open_gzip_stream is more defensive and will try multiple variants.)
        """
        return unquote_plus(raw_key)

    def parse_s3_key(self, raw_key: str) -> Tuple[str, str, str, str, Optional[str]]:
        """
        Decode and parse the event key into components.
        Raises ValueError if the pattern does not match.
        """
        k = self.decoded_key(raw_key)
        m = self.KEY_RE.match(k)
        if not m:
            raise ValueError(f"Object key does not match expected pattern: {k!r}")
        provider = m.group("provider")
        branch = m.group("branch")
        file_type = m.group("type")
        ts_str = m.group("ts")
        chunk = m.group("chunk")
        return provider, branch, file_type, ts_str, chunk

 

    def open_gzip_stream(self, bucket: str, raw_key: str) -> gzip.GzipFile:
        """
        Open a gzip stream from S3 using resilient key handling.
        We try multiple decoding strategies because real keys can contain
        literal '+' (not spaces) and some SDKs provide pre-decoded keys.
        If all attempts fail with NoSuchKey, we emit nearby keys to logs.
        """
        candidates = []
        def _add(k: str):
            if k not in candidates:
                candidates.append(k)

       
        _add(self.decoded_key(raw_key))
        
        _add(raw_key)
        
        _add(unquote(raw_key))

        last_err: Optional[Exception] = None
        for key in candidates:
            try:
                obj = self.s3.get_object(Bucket=bucket, Key=key)
                body = obj["Body"]  
                
                return gzip.GzipFile(fileobj=io.BufferedReader(body), mode="rb")
            except ClientError as e:
                code = (e.response.get("Error") or {}).get("Code")
                if code in ("NoSuchKey", "404"):
                    last_err = e
                    continue  
                
                raise

        
        try:
            parent = candidates[0].rsplit("/", 1)[0] if "/" in candidates[0] else ""
            resp = self.s3.list_objects_v2(
                Bucket=bucket,
                Prefix=(parent + "/" if parent else ""),
                MaxKeys=50,
            )
            nearby = [c["Key"] for c in resp.get("Contents", [])]
            logger.warning(
                "NoSuchKey for any variant. Tried=%s. Nearby (first 50): %s",
                candidates, nearby
            )
        except Exception:
            
            pass

        if last_err:
            raise last_err
        raise RuntimeError("Unexpected: failed to open gzip stream and no error captured.")

 

    def upload_file(self, bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream"):
        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            CacheControl="no-cache",
        )
        logger.info("Uploading s3://%s/%s (%d bytes)", bucket, key, len(data))


    def upload_json(self, bucket: str, key: str, obj: Any) -> None:
        """Upload a small JSON object with proper content-type and UTF‑8 encoding."""
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.upload_file(bucket, key, body, content_type="application/json; charset=utf-8")

    def upload_text(self, bucket: str, key: str, text: str, content_type: str = "text/plain; charset=utf-8") -> None:
        self.upload_file(bucket, key, text.encode("utf-8"), content_type=content_type)
