import io, gzip, json, datetime as dt
from typing import Dict, Iterable, Optional
from shared.s3 import upload_bytes

class PricesNDJSONEmitter:
    """
    Usage:
        with PricesNDJSONEmitter(prefix="prices/") as emit:
            emit.write(product_dict)
    At exit it uploads a single gzipped NDJSON to S3.
    """
    def __init__(self, prefix: str = "prices/") -> None:
        now = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.key = f"{prefix}prices_{now}.ndjson.gz"
        self._buf = io.BytesIO()
        self._gz = gzip.GzipFile(fileobj=self._buf, mode="wb")

    def write(self, item: Dict) -> None:
        self._gz.write((json.dumps(item, ensure_ascii=False) + "\n").encode("utf-8"))

    def close(self) -> None:
        if self._gz:
            self._gz.close()
            data = self._buf.getvalue()
            upload_bytes(self.key, data, content_type="application/gzip")
            self._gz = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
