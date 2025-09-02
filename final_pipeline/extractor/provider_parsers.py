from typing import Iterable, Dict, Optional
import gzip, io, json

def parse_ndjson_gz(data: bytes) -> Iterable[Dict]:
    with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as gz:
        for line in gz:
            try:
                yield json.loads(line.decode("utf-8"))
            except Exception:
                continue

def parse_by_filename(key: str, data: bytes) -> Optional[Iterable[Dict]]:
    """
    Hook for provider-specific formats (e.g., CPFTA XML inside .gz).
    Return an iterator of product dicts, or None if unsupported.
    """
    # TODO: implement parsers:
    # if "goodpharm" in key.lower():
    #     return parse_goodpharm(data)
    # if "carrefour" in key.lower() and key.lower().endswith(".gz"):
    #     return parse_carrefour(data)
    return None
