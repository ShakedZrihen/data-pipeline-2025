
# --- PATCHED BY ASSISTANT (AI-enriched v3 + pg8000/psycopg2 fallback) ---
import os, json, logging, base64, gzip, urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
from pg_resilient import from_env as _pg_from_env__probe  # optional import for type hints

# Try pg8000 first, fall back to psycopg2
_db_mode = "pg8000"
try:
    import pg8000
    _has_pg8000 = True
except Exception:
    _has_pg8000 = False
    _db_mode = "psycopg2"
    import psycopg2
    import psycopg2.extras

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("consumer")
if not logger.handlers:
    import sys
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    h.setFormatter(fmt)
    logger.addHandler(h)
logger.setLevel(LOG_LEVEL)

LOG_EMPTY_INFO = (os.getenv("LOG_EMPTY_INFO","false").lower() in ("1","true","yes","on"))
FILL_NULL_DISCOUNT = (os.getenv("FILL_NULL_DISCOUNT","true").lower() in ("1","true","yes","on"))

# ----------------- Utils -----------------

def _json_loads_maybe_base64(s: str):
    try: return json.loads(s)
    except Exception:
        try:
            raw = base64.b64decode(s)
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None

def _unwrap_sns_envelope(obj):
    if isinstance(obj, dict) and "Message" in obj and isinstance(obj["Message"], str):
        inner = _json_loads_maybe_base64(obj["Message"])
        return inner if inner is not None else obj
    return obj

def _coerce_number(x):
    if x is None: return None
    if isinstance(x, (int, float, Decimal)):
        try: return Decimal(str(x))
        except Exception: return None
    if isinstance(x, str):
        s = x.strip().replace(",", "")
        try: return Decimal(s)
        except Exception: return None
    return None

def _iso_dt(x):
    if x is None: return None
    if isinstance(x, (int, float)):
        val = float(x)
        if val > 1e12: val /= 1000.0
        try: return datetime.fromtimestamp(val, tz=timezone.utc)
        except Exception: return None
    if isinstance(x, str):
        s = x.strip()
        fmts = ("%Y-%m-%dT%H:%M:%S.%fZ","%Y-%m-%dT%H:%M:%SZ","%Y-%m-%d %H:%M:%S","%Y-%m-%d")
        for fmt in fmts:
            try:
                dt = datetime.strptime(s, fmt)
                if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue
        try: return datetime.fromisoformat(s.replace("Z","+00:00"))
        except Exception: return None
    return None

def _ensure_str(x, default="Unknown"):
    if x is None: return default
    if isinstance(x, str):
        s = x.strip()
        return s if s else default
    return str(x)

def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    return (v or "").strip().lower() in ("1","true","yes","y","on") if v is not None else default

# ----------------- OpenAI (JSON mode) + simple cache -----------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
_enrich_cache: Dict[tuple, Any] = {}

def _chat_json(prompt: str, max_tokens: int = 24):
    if not OPENAI_API_KEY:
        return None
    body = {
        "model": OPENAI_MODEL,
        "response_format": {"type":"json_object"},
        "messages": [
            {"role":"system","content":"Return ONLY strict JSON. If unsure, set fields to 'Unknown'. Never invent prices or numeric IDs."},
            {"role":"user","content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": max_tokens,
    }
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}","Content-Type":"application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            content = data["choices"][0]["message"]["content"]
            return json.loads(content)
    except Exception as e:
        logger.debug(f"OpenAI call failed: {e}")
        return None

def enrich_branch(provider: str, branch_name: str, city: Optional[str], address: Optional[str]):
    key = ("branch", provider, branch_name)
    if key in _enrich_cache: return _enrich_cache[key]
    need_city = not city or city == "Unknown"
    need_addr = not address or address == "Unknown"
    if not (need_city or need_addr):
        res = {"city": city or "Unknown", "address": address or "Unknown"}
        _enrich_cache[key] = res
        return res
    prompt = (
        f"Provider: {provider}\nBranch name: {branch_name}\n"
        "Return JSON {\"city\":\"...\",\"address\":\"...\"}. "
        "If unsure for a field, use \"Unknown\". City must be Israeli city/town."
    )
    out = _chat_json(prompt, max_tokens=40) or {}
    res = {"city": out.get("city") or city or "Unknown",
           "address": out.get("address") or address or "Unknown"}
    _enrich_cache[key] = res
    return res

def enrich_brand(product_name: str, current_brand: Optional[str]):
    if current_brand and current_brand != "Unknown": return current_brand
    key = ("brand", product_name)
    if key in _enrich_cache: return _enrich_cache[key]
    prompt = (
        f"Product: {product_name}\n"
        "Return JSON {\"brand\":\"...\"}. If unclear, set \"brand\":\"Unknown\"."
    )
    out = _chat_json(prompt, max_tokens=16) or {}
    brand = out.get("brand") or "Unknown"
    _enrich_cache[key] = brand
    return brand

# ----------------- Deterministic enrichment (no external services) -----------------
import re

def _canon_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("־", "-").replace("–", "-")
    s = re.sub(r"[^\w\u0590-\u05FF\- ]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def _canon_branch_key(provider: str, branch: str) -> str:
    return f"{_canon_text(provider)}|{_canon_text(branch)}"

# Seed mapping; extend over time or load from DB/S3 at cold start
BRANCH_MAP = {
    _canon_branch_key("victory", "תל אביב - אבן גבירול 100"): ("תל אביב-יפו", "אבן גבירול 100"),
    _canon_branch_key("yohananof", "תל אביב - יפו"): ("תל אביב-יפו", "דרך שלמה 90"),
}

def enrich_branch_deterministic(provider: str, branch_name: str) -> tuple[str, str]:
    key = _canon_branch_key(provider, branch_name)
    if key in BRANCH_MAP:
        return BRANCH_MAP[key]
    p, b = key.split("|", 1)
    for k, (city, addr) in BRANCH_MAP.items():
        kp, kb = k.split("|", 1)
        if kp == p and (kb.startswith(b) or b.startswith(kb) or kb in b or b in kb):
            return city, addr
    # Log once for missing mapping to help curation
    try:
        ck = _canon_branch_key(provider, branch_name)
        logger.info(f"Missing branch mapping for provider='{provider}' branch='{branch_name}' canon='{ck}'")
    except Exception:
        pass
    return "Unknown", "Unknown"

BRAND_ALIASES = {
    "תנובה": "תנובה",
    "שטראוס": "שטראוס",
    "טרה": "טרה",
    "אוסם": "אוסם",
    "עלית": "עלית",
    "קוקהקולה": "קוקה-קולה",
    "קוקקולה": "קוקה-קולה",
}

def _strip_non_alnum_hebrew(s: str) -> str:
    return re.sub(r"[^\w\u0590-\u05FF]+", "", s or "")

def extract_brand_and_clean_name(product_name: str) -> tuple[Optional[str], str]:
    raw = product_name or ""
    compact = _strip_non_alnum_hebrew(raw).lower()
    brand = None
    for alias in sorted(BRAND_ALIASES.keys(), key=len, reverse=True):
        if alias in compact:
            brand = BRAND_ALIASES[alias]
            break
    clean = raw
    if brand:
        patt = re.compile(rf"\b{re.escape(brand)}\b", re.IGNORECASE)
        clean = patt.sub("", clean)
        clean = re.sub(r"\s{2,}", " ", clean).strip()
    return (brand or None), (clean or raw).strip()

# Heuristic extractor for Hebrew product strings when aliases miss
HE_HEURISTIC_DESCRIPTORS = [
    "חלב", "יוגורט", "גבינה", "מעדן", "משקה", "שוקולד", "חטיף", "עוגיות",
    "טחינה", "מיונז", "מרגרינה", "קפה", "תה", "בקבוק", "מיץ", "בירה",
    "פסטה", "אורז", "סוכר", "מלח", "קמח",
]

def heuristic_brand_from_product(product_name: str) -> Optional[str]:
    if not product_name:
        return None
    s = str(product_name)
    # Normalize spaces; keep Hebrew letters and spaces
    s_compact = _strip_non_alnum_hebrew(s).lower()
    if not s_compact:
        return None
    # Try removing known descriptors from the start of the compact string
    for d in sorted(HE_HEURISTIC_DESCRIPTORS, key=len, reverse=True):
        dd = _strip_non_alnum_hebrew(d).lower()
        if dd and s_compact.startswith(dd) and len(s_compact) > len(dd):
            rest = s_compact[len(dd):]
            # take leading Hebrew letters from rest as brand candidate
            m = re.match(r"[\u0590-\u05FF]+", rest)
            if m and len(m.group(0)) >= 2:
                return m.group(0)
    # Fallback: take first Hebrew run before digits/percent
    m2 = re.match(r"[\u0590-\u05FF]+", s_compact)
    if m2 and len(m2.group(0)) >= 2:
        return m2.group(0)
    return None

from decimal import Decimal, InvalidOperation

def _to_dec(x):
    try:
        return Decimal(str(x)) if x is not None else None
    except (InvalidOperation, ValueError):
        return None

def derive_discount(price, *, promo_price=None, discount_amount=None, discount_percent=None):
    p = _to_dec(price)
    if p is None or p <= 0:
        return None, p
    cand = _to_dec(promo_price)
    if cand is not None and Decimal("0") <= cand <= p:
        return cand, p
    amt = _to_dec(discount_amount)
    if amt is not None:
        cand = p - amt
        if cand < 0:
            cand = Decimal("0")
        if cand > p:
            cand = p
        return cand, p
    pct = _to_dec(discount_percent)
    if pct is not None:
        cand = p * (Decimal("1") - pct / Decimal("100"))
        if cand < 0:
            cand = Decimal("0")
        if cand > p:
            cand = p
        return cand, p
    return None, p

# ----------------- S3 pointer fetch -----------------
_s3 = boto3.client("s3")

# Optional: load enrichment maps from S3 at cold start so you can update without redeploy
def _load_json_from_s3(bucket: Optional[str], key: Optional[str]) -> Optional[dict]:
    if not bucket or not key:
        return None
    try:
        obj = _s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        logger.warning(f"Failed to load mapping from s3://{bucket}/{key}: {e}")
        return None

def _init_enrichment_maps():
    # Branch map format expected:
    # { "victory": { "branch_1": {"city":"...","address":"..."}, ... }, "yohananof": { ... } }
    b_bucket = os.getenv("BRANCH_MAP_S3_BUCKET")
    b_key = os.getenv("BRANCH_MAP_S3_KEY")
    branch_json = _load_json_from_s3(b_bucket, b_key)
    if isinstance(branch_json, dict):
        count = 0
        for prov, entries in branch_json.items():
            if not isinstance(entries, dict):
                continue
            for bname, val in entries.items():
                if isinstance(val, dict):
                    city = val.get("city") or "Unknown"
                    addr = val.get("address") or "Unknown"
                    BRANCH_MAP[_canon_branch_key(prov, bname)] = (city, addr)
                    count += 1
        if count:
            logger.info(f"Loaded {count} branch mappings from S3")

    # Also support inline JSON via env var BRANCH_MAP_INLINE_JSON
    inline = os.getenv("BRANCH_MAP_INLINE_JSON")
    if inline:
        try:
            data = json.loads(inline)
            if isinstance(data, dict):
                for prov, entries in data.items():
                    if not isinstance(entries, dict):
                        continue
                    for bname, val in entries.items():
                        if isinstance(val, dict):
                            BRANCH_MAP[_canon_branch_key(prov, bname)] = (
                                val.get("city") or "Unknown",
                                val.get("address") or "Unknown",
                            )
                logger.info("Loaded branch mappings from inline JSON")
        except Exception as e:
            logger.warning(f"Failed to parse BRANCH_MAP_INLINE_JSON: {e}")

    # Brand aliases format expected:
    # { "תנובה": ["תנובה","tnuva"], "קוקה-קולה": ["קוקהקולה","cocacola"] }
    a_bucket = os.getenv("BRAND_ALIASES_S3_BUCKET")
    a_key = os.getenv("BRAND_ALIASES_S3_KEY")
    alias_json = _load_json_from_s3(a_bucket, a_key)
    if isinstance(alias_json, dict):
        added = 0
        for display, aliases in alias_json.items():
            if isinstance(aliases, list):
                for alias in aliases:
                    alias_key = _strip_non_alnum_hebrew(str(alias)).lower()
                    if alias_key:
                        BRAND_ALIASES[alias_key] = display
                        added += 1
        if added:
            logger.info(f"Loaded {added} brand aliases from S3")

    # Also support inline JSON via env var BRAND_ALIASES_INLINE_JSON
    inline_alias = os.getenv("BRAND_ALIASES_INLINE_JSON")
    if inline_alias:
        try:
            data = json.loads(inline_alias)
            if isinstance(data, dict):
                added = 0
                for display, aliases in data.items():
                    if isinstance(aliases, list):
                        for alias in aliases:
                            alias_key = _strip_non_alnum_hebrew(str(alias)).lower()
                            if alias_key:
                                BRAND_ALIASES[alias_key] = display
                                added += 1
                if added:
                    logger.info(f"Loaded {added} brand aliases from inline JSON")
        except Exception as e:
            logger.warning(f"Failed to parse BRAND_ALIASES_INLINE_JSON: {e}")

_init_enrichment_maps()
def _fetch_items_from_s3(bucket: str, key: str) -> List[dict]:
    obj = _s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    try:
        body = gzip.decompress(body)
    except Exception:
        pass
    try:
        data = json.loads(body.decode("utf-8"))
    except Exception:
        logger.warning(f"Could not JSON-parse S3 {bucket}/{key}")
        return []
    if isinstance(data, dict) and isinstance(data.get("items"), list): return data["items"]
    if isinstance(data, list): return data
    return []

# ----------------- Message extraction -----------------
def _extract_items_and_meta(msg: dict):
    kind = _ensure_str(msg.get("kind", ""), "")
    if not kind and ("chunk_seq" in msg or "chunk" in msg): kind = "chunk"
    if kind == "manifest":
        return [], {}, msg, kind, msg
    items = None
    for path in (("items",),("payload","items"),("data","items"),("records",),("Records",)):
        cur, ok = msg, True
        for k in path:
            if isinstance(cur, dict) and k in cur: cur = cur[k]
            else: ok = False; break
        if ok and isinstance(cur, list):
            items = cur; break
    if items is None:
        b = msg.get("items_s3_bucket") or msg.get("itemsBucket") or msg.get("bucket")
        k = msg.get("items_s3_key") or msg.get("itemsKey") or msg.get("key")
        if b and k: items = _fetch_items_from_s3(b, k)
    if items is None: items = []

    branch = msg.get("branch") or msg.get("branch_info") or {}
    if isinstance(branch, str): branch = {"name": branch}
    if not isinstance(branch, dict): branch = {}

    meta = {
        "provider": msg.get("provider"),
        "type": msg.get("type") or msg.get("output_type"),
        "timestamp": msg.get("timestamp"),
        "group_id": msg.get("group_id") or msg.get("MessageGroupId")
    }
    return items, branch, meta, kind, msg

def _parse_record_body(body: str):
    obj = _json_loads_maybe_base64(body)
    if obj is None: return None
    obj = _unwrap_sns_envelope(obj)
    for k in ("body","message","MessageBody"):
        if isinstance(obj, dict) and k in obj:
            inner = _json_loads_maybe_base64(obj[k]) if isinstance(obj[k], str) else obj[k]
            if inner is not None: obj = inner
    return obj if isinstance(obj, dict) else None

# ----------------- Database -----------------
class DB:
    def __init__(self):
        self.url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
        if not self.url:
            raise RuntimeError("SUPABASE_DB_URL/DATABASE_URL not set")
        self.schema = os.getenv("DB_SCHEMA","public")
        self.t_products = os.getenv("TABLE_PRODUCTS","products")
        self.t_branches = os.getenv("TABLE_BRANCHES","branches")
        self.t_prices = os.getenv("TABLE_PRICES","prices")

        if self.url.startswith("postgres://"):
            self.url = "postgresql://" + self.url[len("postgres://"):]

        import urllib.parse as up
        p = up.urlparse(self.url)
        user, pwd, host = p.username, p.password, p.hostname
        port, db = (p.port or 5432), p.path.lstrip("/")

        from ssl import create_default_context
        ctx = create_default_context()
        rootcert = os.getenv("PGSSLROOTCERT")
        if rootcert and os.path.exists(rootcert):
            ctx.load_verify_locations(rootcert); logger.info(f"Loaded CA from {rootcert}")
        else:
            bundle = os.path.join(os.path.dirname(__file__), "certs", "supabase.crt")
            if os.path.exists(bundle):
                ctx.load_verify_locations(bundle); logger.info(f"Loaded CA from {bundle}")

        if _db_mode == "pg8000":
            self.conn = pg8000.dbapi.connect(user=user, password=pwd, host=host, port=port, database=db, ssl_context=ctx)
        else:
            self.conn = psycopg2.connect(user=user, password=pwd, host=host, port=port, dbname=db, sslmode="require", sslrootcert=os.getenv("PGSSLROOTCERT") or None)

    def upsert_branch(self, name, address, city) -> int:
        qset = f"SET search_path TO {self.schema}"
        qsel = f"SELECT branch_id FROM {self.t_branches} WHERE name=%s AND COALESCE(address,'')=%s AND COALESCE(city,'')=%s LIMIT 1"
        qins = f"INSERT INTO {self.t_branches} (name, address, city) VALUES (%s,%s,%s) RETURNING branch_id"
        if _db_mode == "pg8000":
            c = self.conn.cursor()
        else:
            c = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        c.execute(qset); c.execute(qsel, (name, address or "", city or ""))
        row = c.fetchone()
        if row:
            bid = row[0] if _db_mode == "pg8000" else row["branch_id"]
            c.close(); return bid
        c.execute(qins, (name, address, city))
        row = c.fetchone()
        bid = row[0] if _db_mode == "pg8000" else row["branch_id"]
        self.conn.commit(); c.close(); return bid

    def upsert_product(self, product_name, brand_name, barcode) -> int:
        brand_name = brand_name or "Unknown"
        qset = f"SET search_path TO {self.schema}"
        if barcode:
            qups = f"""
                INSERT INTO {self.t_products} (barcode, product_name, brand_name)
                VALUES (%s,%s,%s)
                ON CONFLICT (barcode) DO UPDATE
                SET product_name=EXCLUDED.product_name,
                    brand_name=COALESCE(EXCLUDED.brand_name, {self.t_products}.brand_name)
                RETURNING product_id
            """
            c = self.conn.cursor(); c.execute(qset); c.execute(qups, (barcode, product_name, brand_name))
            pid = c.fetchone()[0]; self.conn.commit(); c.close(); return pid
        qsel = f"SELECT product_id FROM {self.t_products} WHERE product_name=%s AND COALESCE(brand_name,'')=%s LIMIT 1"
        qins = f"INSERT INTO {self.t_products} (barcode, product_name, brand_name) VALUES (NULL,%s,%s) RETURNING product_id"
        c = self.conn.cursor(); c.execute(qset); c.execute(qsel, (product_name, brand_name or ""))
        row = c.fetchone()
        if row: pid = row[0]; self.conn.commit(); c.close(); return pid
        c.execute(qins, (product_name, brand_name))
        pid = c.fetchone()[0]; self.conn.commit(); c.close(); return pid

    def insert_price(self, product_id, branch_id, price, discount_price, ts):
        if discount_price is None and FILL_NULL_DISCOUNT: discount_price = price
        qset = f"SET search_path TO {self.schema}"
        qins = f"""
            INSERT INTO {self.t_prices} (product_id, branch_id, price, discount_price, ts)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (product_id, branch_id, ts) DO NOTHING
        """
        c = self.conn.cursor(); c.execute(qset); c.execute(qins, (product_id, branch_id, price, discount_price, ts))
        self.conn.commit(); c.close()

# ----------------- Normalization -----------------
def _normalize_item(item: dict, provider: str):
    product = item.get("product") or item.get("name") or item.get("product_name") or item.get("description") or ""
    product = str(product).strip()
    brand = item.get("brand") or item.get("brand_name") or item.get("manufacturer")
    brand = str(brand).strip() if brand else None
    # Deterministic brand enrichment
    if not brand or brand.strip() == "" or brand.lower() in {"unknown", "לא ידוע"}:
        # 1) From nested meta (if producer/manufacturer provided)
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        for k in ("manufacturer", "brand", "supplier", "producer"):
            v = (meta.get(k) if meta else None)
            if v and str(v).strip():
                brand = str(v).strip()
                break
    if not brand or brand.strip() == "" or brand.lower() in {"unknown", "לא ידוע"}:
        # 2) Alias match within product string
        ext_brand, clean_name = extract_brand_and_clean_name(product)
        if ext_brand:
            brand = ext_brand
            product = clean_name
    if not brand or brand.strip() == "" or brand.lower() in {"unknown", "לא ידוע"}:
        # 3) Heuristic from Hebrew product descriptors
        hb = heuristic_brand_from_product(product)
        if hb:
            # Map heuristic token through aliases if possible for canonical display
            key = _strip_non_alnum_hebrew(hb).lower()
            display = BRAND_ALIASES.get(key)
            brand = display or hb
            # try to remove brand from product nicely
            try:
                patt = re.compile(rf"\b{re.escape(brand)}\b")
                product = patt.sub("", product)
            except Exception:
                pass
    brand = brand or "Unknown"
    barcode = item.get("barcode") or item.get("gtin") or item.get("sku") or None
    if barcode is not None: barcode = str(barcode).strip() or None
    price = _coerce_number(item.get("price") or item.get("Price") or item.get("unit_price"))
    promo_price = _coerce_number(item.get("discount_price") or item.get("sale_price") or item.get("promo_price"))
    discount_amount = _coerce_number(item.get("discount_amount"))
    discount_percent = _coerce_number(item.get("discount_percent"))
    dprice, price_dec = derive_discount(price, promo_price=promo_price,
                                        discount_amount=discount_amount,
                                        discount_percent=discount_percent)
    price = price_dec
    ts = _iso_dt(item.get("ts") or item.get("timestamp") or item.get("date")) or datetime.now(timezone.utc)
    return product, brand, barcode, price, dprice, ts

# ----------------- Handler -----------------
def lambda_handler(event, context=None):
    try:
        db = DB()
    except Exception as e:
        logger.error(f"DB init failed: {e}"); raise

    batch_failures = []
    processed = 0; errors = 0

    for rec in event.get("Records", []):
        message_id = rec.get("messageId")
        try:
            body = rec.get("body","")
            msg = _parse_record_body(body)
            if not isinstance(msg, dict):
                if LOG_EMPTY_INFO: logger.info("Record body not dict; skipping")
                else: logger.debug("Record body not dict; skipping")
                continue

            items, branch, meta, kind, raw_msg = _extract_items_and_meta(msg)
            if kind == "manifest":
                logger.debug(f"Skipping manifest message for group_id={meta.get('group_id','')}"); continue
            if not items:
                if LOG_EMPTY_INFO:
                    logger.info(f"Empty items; kind={kind or 'None'} keys={list(raw_msg.keys())[:10]} pointer=({raw_msg.get('items_s3_bucket')},{raw_msg.get('items_s3_key')})")
                else:
                    logger.debug(f"No items; kind={kind or 'None'}")
                continue

            provider = _ensure_str(meta.get("provider"), default="Unknown Provider")
            name = _ensure_str(branch.get("name"), default="Unknown Branch")
            # Deterministic city/address enrichment
            en_city, en_addr = enrich_branch_deterministic(provider, name)
            city = en_city or "Unknown"
            address = en_addr or "Unknown"

            # Call upsert_branch in a way that supports both signatures
            try:
                branch_id = db.upsert_branch(name=name, address=address, city=city)
            except TypeError:
                branch_id = db.upsert_branch(provider=provider, name=name, address=address, city=city)

            for it in items:
                product, brand, barcode, price, dprice, ts = _normalize_item(it, provider)
                if not product and not barcode:
                    continue
                if price is None or (isinstance(price, Decimal) and price <= 0):
                    continue
                pid = db.upsert_product(product, brand, barcode)
                db.insert_price(pid, branch_id, price, dprice, ts)
                processed += 1

        except Exception as e:
            errors += 1
            logger.exception(f"Error processing record {message_id}: {e}")
            if message_id: batch_failures.append({"itemIdentifier": message_id})

    logger.info(json.dumps({"status":"ok","processed":processed,"errors":errors}))
    return {"batchItemFailures": batch_failures}



# === BEGIN MONKEY-PATCH: resilient DB for Lambda ===
try:
    from pg_resilient import from_env as _pg_from_env
    _HAS_RESILIENT = True
except Exception as _e:
    _HAS_RESILIENT = False

if _HAS_RESILIENT:
    _OldDB = DB

    def _DB___init__(self):
        # Keep table/schema envs from original
        self.url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
        self.schema = os.getenv("DB_SCHEMA","public")
        self.t_products = os.getenv("TABLE_PRODUCTS","products")
        self.t_branches = os.getenv("TABLE_BRANCHES","branches")
        self.t_prices = os.getenv("TABLE_PRICES","prices")

        # If only DATABASE_URL is set, derive PG* vars for the resilient client
        if self.url:
            if self.url.startswith("postgres://"):
                self.url = "postgresql://" + self.url[len("postgres://"):]
            import urllib.parse as _up
            _p = _up.urlparse(self.url)
            _host, _port = _p.hostname, _p.port or 5432
            _user, _pwd = _p.username, _p.password
            _db = _p.path.lstrip("/") if _p.path else ""
            # Populate env if missing
            os.environ.setdefault("PGHOST", _host or "")
            os.environ.setdefault("PGPORT", str(_port))
            os.environ.setdefault("PGUSER", _user or "")
            os.environ.setdefault("PGPASSWORD", _pwd or "")
            os.environ.setdefault("PGDATABASE", _db or "")
            # If Supabase pooled is desired, user can set PGPORT=6543 and PGPOOL=true externally

        self.db = _pg_from_env()  # resilient client
        try:
            self.db.ensure()
        except Exception as e:
            raise RuntimeError(f"DB init failed: {e}")

        self.conn = None  # disable old raw connection

    def _DB_upsert_branch(self, provider, name, address, city) -> int:
        qset = f"SET search_path TO {self.schema}"
        qsel = f"SELECT branch_id FROM {self.t_branches} WHERE provider=%s AND COALESCE(name,'')=%s AND COALESCE(address,'')=%s AND COALESCE(city,'')=%s LIMIT 1"
        qins = f"INSERT INTO {self.t_branches} (provider, name, address, city) VALUES (%s,%s,%s,%s) RETURNING branch_id"
        self.db.execute(qset)
        row = self.db.execute(qsel, (provider or "Unknown", name or "", address or "", city or ""), fetch="one")
        if row and row[0]:
            return row[0][0]
        row = self.db.execute(qins, (provider or "Unknown", name, address, city), fetch="one")
        return row[0][0] if row and row[0] else None
    def _DB_upsert_product(self, product_name, brand_name, barcode) -> int:
        brand_name = brand_name or "Unknown"
        qset = f"SET search_path TO {self.schema}"
        self.db.execute(qset)
        if barcode:
            qups = f"""                INSERT INTO {self.t_products} (barcode, product_name, brand_name)
                VALUES (%s,%s,%s)
                ON CONFLICT (barcode) DO UPDATE
                SET product_name=EXCLUDED.product_name,
                    brand_name=COALESCE(EXCLUDED.brand_name, {self.t_products}.brand_name)
                RETURNING product_id
            """
            row = self.db.execute(qups, (barcode, product_name, brand_name), fetch="one")
            return row[0][0] if row and row[0] else None
        qsel = f"SELECT product_id FROM {self.t_products} WHERE COALESCE(product_name,'')=%s AND COALESCE(brand_name,'')=%s LIMIT 1"
        qins2 = f"INSERT INTO {self.t_products} (product_name, brand_name) VALUES (%s,%s) RETURNING product_id"
        row = self.db.execute(qsel, (product_name or "", brand_name), fetch="one")
        if row and row[0]:
            return row[0][0]
        row2 = self.db.execute(qins2, (product_name or "", brand_name), fetch="one")
        return row2[0][0] if row2 and row2[0] else None

    def _DB_insert_price(self, product_id, branch_id, price, discount_price, ts):
        if discount_price is None and FILL_NULL_DISCOUNT:
            discount_price = price
        qset = f"SET search_path TO {self.schema}"
        qins = f"""            INSERT INTO {self.t_prices} (product_id, branch_id, price, discount_price, ts)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (product_id, branch_id, ts) DO NOTHING
        """
        self.db.execute(qset)
        self.db.execute(qins, (product_id, branch_id, price, discount_price, ts))

    DB.__init__ = _DB___init__
    DB.upsert_branch = _DB_upsert_branch
    DB.upsert_product = _DB_upsert_product
    DB.insert_price = _DB_insert_price
# === END MONKEY-PATCH ===



# === BEGIN MONKEY-PATCH v2: Use ON CONFLICT for idempotent upserts ===
def _DB_upsert_branch_v2(self, name, address, city) -> int:
    # Normalize "unknown-ish" fields
    def _nz(v, default="Unknown"):
        if v is None: return default
        vs = str(v).strip()
        return vs if vs else default

    name_v = _nz(name, default="Unknown")
    addr_v = _nz(address, default="Unknown")
    city_v = _nz(city, default="Unknown")

    qset = f"SET search_path TO {self.schema}"
    q = f"""        INSERT INTO {self.t_branches} (name, address, city)
        VALUES (%s, %s, %s)
        ON CONFLICT ON CONSTRAINT uq_branches_name_city_address
        DO UPDATE SET updated_at = now()
        RETURNING branch_id
    """
    self.db.execute(qset)
    row = self.db.execute(q, (name_v, addr_v, city_v), fetch="one")
    return row[0][0] if row and row[0] else None

def _DB_upsert_product_v2(self, product_name, brand_name, barcode) -> int:
    # Normalize strings
    def _nz(v, default="Unknown"):
        if v is None: return default
        vs = str(v).strip()
        return vs if vs else default

    product_v = _nz(product_name, default="")
    brand_v = _nz(brand_name, default="Unknown")
    barcode_v = (str(barcode).strip() if barcode is not None else None)
    qset = f"SET search_path TO {self.schema}"
    self.db.execute(qset)

    if barcode_v:
        q = f"""            INSERT INTO {self.t_products} (barcode, product_name, brand_name)
            VALUES (%s,%s,%s)
            ON CONFLICT (barcode) DO UPDATE
            SET product_name = EXCLUDED.product_name,
                brand_name   = COALESCE(EXCLUDED.brand_name, {self.t_products}.brand_name),
                updated_at   = now()
            RETURNING product_id
        """
        row = self.db.execute(q, (barcode_v, product_v, brand_v), fetch="one")
        return row[0][0] if row and row[0] else None

    # No barcode: rely on your partial unique index
    q = f"""        INSERT INTO {self.t_products} (barcode, product_name, brand_name)
        VALUES (NULL, %s, %s)
        ON CONFLICT ON CONSTRAINT ux_products_name_brand_when_no_barcode
        DO UPDATE SET updated_at = now()
        RETURNING product_id
    """
    row = self.db.execute(q, (product_v, brand_v), fetch="one")
    return row[0][0] if row and row[0] else None

# Activate v2 upserts
DB.upsert_branch = _DB_upsert_branch_v2
DB.upsert_product = _DB_upsert_product_v2
# === END MONKEY-PATCH v2 ===


# === BEGIN MONKEY-PATCH v3: Field enrichment (city/address/brand) + barcode upgrade ===
import json as _json, urllib.request as _urlreq, urllib.error as _urlerr, socket as _socket, re as re

# Minimal Israeli city list (extend as needed)
_IL_CITIES = {
    "tel aviv", "tel-aviv", "tlv", "jerusalem", "haifa", "beer sheva", "beersheba", "be'er sheva",
    "ashdod", "ashkelon", "netanya", "holon", "ramat gan", "rishon lezion", "rishon lezion",
    "petah tikva", "rehovot", "herzliya", "lod", "ramla", "nahariya", "hadera", "eilat",
    "kfar saba", "kiryat ono", "kiryat gat", "kiryat shmona", "modiin", "modiin-maccabim-reut",
    "bat yam", "givatayim", "sderot", "yavne", "nazareth", "acre", "akko", "umm al-fahm",
    "ariel", "tiberias", "rannana", "ra'anana", "mevasseret zion"
}

def _normtxt(s):
    if s is None: return ""
    return " ".join(str(s).split())

def _nz(v, default="Unknown"):
    t = _normtxt(v)
    return t if t else default

def _infer_city_from_name(name):
    n = _normtxt(name).lower()
    best = None
    for c in _IL_CITIES:
        if c in n:
            # normalize a few
            if c in ("tlv", "tel-aviv"): return "Tel Aviv"
            if c == "beer sheva" or c == "beersheba" or c == "be'er sheva": return "Beer Sheva"
            if c == "rishon lezion": return "Rishon LeZion"
            if c == "modiin" or c == "modiin-maccabim-reut": return "Modiin-Maccabim-Reut"
            if c == "acre" or c == "akko": return "Akko"
            if c == "rannana" or c == "ra'anana": return "Ra'anana"
            return " ".join(w.capitalize() for w in c.split())
    return None

def _openai_json(system, prompt, schema_hint=None, timeout=1.8):
    key = os.getenv("OPENAI_API_KEY")
    if not key: return None
    body = {
        "model": os.getenv("OPENAI_MODEL","gpt-4o-mini"),
        "messages": [
            {"role":"system","content": system},
            {"role":"user","content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 128
    }
    req = _urlreq.Request("https://api.openai.com/v1/chat/completions",
                          data=_json.dumps(body).encode("utf-8"),
                          headers={
                              "Authorization": f"Bearer {key}",
                              "Content-Type": "application/json"
                          })
    try:
        with _urlreq.urlopen(req, timeout=timeout) as resp:
            data = _json.loads(resp.read().decode("utf-8"))
            txt = data["choices"][0]["message"]["content"]
            # try to find JSON object in response
            m = re.search(r"\{[\s\S]*\}", txt)
            if m:
                return _json.loads(m.group(0))
            # fallback: naive parse
            return None
    except (_urlerr.URLError, _socket.timeout, KeyError, ValueError):
        return None

def enrich_branch_fields(name, address, city):
    # Heuristic city from name
    city_h = _infer_city_from_name(name)
    address_h = None

    # If still missing and OpenAI is available, ask it to parse
    final_city = _nz(city, default=city_h or "Unknown")
    final_address = _nz(address, default="Unknown")

    if (final_city == "Unknown" or final_address == "Unknown"):
        js = _openai_json(
            "You extract Israeli branch city and street address if present in the name. "
            "Return compact JSON with keys city and address; if unknown set to Unknown.",
            f"Branch name: {name!r}. Known city: {city!r}. Known address: {address!r}. ",
            timeout=float(os.getenv("OPENAI_TIMEOUT","1.8"))
        )
        if isinstance(js, dict):
            c = _nz(js.get("city"), default=final_city)
            a = _nz(js.get("address"), default=final_address)
            final_city, final_address = c, a

    # If heuristic city better than Unknown, prefer it
    if city_h and final_city == "Unknown":
        final_city = city_h
    return final_address, final_city

_BRAND_HINTS = {
    # add common brands you see
    "tnuva","tara","elita","nescafe","coca-cola","sprite","fanta","pepsi","osem","strauss",
    "milka","nutella","heinz","hellmann's","prigat","tapuzina","sano","tofes","assaf",
}

def enrich_product_fields(product_name, brand_name):
    p = _normtxt(product_name)
    b = _normtxt(brand_name)
    # Simple heuristic: if brand unknown, try first token(s) until a size/number
    if not b or b.lower() == "unknown":
        toks = p.split()
        lead = []
        for t in toks:
            if re.match(r"^\d", t) or re.match(r"^\d+(ml|l|gr|g|kg|pack|pcs)$", t.lower()):
                break
            lead.append(t)
            if len(lead) >= 2:
                break
        guess = " ".join(lead).strip()
        if guess and guess.lower() in _BRAND_HINTS:
            b = guess
    # Optional LLM refinement if still Unknown
    if not b or b.lower() == "unknown":
        js = _openai_json(
            "Split a grocery product name into brand and normalized product name. "
            "Return JSON: {brand_name, product_name}. If brand unknown, brand_name='Unknown'.",
            f"Product: {p!r}",
            timeout=float(os.getenv("OPENAI_TIMEOUT","1.6"))
        )
        if isinstance(js, dict):
            b2 = _nz(js.get("brand_name"), default=b or "Unknown")
            p2 = _nz(js.get("product_name"), default=p)
            return p2, b2
    return p, (b if b else "Unknown")

# Override branch upsert to enrich before write (keeps same ON CONFLICT logic)
def _DB_upsert_branch_v3(self, provider, name, address, city) -> int:
    addr_v, city_v = enrich_branch_fields(name, address, city)
    qset = f"SET search_path TO {self.schema}"
    q = f"""        INSERT INTO {self.t_branches} (provider, name, address, city)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT uq_branches_provider_name_city_address
        DO UPDATE SET updated_at = now()
        RETURNING branch_id
    """
    self.db.execute(qset)
    row = self.db.execute(q, (_nz(provider, "Unknown"), _nz(name,"Unknown"), addr_v, city_v), fetch="one")
    return row[0][0] if row and row[0] else None


# Upgrade no-barcode product to have barcode if it matches on (name,brand)
def _try_upgrade_barcode(self, product_name, brand_name, barcode):
    if not barcode: return None
    q = f"""        UPDATE {self.t_products}
           SET barcode = %s, updated_at = now()
         WHERE product_id = (
           SELECT product_id FROM {self.t_products}
            WHERE barcode IS NULL
              AND product_name = %s
              AND brand_name = %s
            LIMIT 1
         )
        RETURNING product_id
    """
    row = self.db.execute(q, (barcode, product_name, brand_name), fetch="one")
    return row[0][0] if row and row[0] else None

def _DB_upsert_product_v3(self, product_name, brand_name, barcode) -> int:
    # Enrich fields first
    p_v, b_v = enrich_product_fields(product_name, brand_name)
    b_v = b_v or "Unknown"
    bc_v = (_normtxt(barcode) or None) if barcode is not None else None

    self.db.execute(f"SET search_path TO {self.schema}")
    # First try to upgrade existing no-barcode row
    if bc_v:
        upid = _try_upgrade_barcode(self, p_v, b_v, bc_v)
        if upid:
            return upid

    if bc_v:
        q = f"""            INSERT INTO {self.t_products} (barcode, product_name, brand_name)
            VALUES (%s,%s,%s)
            ON CONFLICT (barcode) DO UPDATE
            SET product_name = EXCLUDED.product_name,
                brand_name   = COALESCE(EXCLUDED.brand_name, {self.t_products}.brand_name),
                updated_at   = now()
            RETURNING product_id
        """
        row = self.db.execute(q, (bc_v, p_v, b_v), fetch="one")
        return row[0][0] if row and row[0] else None

    q = f"""        INSERT INTO {self.t_products} (barcode, product_name, brand_name)
        VALUES (NULL, %s, %s)
        ON CONFLICT ON CONSTRAINT ux_products_name_brand_when_no_barcode
        DO UPDATE SET updated_at = now()
        RETURNING product_id
    """
    row = self.db.execute(q, (p_v, b_v), fetch="one")
    return row[0][0] if row and row[0] else None

# Activate enrichment overrides
DB.upsert_branch = _DB_upsert_branch_v3
DB.upsert_product = _DB_upsert_product_v3
# === END MONKEY-PATCH v3 ===
