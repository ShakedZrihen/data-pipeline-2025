import os, re, json, random
from typing import Dict, Optional

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

_client = None
_openai_key = os.getenv("OPENAI_API_KEY")
if OpenAI and _openai_key:
    _client = OpenAI(api_key=_openai_key)

ENRICH_FLAG = os.getenv("ENRICH_ENABLE", os.getenv("ENRICH", "0"))
USE_OPENAI = (ENRICH_FLAG == "1") and (_client is not None)

try:
    SAMPLE_RATE = float(os.getenv("ENRICH_SAMPLE_RATE", "0.10"))
except Exception:
    SAMPLE_RATE = 0.10


EAN13_PAT = re.compile(r'(?:^|[^\d])(729\d{10}|\d{13})(?=$|[^\d])')

SIZE_PATTERNS = [
    (re.compile(r'(\d+(?:[.,]\d+)?)\s*(?:ל|ל\')?יטר|\b1\s*ליטר', re.UNICODE), 'ליטר'),
    (re.compile(r'(\d+(?:[.,]\d+)?)\s*מ\"?ל', re.UNICODE), 'מ"ל'),
    (re.compile(r'(\d+(?:[.,]\d+)?)\s*ק\"?ג', re.UNICODE), 'ק"ג'),
    (re.compile(r'(\d+(?:[.,]\d+)?)\s*גר(?:ם)?', re.UNICODE), 'גרם'),
    (re.compile(r'(\d+)\s*יח(?:\'|”|״)?(?:ד(?:ה|ות))?', re.UNICODE), 'יחידות'),
]

BRANDS = ['תנובה', 'טרה', 'שטראוס', 'קוקה קולה', 'ימבה', 'נספרסו', 'סוגת', 'אוסם', 'עלית']

CATEGORY_RULES = [
    (['חלב', 'יוגורט', 'לבן', 'גבינה'], 'חלב ומוצריו'),
    (['קוקה', 'קולה', 'משקה', 'ספרייט', 'פאנטה', 'שתיה'], 'משקאות קלים'),
    (['אורז', 'פסטה', 'קוסקוס'], 'מוצרי מזווה'),
    (['קפה', 'נס', 'אספרסו'], 'קפה ותה'),
    (['תה', 'תה ירוק', 'ארל גריי'], 'קפה ותה'),
]

SYS_PROMPT = """You are a product normalizer for Israeli supermarket items.
Given a raw product name in Hebrew, return a compact JSON with keys:
canonical_name, brand, category, size_value (number), size_unit (string like 'גרם','ק\"ג','ליטר','מ\"ל','יחידות').
Never include extra keys. If unknown, use null."""

def extract_barcode(text: str) -> Optional[str]:
    if not text:
        return None
    m = EAN13_PAT.search(text)
    if not m:
        return None
    code = m.group(1)
    return code if len(code) == 13 else None

def _heuristic_enrich(name: str) -> Dict:
    out = {
        "canonical_name": None,
        "brand": None,
        "category": None,
        "size_value": None,
        "size_unit": None,
    }
    if not name:
        return out

    for b in BRANDS:
        if b in name:
            out["brand"] = b
            break

    for pat, unit in SIZE_PATTERNS:
        m = pat.search(name)
        if m:
            try:
                val = m.group(1)
            except IndexError:
                val = '1'
            val = val.replace(',', '.')
            try:
                out["size_value"] = float(val)
            except Exception:
                pass
            out["size_unit"] = unit
            break

    for keywords, cat in CATEGORY_RULES:
        if any(k in name for k in keywords):
            out["category"] = cat
            break

    canon = EAN13_PAT.sub(' ', name)
    canon = re.sub(r'\s+', ' ', canon).strip()
    out["canonical_name"] = canon if canon else None
    return out

def _openai_struct(product_name: str) -> Dict:
    if not _client:
        return {}
    try:
        resp = _client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": SYS_PROMPT},
                {"role": "user", "content": product_name or ""},
            ],
            temperature=0.2,
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content)
        return {
            "canonical_name": (data.get("canonical_name") or None),
            "brand": (data.get("brand") or None),
            "category": (data.get("category") or None),
            "size_value": data.get("size_value"),
            "size_unit": (data.get("size_unit") or None),
        }
    except Exception:
        return {}

def enrich_row(row: Dict) -> Dict:

    row = dict(row)
    name = row.get("product") or ""

    bc = extract_barcode(name)
    if bc:
        row["barcode"] = bc

    heur = _heuristic_enrich(name)
    for k, v in heur.items():
        if v is not None and not row.get(k):
            row[k] = v

    if USE_OPENAI and (random.random() < SAMPLE_RATE):
        ai = _openai_struct(name)
        for k, v in ai.items():
            if v is not None:
                row[k] = v

    return row
