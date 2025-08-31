# extractProcess/ai_enricher_simple.py
from typing import Dict, Any, List
from openai import OpenAI
import os, json

PROMPT = (
    'אתה מקבל פריט בודד שנמכר בסופרמרקט בישראל.\n'
    'השדות המסופקים: type, product, unit, ManufacturerName, ManufacturerItemDescription.\n'
    'קבע שני שדות בלבד:\n\n'
    '- brand: שקול גם את ManufacturerName וגם את ManufacturerItemDescription ובחר את שם המותג המדויק ביותר. '
    'אם לא ניתן לקבוע — החזר בדיוק "לא ידוע".\n'
    '- itemType: קטגוריה קצרה בעברית (לדוגמה: "פירות", "ירקות", "משקאות", "חטיפים", "מוצרי חלב", '
    '"מאפיה", "בשר ודגים", "קפואים", "ניקיון", "טואלטיקה", "בעלי חיים", "תינוקות", "מזון כללי"). '
    'אם לא ברור — החזר "לא ידוע".\n\n'
    'החזר אך ורק את אובייקט ה-JSON הבא, ללא שום טקסט נוסף (ללא הסברים, ללא כותרות, ללא סימני קוד), ובדיוק במבנה זה:\n'
    '{"brand":"...","itemType":"..."}'
)

def enrich_brand_itemtype(
    envelope: Dict[str, Any],
    item: Dict[str, Any],
    model: str = "gpt-4o-mini"
) -> Dict[str, Any]:
    """
    מקבל פריט יחיד + envelope, שולח ל-LLM ומחזיר את אותו item עם brand ו-itemType שנוספו.
    אם אין מפתח API או יש שגיאה – מחזיר פולבאק פשוט.
    """
    # אפשרות לכבות העשרה ע"י משתנה סביבה
    if os.getenv("AI_ENRICH_DISABLED") == "1":
        item["brand"] = item.get("manu_name") or "לא ידוע"
        item["itemType"] = "לא ידוע"
        return item

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        item["brand"] = item.get("manu_name") or "לא ידוע"
        item["itemType"] = "לא ידוע"
        return item

    client = OpenAI(api_key=api_key)

    payload = {
        "type": (envelope.get("type") or "").strip(),
        "product": item.get("product") or "",
        "unit": item.get("unit") or "",
        "ManufacturerName": item.get("manu_name") or "",
        "ManufacturerItemDescription": item.get("manu_desc") or "",
    }

    try:
        resp = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": "Return strict JSON only."},
                {"role": "user", "content": [
                    {"type": "text", "text": PROMPT},
                    {"type": "input_text", "text": json.dumps(payload, ensure_ascii=False)}
                ]}
            ],
            response_format={"type": "json_object"},
            max_output_tokens=300,
        )

        out_txt = getattr(resp, "output_text", None)
        if out_txt is None:
            out_txt = ""
            for c in getattr(resp, "output", []) or []:
                for p in getattr(c, "content", []) or []:
                    if getattr(p, "type", "") == "output_text":
                        out_txt += p.text or ""

        data = json.loads(out_txt) if out_txt else {}
        brand = data.get("brand")
        item_type = data.get("itemType")

        item["brand"] = brand.strip() if isinstance(brand, str) and brand.strip() else (item.get("manu_name") or "לא ידוע")
        item["itemType"] = item_type.strip() if isinstance(item_type, str) and item_type.strip() else "לא ידוע"
        return item

    except Exception:
        # פולבאק בטוח
        item["brand"] = item.get("manu_name") or "לא ידוע"
        item["itemType"] = "לא ידוע"
        return item


def enrich_brand_itemtype_for_items(
    envelope: Dict[str, Any],
    items: List[Dict[str, Any]],
    model: str = "gpt-4o-mini"
) -> List[Dict[str, Any]]:
    return [enrich_brand_itemtype(envelope, dict(it), model=model) for it in items]
