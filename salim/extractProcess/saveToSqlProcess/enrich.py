####################################################
####################################################
####################################################
### ENRICH WITH AI, UNCOMMENT TO USE
####################################################
####################################################
####################################################
 
# from typing import Dict, Any, List
# from openai import OpenAI
# import os, json

# PROMPT = (
#     'אתה מקבל פריט בודד שנמכר בסופרמרקט בישראל.\n'
#     'השדות המסופקים: type, product, unit, ManufacturerName, ManufacturerItemDescription.\n'
#     'קבע שני שדות בלבד:\n\n'
#     '- brand: שקול גם את ManufacturerName וגם את ManufacturerItemDescription ובחר את שם המותג המדויק ביותר. '
#     'אם לא ניתן לקבוע — החזר בדיוק "לא ידוע".\n'
#     '- itemType: קטגוריה קצרה בעברית (לדוגמה: "פירות", "ירקות", "משקאות", "חטיפים", "מוצרי חלב", '
#     '"מאפיה", "בשר ודגים", "קפואים", "ניקיון", "טואלטיקה", "בעלי חיים", "תינוקות", "מזון כללי"). '
#     'אם לא ברור — החזר "לא ידוע".\n\n'
#     'החזר אך ורק את אובייקט ה-JSON הבא, ללא שום טקסט נוסף (ללא הסברים, ללא כותרות, ללא סימני קוד), ובדיוק במבנה זה:\n'
#     '{"brand":"...","itemType":"..."}'
# )

# def enrich_brand_itemtype(
#     envelope: Dict[str, Any],
#     item: Dict[str, Any],
#     model: str = "gpt-4o-mini"
# ) -> Dict[str, Any]:
#     # אפשרות לכבות העשרה ע"י משתנה סביבה
#     if os.getenv("AI_ENRICH_DISABLED") == "1":
#         print("[enrich] AI_ENRICH_DISABLED=1 → fallback")
#         item["brand"] = item.get("manu_name") or "לא ידוע"
#         item["itemType"] = "לא ידוע"
#         return item

#     api_key = os.getenv("OPENAI_API_KEY")
#     if not api_key:
#         print("[enrich] Missing OPENAI_API_KEY → fallback")
#         item["brand"] = item.get("manu_name") or "לא ידוע"
#         item["itemType"] = "לא ידוע"
#         return item

#     client = OpenAI(api_key=api_key)

#     payload = {
#         "type": (envelope.get("type") or "").strip(),
#         "product": item.get("product") or "",
#         "unit": item.get("unit") or "",
#         "ManufacturerName": item.get("manu_name") or "",
#         "ManufacturerItemDescription": item.get("manu_desc") or "",
#     }

#     try:
#         # קריאה דרך Chat Completions עם אכיפת JSON
#         resp = client.chat.completions.create(
#             model=model,
#             messages=[
#                 {"role": "system", "content": "Return strict JSON only."},
#                 {"role": "user", "content": f"{PROMPT}\n\n{json.dumps(payload, ensure_ascii=False)}"}
#             ],
#             response_format={"type": "json_object"},
#             max_tokens=300,
#             temperature=0.2,
#             # timeout כללי לקריאה (שניות) – מונע תקיעות
#             timeout=30,
#         )

#         out_txt = resp.choices[0].message.content or ""
#         # אמור להיות JSON טהור לפי response_format
#         data = json.loads(out_txt) if out_txt else {}
#         brand = data.get("brand")
#         item_type = data.get("itemType")

#         item["brand"] = brand.strip() if isinstance(brand, str) and brand.strip() else (item.get("manu_name") or "לא ידוע")
#         item["itemType"] = item_type.strip() if isinstance(item_type, str) and item_type.strip() else "לא ידוע"

#         print(f"[enrich] Success → brand='{item['brand']}', itemType='{item['itemType']}'")
#         return item

#     except Exception as e:
#         print(f"[enrich] OpenAI call failed: {type(e).__name__}: {e} → fallback")
#         item["brand"] = item.get("manu_name") or "לא ידוע"
#         item["itemType"] = "לא ידוע"
#         return item


# def enrich_brand_itemtype_for_items(
#     envelope: Dict[str, Any],
#     items: List[Dict[str, Any]],
#     model: str = "gpt-4o-mini"
# ) -> List[Dict[str, Any]]:
#     return [enrich_brand_itemtype(envelope, dict(it), model=model) for it in items]




####################################################
####################################################
####################################################
## ENRICHMENT WITHOUT AI. USING LLM FOR ENRICHMENT TAKES TOO LONG,
## SO THIS VERSION IS USED TO ALLOW INSERTING THE DATA INTO POSTGRES.
## OTHERWISE, THE PROCESS WOULD TAKE DAYS.
## YOU CAN COMMENT OUT THIS PART AND UNCOMMENT THE PREVIOUS ONE
## TO ENABLE AI-BASED ENRICHMENT.
####################################################
####################################################
####################################################


from typing import Dict, Any, List

def enrich_brand_itemtype(
    envelope: Dict[str, Any],
    item: Dict[str, Any],
) -> Dict[str, Any]:

    item["brand"] = item.get("manu_name") or "לא ידוע"
    item["itemType"] = "לא ידוע"
    return item


def enrich_brand_itemtype_for_items(
    envelope: Dict[str, Any],
    items: List[Dict[str, Any]],
    model: str = "gpt-4o-mini"
) -> List[Dict[str, Any]]:
    return [enrich_brand_itemtype(envelope, dict(it), model=model) for it in items]