import os
from typing import Dict, Optional

def enrich_with_openai(item: Dict) -> Dict:
    # If OPENAI_API_KEY is not provided, return heuristic enrichment
    if not os.getenv("OPENAI_API_KEY"):
        return heuristic_enrich(item)
    try:
        # Lazy import to avoid dependency if not used
        from openai import OpenAI
        client = OpenAI()
        name = item.get("canonical_name") or ""
        prompt = f"Extract brand and category for the product: '{name}'. Respond with JSON only, with keys 'brand' and 'category'."
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        txt = resp.choices[0].message.content
        import json
        data = json.loads(txt)
        item["brand"] = item.get("brand") or data.get("brand")
        item["category"] = item.get("category") or data.get("category")
        return item
    except Exception:
        return heuristic_enrich(item)

def heuristic_enrich(item: Dict) -> Dict:
    name = (item.get("canonical_name") or "").lower()
    if "חלב" in name:
        item.setdefault("category", "חלב ומוצריו")
    if "יוגורט" in name:
        item.setdefault("category", "חלב ומוצריו")
    if "קולה" in name or "קוקה" in name:
        item.setdefault("brand", "קוקה קולה")
        item.setdefault("category", "משקאות")
    if "לחם" in name:
        item.setdefault("category", "מזון יבש")
    return item
