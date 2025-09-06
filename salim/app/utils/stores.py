import json, os
from fastapi import HTTPException


def load_stores_data():
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "stores"))
    providers = {}
    for fname in os.listdir(base_path):
        if fname.endswith(".json"):
            with open(os.path.join(base_path, fname), encoding="utf-8") as f:
                data = json.load(f)
                providers[data["provider_id"]] = {
                    "provider_name": data["provider_name"],
                    "branches": data.get("branches", {})
                }
    return providers

STORES_DATA = load_stores_data()
PROVIDER_NAME_TO_ID = {v["provider_name"]: k for k, v in STORES_DATA.items()}
PROVIDER_ID_TO_NAME = {k: v["provider_name"] for k, v in STORES_DATA.items()}

def _normalize_branch(b: str):
    if b is None:
        return ""
    s = str(b)
    # נסיון 1: כמו שהוא
    if s in ("",):
        return s
    # נסיון 2: בלי אפסים מובילים (001 -> 1)
    try:
        return str(int(s))
    except ValueError:
        # אם לא מספרי, נחזיר כמו שהוא
        return s

def _branch_display(branches: dict, branch_code: str):
    # חיפוש מדויק
    if branch_code in branches:
        v = branches[branch_code]
    else:
        # חיפוש בגרסה מנורמלת (למשל "001" -> "1")
        norm = _normalize_branch(branch_code)
        v = branches.get(norm)
    if not v:
        return branch_code
    return v.get("name", branch_code) if isinstance(v, dict) else v

def enrich_row(row: dict):
    prov = STORES_DATA.get(str(row.get("provider")))
    if prov:
        row["provider"] = prov["provider_name"]
        row["branch"] = _branch_display(prov["branches"], str(row.get("branch")))
    # להסתיר id
    row.pop("id", None)
    return row


def enrich_provider_branch(row: dict) -> dict:
    provider_id = str(row.get("provider"))
    branch_code = str(row.get("branch"))

    provider_data = STORES_DATA.get(provider_id)
    if provider_data:
        # המרת מזהה ספק לשם
        row["provider"] = provider_data["provider_name"]

        # המרת מזהה סניף לשם, כולל טיפול ב-001/1
        branches = provider_data.get("branches", {})
        row["branch"] = _branch_display(branches, branch_code)

    return row


def resolve_provider_id_or_404(provider: str) -> str:
    if provider.isdigit():
        provider_id = provider
    else:
        provider_id = PROVIDER_NAME_TO_ID.get(provider)

    if provider_id not in STORES_DATA:
        raise HTTPException(status_code=404, detail="Provider not found")

    return provider_id
