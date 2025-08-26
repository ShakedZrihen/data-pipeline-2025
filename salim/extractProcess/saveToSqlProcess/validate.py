from jsonschema import Draft202012Validator
from extractProcess.saveToSqlProcess.schema import JSON_SCHEMA

_validator = Draft202012Validator(JSON_SCHEMA)

def validate_message(msg: dict):

    errors = sorted(_validator.iter_errors(msg), key=lambda e: e.path)
    if errors:
        e = errors[0]
        loc = ".".join([str(p) for p in e.path]) or "<root>"
        return False, f"{loc}: {e.message}"
    t = msg.get("type")
    u = msg.get("unit")
    if t == "promoFull" and not isinstance(u, (int, float)):
        return False, "unit must be numeric (MinQty) for promoFull"
    if t == "pricesFull" and not isinstance(u, str):
        return False, "unit must be string for pricesFull"
    return True, None
