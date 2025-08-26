JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "provider": {"type": "string", "minLength": 1},
        "branch":   {"type": "string", "minLength": 1},
        "type":     {"enum": ["pricesFull", "promoFull"]},
        "timestamp":{"type": "string", "format": "date-time"},

        "productId": {"type": "string", "minLength": 1},
        "product":   {"type": "string", "minLength": 1},
        "price":     {"type": "number"},
        "unit":      {},
        "currency":  {"type": "string"}
    },
    "required": ["provider","branch","type","timestamp","productId","product","price","unit"],
    "additionalProperties": True
}
