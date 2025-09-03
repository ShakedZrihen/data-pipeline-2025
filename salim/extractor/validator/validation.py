from jsonschema import validate


class DataValidator:
    def __init__(self, data):
        self.data = data

    def validate_data(self):
        schema = {
            "type": "object",
            "properties": {
                "provider": {"type": "string"},
                "branch": {"type": "string"},
                "type": {"type": "string"},
                "timestamp": {"type": "string"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "price": {"type": "number"},
                            "product": {"type": "string"},
                            "unit": {"type": "string"},
                        },
                        "required": ["price", "product", "unit"],
                    },
                },
            },
        }
        validate(self.data, schema)
