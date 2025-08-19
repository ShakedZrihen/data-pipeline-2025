import unittest

from jsonschema import ValidationError
from validation import DataValidator


class TestDataValidator(unittest.TestCase):
    def test_valid_data(self):
        data = {
            "provider": "GoodPharm",
            "branch": "123",
            "type": "PriceFull",
            "timestamp": "2025-08-19T10:00:00",
            "items": [
                {"price": 12.5, "product": "Aspirin", "unit": "box"},
                {"price": 5.0, "product": "Bandage", "unit": "pack"},
            ],
        }
        validator = DataValidator(data)
        # Should not raise
        validator.validate_data()

    def test_missing_optional_root_fields(self):
        data = {"items": [{"price": 9.99, "product": "Soap", "unit": "piece"}]}
        validator = DataValidator(data)
        # Should not raise since root fields are optional
        validator.validate_data()

    def test_missing_required_item_field(self):
        data = {
            "items": [
                {"price": 3.5, "product": "Shampoo"}  # missing "unit"
            ]
        }
        validator = DataValidator(data)
        with self.assertRaises(ValidationError):
            validator.validate_data()

    def test_wrong_type(self):
        data = {
            "provider": "GoodPharm",
            "items": [{"price": "not-a-number", "product": "Soap", "unit": "piece"}],
        }
        validator = DataValidator(data)
        with self.assertRaises(ValidationError):
            validator.validate_data()


if __name__ == "__main__":
    unittest.main()
