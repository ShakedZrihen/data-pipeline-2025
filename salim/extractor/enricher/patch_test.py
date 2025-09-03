import io
import sys
import unittest

from patch import DataPatcher


class TestDataPatcher(unittest.TestCase):
    def test_enrich_price(self):
        data = {
            "provider": "7290058197699",
            "type": "price",
            "branch": "752",
            "items": [
                {
                    "price": 22.9,
                    "product": 'וייט גלו - משחת שיניים מלבינה להסרת כתמים ופלאק 100 מ"ל + מברשת שיניים',
                    "unit": "liter",
                },
                {
                    "price": 22.9,
                    "product": 'וויט גלו - משחת שיניים מלבינה עם פחם להסרת כתמים יסודית 100 מ"ל',
                    "unit": "liter",
                },
                {
                    "price": 22.9,
                    "product": "וייט גלו - משחת שיניים אינסטנט וויט 150 גרם + מברשת שיניים",
                    "unit": "gram",
                },
            ],
        }
        patcher = DataPatcher(data)
        patched_data = patcher.enrich()

        print(f"Original: {data}")
        print(f"patche: {patched_data}")

        for item in patched_data["items"]:
            self.assertIn(item["unit"], ["unit", "liter", "ml", "kg", "gram", "meter"])

        self.assertTrue(patched_data["provider"].isalpha())
        self.assertIsNotNone(patched_data["address"])

    def test_enrich_promo(self):
        capturedOutput = io.StringIO()
        sys.stdout = capturedOutput
        data = {
            "provider": "7290058197699",
            "type": "promo",
            "branch": "752",
            "promos": [
                {
                    "price": 22.9,
                    "product": 'וייט גלו - משחת שיניים מלבינה להסרת כתמים ופלאק 100 מ"ל + מברשת שיניים',
                    "min_qty": 2,
                },
                {
                    "price": 22.9,
                    "product": 'וויט גלו - משחת שיניים מלבינה עם פחם להסרת כתמים יסודית 100 מ"ל',
                    "min_qty": 0,
                },
                {
                    "price": 22.9,
                    "product": "וייט גלו - משחת שיניים אינסטנט וויט 150 גרם + מברשת שיניים",
                    "min_qty": 4,
                },
            ],
        }
        patcher = DataPatcher(data)
        patched_data = patcher.enrich()

        print(f"Original: {data}")
        print(f"patche: {patched_data}")

        sys.stdout = sys.__stdout__
        print("Captured", capturedOutput.getvalue())

        for item in patched_data["promos"]:
            self.assertIsInstance(item.get("min_qty", ""), int)

        self.assertTrue(patched_data["provider"].isalpha())
        self.assertIsNotNone(patched_data["address"])
