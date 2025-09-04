from typing import List, Dict
import re

def _clean_barcode(val) -> str | None:
    if val is None:
        return None
    s = re.sub(r"\D", "", str(val))
    if 7 <= len(s) <= 20:
        return s
    return None

def parse(root) -> List[Dict]:

    items: List[Dict] = []
    for el in root.findall(".//Item"):
        name = (el.findtext("ItemName") or el.findtext("Name") or "").strip()

        price_text = (
            el.findtext("ItemPrice")
            or el.findtext("Price")
            or "0"
        )
        unit = (
            el.findtext("UnitOfMeasure")
            or el.findtext("Unit")
            or "unit"
        )

        code = (
            el.findtext("ItemCode")
            or el.findtext("ItemId")
            or el.findtext("ProductId")
            or el.findtext("Code")
            or el.findtext("Barcode")
            or el.findtext("ItemBarcode")
        )

        try:
            price = float(str(price_text).replace(",", "."))
        except Exception:
            price = 0.0

        if name:
            row: Dict = {"product": name, "price": price, "unit": unit}
            bc = _clean_barcode(code)
            if bc:
                row["barcode"] = bc
            items.append(row)
    return items
