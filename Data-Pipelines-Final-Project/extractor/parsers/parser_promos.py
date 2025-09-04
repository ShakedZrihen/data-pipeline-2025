def _clean_digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

_CANDIDATE_TAGS = (
    "Barcode", "ItemBarCode",
    "ItemCode", "ItemID", "ItemId", "Code",
    "ProductId", "ProductID",
)

def _find_barcodes_in_promo(el):

    codes = set()

    for tag in _CANDIDATE_TAGS:
        v = el.findtext(f".//{tag}")
        if v:
            digits = _clean_digits(v)
            if 7 <= len(digits) <= 20:
                codes.add(digits)

    for item in el.findall(".//Item"):
        for tag in _CANDIDATE_TAGS:
            v = item.findtext(f".//{tag}") or item.findtext(tag)
            if v:
                digits = _clean_digits(v)
                if 7 <= len(digits) <= 20:
                    codes.add(digits)

    return codes

def parse(root):
    promos = []
    for el in root.findall(".//Promotion"):
        name = (el.findtext("PromotionDescription") or el.findtext("Name") or "unknown").strip()
        price_text = el.findtext("DiscountedPrice") or el.findtext("Price") or "0"
        try:
            price = float(str(price_text).replace(",", "."))
        except Exception:
            price = 0.0

        unit = el.findtext("UnitOfMeasure") or "unit"

        barcodes = _find_barcodes_in_promo(el)

        if barcodes:
            for bc in barcodes:
                promos.append({
                    "product": name,
                    "price": price,
                    "unit": unit,
                    "promo_text": name,
                    "barcode": bc,
                })
        else:
            promos.append({
                "product": name,
                "price": price,
                "unit": unit,
                "promo_text": name,
            })

    return promos
