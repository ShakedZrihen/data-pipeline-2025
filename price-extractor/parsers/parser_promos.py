def parse(root):
    promos = []
    for el in root.findall('.//Promotion'):
        name = (el.findtext('PromotionDescription') or el.findtext('Name') or 'unknown').strip()
        price_text = el.findtext('DiscountedPrice') or el.findtext('Price') or '0'
        unit = el.findtext('UnitOfMeasure') or 'unit'
        try:
            price = float(str(price_text).replace(',', '.'))
        except Exception:
            price = 0.0
        promos.append({'product': name, 'price': price, 'unit': unit})
    return promos
