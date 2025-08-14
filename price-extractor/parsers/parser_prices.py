def parse(root):
    items = []
    for el in root.findall('.//Item'):
        name = (el.findtext('ItemName') or el.findtext('Name') or '').strip()
        price_text = el.findtext('Price') or '0'
        unit = el.findtext('UnitOfMeasure') or 'unit'
        try:
            price = float(str(price_text).replace(',', '.'))
        except Exception:
            price = 0.0
        if name:
            items.append({'product': name, 'price': price, 'unit': unit})
    return items
