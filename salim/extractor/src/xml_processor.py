
"""
XML Processor for Israeli Supermarket Price/Promo Files
Stream-friendly, robust to nested fields and attribute-only schemas (e.g., Super-Pharm).
"""
from __future__ import annotations
import logging
import re
import xml.etree.ElementTree as ET
from typing import Dict, Iterator, Optional, Sequence

logger = logging.getLogger(__name__)



def _local(tag: str) -> str:
    """Strip XML namespace and lowercase."""
    return tag.split("}")[-1].lower() if tag else ""

def _first_text_anywhere(elem: ET.Element, names: Sequence[str]) -> Optional[str]:
    """
    Return text OR attribute value of the first descendant whose localname
    matches any of `names` (case-insensitive). Searches recursively.
    """
    wanted = {n.lower() for n in names}

    
    for k, v in elem.attrib.items():
        if _local(k) in wanted and v:
            v = v.strip()
            if v:
                return v

    
    for node in elem.iter():
        lname = _local(node.tag)
        if lname in wanted:
            if node.text:
                t = node.text.strip()
                if t:
                    return t
        
        for k, v in node.attrib.items():
            if _local(k) in wanted and v:
                v = v.strip()
                if v:
                    return v
    return None

def _get_any(elem: ET.Element, *names: str) -> Optional[str]:
    return _first_text_anywhere(elem, names)

_num_re = re.compile(r"[-+]?\d+(?:[.,]\d+)?")

def _find_any_price(elem: ET.Element) -> Optional[str]:
    """
    Fallback: find the first descendant whose tag/attribute *contains* 'price'
    (case-insensitive), and return the first numeric-looking text/value.
    """
    for node in elem.iter():
       
        lname = _local(node.tag)
        if "price" in lname:
            if node.text:
                txt = node.text.strip()
                if _num_re.search(txt):
                    return txt
       
        for k, v in node.attrib.items():
            lname_k = _local(k)
            if ("price" in lname_k) and v:
                vv = v.strip()
                if _num_re.search(vv):
                    return vv
    return None



def _item_tagset(provider: str, file_type: str) -> set[str]:
    """
    Decide which tags are 'item-like' based on provider and file type.
    Keep defaults broad, and add very narrow extensions per provider where needed.
    """
    base = {
        
        "item", "product", "price", "promo",
        "line", "productprice", "productitem", "pricedetail",
        "pricedata", "promotion", "promoline", "row", "record",
      
        "מבצע", "פריט", "שורה",
    }

    p = (provider or "").lower()
    t = (file_type or "").lower()

    
    if p == "victory" and t == "promofull":
        base.add("sale")

   

    return base


class XMLProcessor:
    """
    Stream-friendly processor:
      - iter_items(file_obj, provider, file_type) -> yields 'raw' dicts
      - parse(xml_bytes, file_type) for non-streaming fallback
    """

    def iter_items(self, file_obj, provider: str, file_type: str) -> Iterator[Dict]:
        """
        Stream over <Item> / <Product> elements and yield raw dicts.
        Robust to namespaces, deep nesting, and attribute-only schemas.
        """
        item_tags = _item_tagset(provider, file_type)

        total = 0
        yielded = 0

       
        seen_sample = set()

        try:
            for event, elem in ET.iterparse(file_obj, events=("end",)):
                tag = _local(elem.tag)
                if len(seen_sample) < 12:
                    seen_sample.add(tag)

                if tag in item_tags:
                    total += 1
                    raw = self._to_raw(elem, provider=provider, file_type=file_type)
                    if raw:
                        yielded += 1
                        yield raw
                    
                    elem.clear()
        except ET.ParseError as e:
            logger.error("XML parse error: %s", e)

        if total == 0:
           
            logger.warning(
                "Streaming pass found 0 items (provider=%s type=%s). "
                "Seen tags sample=%s. If the source file is a tiny skeleton "
                "with empty <Items>/<Promotions>, this is expected.",
                provider, file_type, sorted(seen_sample)
            )

        logger.info("XMLProcessor: provider=%s type=%s items_seen=%s items_emitted=%s",
                    provider, file_type, total, yielded)

    def _to_raw(self, elem: ET.Element, provider: str, file_type: str) -> Optional[Dict]:
        """
        Convert an itemish element to a generic raw dict expected by the normalizer.
        Looks for fields recursively and in attributes (English + common Hebrew aliases).
        """
      
        name = _get_any(
            elem,
            
            "ItemName", "ItemDescription", "ProdName", "ProductName", "Name",
            "Description", "LongName", "BrandName", "ProdDesc", "Mnemonic",
            "DisplayName",
          
            "PromotionDescription", "PromotionName", "PromoDescription",
            
            "שםפריט", "תיאורפריט", "שםמוצר", "תיאורמוצר", "שםארוך", "מותג",
          
            "תיאורמבצע", "שםמבצע",
        )

       
        barcode = _get_any(
            elem,
            "ItemCode", "Barcode", "ProductId", "ProductID", "ProductCode",
            "ItemId", "ItemID", "Code", "SKU",
           
            "ברקוד", "מקט", "מק\"ט", "קודפריט", "קודמוצר"
        )

        
        unit = _get_any(
            elem,
            "UnitQty", "Quantity", "QtyInPackage", "UnitOfMeasure", "UOM", "Unit",
            "QuantityInPackage", "PackQuantity", "PackageQty", "MeasureUnit",
            "Weight", "WeightInGram", "Volume",
          
            "יחידה", "יחידות", "כמות", "משקל", "נפח", "אריזה", "כמותבאריזה",
            "משקלבגרם", "נפחבמ\"ל", "יח"
        )

     
        price = _get_any(
            elem,
            "ItemPrice", "Price", "UnitPrice", "PriceFull", "CurrentPrice",
            "SellPrice", "PromoPrice", "PromotionPrice", "DiscountPrice",
            "FinalPrice", "PayPrice", "SalePrice", "PriceAfterDiscount",
           
            "מחיר", "מחירפריט", "מחיריחידה", "מחירמבצע", "מחירסופי", "מחירלאחרהנחה"
        )
        if not price:
            price = _find_any_price(elem)  

       
        update = _get_any(
            elem,
            "PriceUpdateDate", "UpdateDate", "LastUpdateDate", "StartDate", "ValidOn",
            "UpdateTime", "Timestamp", "EffectiveDate",
           
            "PromoStartDate", "PromotionStartDate", "ValidFrom", "StartTime",
            
            "תאריךעדכון", "תאריךתחילה", "תאריךתחולה", "תאריך",
        )

       
        manufacturer = _get_any(
            elem,
            "ManufacturerName", "ManufactureName", "Manufacturer", "Brand", "SupplierName",
            
            "יצרן", "מותג", "ספק"
        )

        chain_id = _get_any(elem, "ChainId", "ChainID", "ChainNumber")
        sub_chain_id = _get_any(elem, "SubChainId", "SubChainID", "SubChainNumber")
        store_id = _get_any(elem, "StoreId", "StoreID", "StoreNum", "StoreNumber")

        raw: Dict[str, Optional[str]] = {
            "name": name,
            "price": price,
            "unit": unit,
            "barcode": barcode,
            "manufacturer": manufacturer,
            "chain_id": chain_id,
            "sub_chain_id": sub_chain_id,
            "store_id": store_id,
            "update": update,
            "date": update,         
            "provider": provider,
            "type": file_type,
        }

        
        if not (name or barcode):
            return None
        return raw

   

    def parse(self, xml_bytes: bytes, file_type: str) -> Dict:
        """
        Non-streaming fallback. Returns dict with 'items' and 'metadata'.
        """
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError:
            try:
                text = xml_bytes.decode("utf-8", errors="ignore")
                root = ET.fromstring(text)
            except Exception as e2:
                logger.error("Failed to parse XML: %s", e2)
                return {"items": [], "metadata": {}}

        items = []
        item_like = {"item", "product", "price", "promo", "line",
                     "productprice", "productitem", "pricedetail",
                     "pricedata", "promotion", "promoline", "row", "record",
                     "מבצע", "פריט", "שורה"}

        
        if (file_type or "").lower() == "promofull":
            item_like = set(item_like)
            item_like.add("sale")

        for elem in root.iter():
            tag = _local(elem.tag)
            if tag in item_like:
                raw = self._to_raw(elem, provider="unknown", file_type=file_type)
                if raw:
                    items.append(raw)
        return {"items": items, "metadata": {}}