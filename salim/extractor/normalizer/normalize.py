import json
from typing import Any, Dict
from collections.abc import Mapping
from datetime import datetime


class DataNormalizer:
    def __init__(self, json_dict: Dict[str, Any],timestamp:str):
        self.data = json_dict
        self.timestamp = self._parse_time(timestamp).isoformat()

    def normalize(self):
        data = self.data
        provider_id = str(data.get("ChainId", ""))
        store_id = str(data.get("StoreId", ""))

        # Normalize the items container to a list of dicts
        items_container = data.get("Items") or {}
        items = {}
        if isinstance(items_container, Mapping):
            items = items_container.get("Item", [])
        else:
            items = items_container  # already a list (or bad input)

        # If a single item came as an object, wrap it
        if isinstance(items, Mapping):
            items = [items]
        if not isinstance(items, list):
            items = []

        promos_container = data.get("Promotions") or []
        promos = []

        if isinstance(promos_container,Mapping):
           promos = promos_container.get("Promotion",[]) 
        else:
            promos = promos_container

        if isinstance(promos,Mapping):
            promos = [promos]
        if not isinstance(promos,list):
            promos = []

        if promos:
            return self._normalize_promotions(
                provider=provider_id, store=store_id, promos=promos
            )
        else:
            return self._normalize_prices(
                provider=provider_id, store=store_id,items=items
            )

    def _normalize_promotions(
        self, provider: str, store: str, promos: list[Dict[str, Any]]
    )->Dict[str,Any]:
        data = {
            "provider": provider,
            "branch": store,
            "type":"PriceFull",
            "timestamp":self.timestamp,
            "promotions":self._normalize_promos_items(promos=promos)
        }
        return data

    def _normalize_prices(self, provider: str, store: str,items: list[Dict[str, Any]])->Dict[str,Any]:
        data = {
            "provider": provider,
            "branch": store,
            "type":"PriceFull",
            "timestamp":self.timestamp,
            "items":self._normalize_items(items)
        }

        return data

    def _normalize_items(self,items: list[Dict[str, Any]])->list[Dict[str, Any]]:
        res = []
        for item in items:
            itm = {}
            itm["price"] = float(item.get("ItemPrice") or -1)
            itm["product"] = item.get("ItemNm") or item.get("ItemName")
            itm["unit"] = self._normalize_units(item.get("UnitQty","")) 
            res.append(itm)
        return res

    def _normalize_promos_items(self,promos:list[Dict[str, Any]])->list[Dict[str,Any]]:
        res = []

        for p in promos:
            item = {}
            item["product"] = p.get("PromotionDescription","None")
            item["price"] = float(p.get("DiscountedPrice") or -1)
            val = p.get("MinQty")
            try:
                item["min_qty"] = int(float(val)) if val not in (None, "") else 1
            except (ValueError, TypeError):
                item["min_qty"] = 1
            res.append(item)
        return res

    @staticmethod
    def _normalize_units(unit:str):
        unit_mapping = {
            'י״ח': 'unit',
            'יח': 'unit',
            'יחידה': 'unit',
            'ליטר': 'liter',
            'ל': 'liter',
            'מ״ל': 'ml',
            'מל': 'ml',
            'ק״ג': 'kg',
            'קג': 'kg',
            'קילוגרם': 'kg',
            'גרם': 'gram',
            'ג': 'gram',
            'מטר': 'meter',
            'מ': 'meter'
        }

        # Even though there is no lower case in hebrew we still use lower and strip 
        # in case of english units
        unit_stripped = unit.lower().strip()

        for hebrew, english in unit_mapping.items():
            if hebrew in unit_stripped:
                return english

        return unit

    @staticmethod
    def _parse_time(time_str: str) -> datetime:
        try:
            return datetime.strptime(time_str, "%Y%m%d%H%M")
        except Exception as e:
            print(f"Got parse_time exception: {e}. continuing...")
            return datetime.min
