import json
from typing import Any, Dict

from openai import OpenAI
from pydantic import BaseModel


class StoreItemPricing(BaseModel):
    product: str
    unit: str


class StoreItemPromotion(BaseModel):
    product: str


class DataPatcher:
    def __init__(self, data: Dict[str, Any]):
        print("initializing data patcher")
        self.data = data
        self.chains_path = {
            "7290058197699": "/super-compare/salim/extractor/enricher/goodpharm_branches.json",
            "7290803800003": "/super-compare/salim/extractor/enricher/yohananof_branches.json",
            "7290058266241": "/super-compare/salim/extractor/enricher/citymarket_branches.json",
            "7290000000003": "/super-compare/salim/extractor/enricher/citymarket_branches.json",
        }
        self.chains_name = {
            "7290058197699": "goodpharm",
            "7290803800003": "yohananof",
            "7290058266241": "citymarket",
            "7290000000003": "citymarket",
            "000": "unknown",
        }
        self.openai_client = OpenAI()

    def _get_chain(self, chain_id: str) -> str | None:
        return self.chains_path.get(chain_id, None)

    def _get_chain_file_content(self, chain_name: str) -> Dict[str, Any]:
        with open(chain_name, "r") as file:
            return json.load(file)

    @staticmethod
    def _get_store_info(stores: list[Dict[str, Any]], target_branch: str) -> str:
        enriched_addr = ""
        for store in stores:
            store_branch = store.get("StoreID", "")
            store_addr = store.get("Address", "")

            if store_branch == target_branch:
                enriched_addr = store_addr
                return enriched_addr

        return enriched_addr

    def _enrich_price(self, product: str, unit: str):
        completion = self.openai_client.chat.completions.parse(
            model="gpt-4o-2024-08-06",
            messages=[
                {"role": "system", "content": "Extract the store item information."},
                {
                    "role": "user",
                    "content": f"""
                    our data is: unit is {unit}, product is: {product}
                    normalize the following fields:
                    unit: either a unit/liter/ml/kg/gram/meter
                    product: needs to be in the format of [brand]-[product name]
                    the product should stay in hebrew.
                    """,
                },
            ],
            response_format=StoreItemPricing,
        )

    def _enrich_prom(self, product: str):
        completion = self.openai_client.chat.completions.parse(
            model="gpt-4o-2024-08-06",
            messages=[
                {"role": "system", "content": "Extract the store item information."},
                {
                    "role": "user",
                    "content": f"""
                    our data is: product is: {product}
                    normalize the following fields:
                    product: needs to be in the format of [brand]-[product name],
                    remove any promotion information(like 2 in 1 and similar)
                    the product should stay in hebrew.
                    """,
                },
            ],
            response_format=StoreItemPromotion,
        )

    def enrich(self):
        provider = self.data.get("provider", "000")
        branch = self.data.get("branch", "unknown")
        t = self.data.get("type", "price")

        print(f"provider {provider} and branch: {branch}")
        # Soft error handling, if we dont find the provider we don't enrich.
        if not provider or not branch:
            print("didnt find provider or branch.")
            return

        self.data["provider"] = self.chains_name[provider]
        print(f"getting chain with provider {provider} and branch: {branch}")
        chain_path = self._get_chain(provider)
        if not chain_path:
            return

        chain_data = self._get_chain_file_content(chain_path)

        stores: list[Dict[str, Any]] = chain_data.get("Stores", [])
        print(f"getting store info with {len(stores)} stores")
        enriched_addr = self._get_store_info(stores, branch)

        self.data["address"] = enriched_addr
        print(f"enriched address: {enriched_addr}")
        if t != "price":
            items: Dict[str, Any] = self.data.get("items", [])
            for idx, item in enumerate(items):
                product = item.get("product", "")
                unit = item.get("unit", "")
                enriched_item = self._enrich_price(product, unit)
                self.data["items"][idx] = enriched_item

        else:
            proms: Dict[str, Any] = self.data.get("promotions", [])
            for idx, item in enumerate(proms):
                product = item.get("product", "")
                enriched_item = self._enrich_prom(product)
                self.data["promotions"][idx] = enriched_item
        print("finished data enrichment.")
        return self.data
