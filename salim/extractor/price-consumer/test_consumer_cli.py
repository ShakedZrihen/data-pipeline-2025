#!/usr/bin/env python3
# Quick CLI test for Consumer enrichment and discount logic (no DB)

import os
import json
from pprint import pprint

import sys

# Minimal stubs so we can import consumer_lambda without installed deps
class _StubS3Client:
    def get_object(self, Bucket, Key):
        raise RuntimeError("S3 get_object not available in CLI test")

class _StubBoto3:
    def client(self, name):
        if name == "s3":
            return _StubS3Client()
        raise RuntimeError(f"Unsupported client: {name}")

class _StubPg8000:  # not used in this test
    class dbapi:
        @staticmethod
        def connect(**kwargs):
            raise RuntimeError("DB connect not available in CLI test")

class _StubPsycopg2Extras: pass
class _StubPsycopg2:
    extras = _StubPsycopg2Extras()

sys.modules.setdefault('boto3', _StubBoto3())
sys.modules.setdefault('pg_resilient', type('X', (), {'from_env': lambda: None}))
sys.modules.setdefault('pg8000', _StubPg8000())
sys.modules.setdefault('psycopg2', _StubPsycopg2())
sys.modules.setdefault('psycopg2.extras', _StubPsycopg2Extras())

import consumer_lambda as c

def main():
    # Prepare a sample message similar to Extractor output
    msg = {
        "provider": "victory",
        "branch": "branch_1",
        "type": "pricesFull",
        "timestamp": "2025-09-05T10:00:00Z",
        "items": [
            {"product": "חלבתנובה 3%", "price": 5.9, "unit": "liter"},
            {"product": "קוקהקולה 1.5 ליטר", "price": 8.9, "discount_percent": 10},
            {"product": "שטראוס יוגורט", "price": 4.5, "discount_amount": 1.0},
        ],
    }

    provider = msg.get("provider")
    branch_name = msg.get("branch")

    # Enrich branch (city/address) deterministically
    city, address = c.enrich_branch_deterministic(provider, branch_name)
    print("Branch enrichment:")
    print({"provider": provider, "branch": branch_name, "city": city, "address": address})

    # Enrich items (brand + discount)
    out = []
    for it in msg["items"]:
        product = it.get("product") or it.get("name") or ""
        brand = (it.get("brand") or it.get("brand_name") or it.get("manufacturer") or "").strip() or None
        if not brand or brand.lower() in {"unknown", "לא ידוע"}:
            ext_brand, clean_name = c.extract_brand_and_clean_name(product)
            if ext_brand:
                brand = ext_brand
                product = clean_name
        price = c._coerce_number(it.get("price"))
        promo_price = c._coerce_number(it.get("discount_price") or it.get("sale_price") or it.get("promo_price"))
        discount_amount = c._coerce_number(it.get("discount_amount"))
        discount_percent = c._coerce_number(it.get("discount_percent"))
        dprice, price_dec = c.derive_discount(price, promo_price=promo_price,
                                              discount_amount=discount_amount,
                                              discount_percent=discount_percent)
        out.append({
            "input": it,
            "product_enriched": product,
            "brand_enriched": brand or "Unknown",
            "price": float(price_dec) if price_dec is not None else None,
            "discount_price": float(dprice) if dprice is not None else None,
            "final_price": float(dprice if dprice is not None else price_dec) if price_dec is not None else None,
        })

    print("\nItems enrichment:")
    pprint(out, width=140, compact=True)


if __name__ == "__main__":
    # Allow inline mappings via env before import-init runs
    # Example:
    #   $env:BRANCH_MAP_INLINE_JSON = '{"victory":{"branch_1":{"city":"תל אביב-יפו","address":"אבן גבירול 100"}}}'
    main()
