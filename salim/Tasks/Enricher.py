
import json
import os
from pathlib import Path

class Enricher:

    def enrich(self, privider_name: str, store_id: str):
        if privider_name == "Yohananof":
            res = self._enrich_Provider("YohananofBranchs.json",store_id)
            return res
        elif privider_name == "ZolVeBegadol":
            res = self._enrich_Provider("ZolVeBegadolBranchs.json",store_id)
            return res
        elif privider_name == "SuperSapir":
            res = self._enrich_Provider("SuperSapirBranchs.json",store_id)
            return res
        else:
            print(f"[WARN] Unknown provider for enrichment: {privider_name}")
            return None
    
    def _enrich_Provider(self, path, store_id: int):
            with open(path, encoding="utf-8") as f:
                rows = json.load(f)    # rows is a list[dict]

            s = store_id.strip()

            for row in rows:
                sid = str(row.get("StoreID", "")).strip()
                if sid == s:
                    print(f"[INFO] Enriched store_id={store_id} with data: {row}")
                    return row          # return the whole matching dict

            return None
