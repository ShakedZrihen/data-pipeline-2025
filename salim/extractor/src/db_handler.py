# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Optional

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DatabaseHandler:
    """
    Simple per-(provider|branch|type) checkpoint store.
    Supports: local JSON file, DynamoDB, MongoDB (optional).
    """

    def __init__(
        self,
        db_type: str = "local",
        table_name: str = "salim_last_runs",
        region: str = "il-central-1",
        connection_string: str = "",
        database: str = "salim",
        local_file: str = "/tmp/last_runs.json",
    ):
        self.db_type = (db_type or "local").lower()
        self.table_name = table_name
        self.region = region
        self.local_file = Path(local_file)

        self._dynamo = None
        self._table = None
        self._mongo = None
        self._mongo_coll = None

        if self.db_type == "dynamo":
            self._dynamo = boto3.resource("dynamodb", region_name=self.region)
            self._table = self._dynamo.Table(self.table_name)
        elif self.db_type == "mongo":
            from pymongo import MongoClient  # imported lazily
            self._mongo = MongoClient(connection_string)
            self._mongo_coll = self._mongo[database][self.table_name]

    @staticmethod
    def _id(provider: str, branch: str, file_type: str) -> str:
        return f"{provider}|{branch}|{file_type}"

    def update_last_run(self, provider: str, branch: str, file_type: str, ts_iso: str) -> None:
        _id = self._id(provider, branch, file_type)
        if self.db_type == "dynamo":
            self._table.put_item(Item={"id": _id, "ts": ts_iso})
        elif self.db_type == "mongo":
            self._mongo_coll.update_one({"_id": _id}, {"$set": {"ts": ts_iso}}, upsert=True)
        else:
            data = {}
            if self.local_file.exists():
                try:
                    data = json.loads(self.local_file.read_text(encoding="utf-8"))
                except Exception:
                    data = {}
            data[_id] = ts_iso
            self.local_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Checkpoint updated: %s -> %s", _id, ts_iso)

    def get_last_run(self, provider: str, branch: str, file_type: str) -> Optional[str]:
        _id = self._id(provider, branch, file_type)
        if self.db_type == "dynamo":
            res = self._table.get_item(Key={"id": _id})
            return (res.get("Item") or {}).get("ts")
        elif self.db_type == "mongo":
            doc = self._mongo_coll.find_one({"_id": _id})
            return doc.get("ts") if doc else None
        else:
            if self.local_file.exists():
                try:
                    data = json.loads(self.local_file.read_text(encoding="utf-8"))
                    return data.get(_id)
                except Exception:
                    return None
            return None
