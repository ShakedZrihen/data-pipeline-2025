import os

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class MongoDBClient:
    def __init__(
        self,
        db_name: str | None = None,
        host: str | None = None,
        port: int | None = None,
    ):
        self.host = host or os.getenv("MONGO_HOST", "mongodb")
        self.port = int(port or os.getenv("MONGO_PORT", "27017"))
        self.user = os.getenv("MONGO_USER")
        self.password = os.getenv("MONGO_PASS")
        self.auth_db = os.getenv("MONGO_AUTH_DB", "admin")
        self.db_name = db_name or os.getenv("MONGO_DB", "extracted_files")

        if not self.user or not self.password:
            raise ValueError(
                "Set MONGO_USER and MONGO_PASS for MongoDB authentication."
            )

        uri = (
            f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/"
            f"{self.db_name}?authSource={self.auth_db}"
        )

        self.client = MongoClient(
            uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000
        )

        try:
            self.client.admin.command("ping")
            print(f"Connected to MongoDB at {self.host}:{self.port}, db={self.db_name}")
        except ConnectionFailure as e:
            raise ConnectionError(f"MongoDB connection failed: {e}")

        self.db = self.client[self.db_name]

    def insert_document(self, collection_name: str, document: dict):
        result = self.db[collection_name].insert_one(document)
        return result.inserted_id

    def find_document(self, collection_name: str, query: dict):
        return self.db[collection_name].find_one(query)

    def find_all(self, collection_name: str, query: dict | None = None):
        return list(self.db[collection_name].find(query or {}))


if __name__ == "__main__":
    mongo = MongoDBClient()
    print("All files:", mongo.find_all("files"))
