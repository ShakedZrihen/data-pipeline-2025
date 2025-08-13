import os

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class MongoDBClient:
    def __init__(self, db_name: str, host: str = "localhost", port: int = 27017):
        self.user = os.getenv("MONGO_INITDB_ROOT_USERNAME")
        self.password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")

        if not self.user or not self.password:
            raise ValueError(
                "MONGO_INITDB_ROOT_USERNAME and MONGO_INITDB_ROOT_PASSWORD environment variables must be set."
            )

        uri = f"mongodb://{self.user}:{self.password}@{host}:{port}/?authSource=admin"
        self.client = MongoClient(uri)

        try:
            # Will raise exception if server is not available
            self.client.admin.command("ping")
            print("Connected to MongoDB...")
        except ConnectionFailure as e:
            raise ConnectionError(f"MongoDB connection failed: {e}")

        self.db = self.client[db_name]

    def insert_document(self, collection_name: str, document: dict):
        result = self.db[collection_name].insert_one(document)
        return result.inserted_id

    def find_document(self, collection_name: str, query: dict):
        return self.db[collection_name].find_one(query)

    def find_all(self, collection_name: str, query: dict = None):
        query = query or {}
        return list(self.db[collection_name].find(query))


if __name__ == "__main__":
    # Example usage
    mongo = MongoDBClient(db_name="extracted_files")

    # Find all
    users = mongo.find_all("files")
    print("All files:", users)
