from typing import Dict

from pymongo import MongoClient

from nbpc.database import AbstractDatabase, DB_NAME, TABLE_NAME


class MongoDatabase(AbstractDatabase):
    default_uri = "mongodb://localhost:27017/"

    def create_db(self):
        pass

    # This is where the session and the connection are created
    def __enter__(self):
        # make a database connection and return it
        self.connect_db()
        self.session = self.client.start_session()
        return self

    # This is for commiting the transaction
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.__exit__(exc_type, exc_val, exc_tb)
        if self.transaction:
            self.transaction.__exit__(exc_type, exc_val, exc_tb)

    def connect_db(self):
        self.client = MongoClient(self.uri)

    def drop_db(self):
        db = self.client[DB_NAME]
        db.drop_collection(TABLE_NAME)

    def start_transaction(self):
        self.transaction = self.session.start_transaction()

    def insert_many(self, items: [Dict[str, str]]) -> [Dict[str, str]]:
        db = self.client[DB_NAME]
        inserted = db[TABLE_NAME].insert_many(items)
        return inserted
