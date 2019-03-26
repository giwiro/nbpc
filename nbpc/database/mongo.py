from typing import Dict

from pymongo import MongoClient

from nbpc.database import AbstractDatabase, DB_NAME, TABLE_NAME


class MongoDatabase(AbstractDatabase):

    def __enter__(self):
        # make a database connection and return it
        self.connect_db()
        self.session = self.client.start_session()
        return self

    # This is where the session and the connection are created
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.__exit__(exc_type, exc_val, exc_tb)
        if self.transaction:
            self.transaction.__exit__(exc_type, exc_val, exc_tb)

    # This is for commiting the transaction
    def connect_db(self):
        auth = f"{self.user}:{self.password}@" if self.user and self.password else ""
        uri = f"mongodb://{auth}{self.host}:{self.port}/"
        self.client = MongoClient(uri)

    def create_db(self):
        pass

    def drop_db(self):
        db = self.client[DB_NAME]
        db.drop_collection(TABLE_NAME)

    def start_transaction(self):
        self.transaction = self.session.start_transaction()

    def insert_many(self, items: [Dict[str, str]]) -> [Dict[str, str]]:
        db = self.client[DB_NAME]
        inserted = db[TABLE_NAME].insert_many(items)
        return inserted

    def insert(self, item: Dict[str, str]) -> Dict[str, str]:
        pass
