from abc import ABC, abstractmethod
from typing import Dict

__names__ = ["AbstractDatabase", "TABLE_NAME", "DB_NAME"]

DB_NAME = "nbpc"
TABLE_NAME = "product"


class AbstractDatabase(ABC):
    default_uri: str

    def __init__(self, uri):
        self.uri = uri
        self.client = None
        self.session = None
        self.transaction = None
        super().__init__()

    # This is where the session and the connection are created
    def __enter__(self):
        # make a database connection and return it
        ...

    # This is for committing the transaction
    def __exit__(self, exc_type, exc_val, exc_tb):
        ...

    @abstractmethod
    def create_db(self):
        pass

    @abstractmethod
    def connect_db(self):
        pass

    @abstractmethod
    def drop_db(self):
        pass

    @abstractmethod
    def start_transaction(self):
        pass

    @abstractmethod
    def insert_many(self, items: [Dict[str, str]]) -> [Dict[str, str]]:
        pass
