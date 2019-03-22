from typing import Type

from nbpc.database.abstract_database import AbstractDatabase, DB_NAME, TABLE_NAME
from nbpc.database.mongo import MongoDatabase


class DatabaseFactory:
    @staticmethod
    def get_class(t: str) -> Type[AbstractDatabase]:
        if t == "mongo":
            return MongoDatabase
