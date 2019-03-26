from pprint import pprint
from typing import Dict

import mysql.connector as mariadb

from nbpc.database import AbstractDatabase, DB_NAME, TABLE_NAME


class MariaDatabase(AbstractDatabase):

    # This is where the session and the connection are created
    def __enter__(self):
        # make a database connection and return it
        self.connect_db()
        return self

    # This is for commiting the transaction
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.transaction:
            self.client.commit()
            self.transaction.close()
            self.client.close()

    def connect_db(self):
        self.client = mariadb.connect(host=self.host, user=self.user, passwd=self.password,
                                      database=self.db if self.db else "")

    def create_db(self):
        create_cursor = self.client.cursor()
        sql = f"CREATE DATABASE IF NOT EXISTS {DB_NAME} DEFAULT CHARACTER SET 'utf8'"
        sql2 = f"USE {DB_NAME}"
        sql3 = f"CREATE TABLE {TABLE_NAME} (" \
            "id INT NOT NULL AUTO_INCREMENT," \
            "name TEXT NOT NULL," \
            "category VARCHAR(120) NOT NULL," \
            "custom_category VARCHAR(120)," \
            "PRIMARY KEY (id)" \
            ")"
        print(sql)
        create_cursor.execute(sql)
        print(sql2)
        create_cursor.execute(sql2)
        print(sql3)
        create_cursor.execute(sql3)

    def drop_db(self):
        delete_cursor = self.client.cursor()
        sql = f"DROP DATABASE IF EXISTS {DB_NAME}"
        print(sql)
        delete_cursor.execute(sql)

    def start_transaction(self):
        self.transaction = self.client.cursor()

    def insert_many(self, items: [Dict[str, str]]) -> [Dict[str, str]]:
        pass

    def insert(self, item: Dict[str, str]) -> Dict[str, str]:
        sql = f"INSERT INTO {TABLE_NAME} " \
            "(name, category, custom_category) " \
            "VALUES(%s, %s, %s)"
        sql_values = (item.get("product_title", ""), item.get("product_category", ""), "")
        self.transaction.execute(sql, sql_values)
