import argparse
import csv
import os
import string
from functools import partial
from multiprocessing import Pool as ProcessPool
from pprint import pprint
from typing import Dict

import pandas as pd
from tqdm import tqdm

from nbpc.database import DatabaseFactory, DB_NAME

WRONG_ENTRIES_FILE_NAME = "wrong_entries.tsv"

COLUMN_NAMES = [
    "marketplace",
    "customer_id",
    "review_id",
    "product_id",
    "product_parent",
    "product_title",
    "product_category",
    "star_rating",
    "helpful_votes",
    "total_votes",
    "vine",
    "verified_purchase",
    "review_headline",
    "review_body",
    "review_date",
]

# Safer, avoid lowMemory option warning
# COLUMN_DTYPES = {
#     "marketplace": str,
#     "customer_id": str,
#     "review_id": str,
#     "product_id": str,
#     "product_parent": str,
#     "product_title": str,
#     "product_category": str,
#     "star_rating": int,
#     "helpful_votes": int,
#     "total_votes": int,
#     "vine": str,
#     "verified_purchase": str,
#     "review_headline": str,
#     "review_body": str,
#     "review_date": str,
# }

Database = DatabaseFactory.get_class("maria")


def map_row(row: pd.Series) -> Dict[str, str]:
    title = row.get("product_title", "")
    # Here we need to get rid of all unsafe chars like emojis
    title = title.encode('ascii', 'ignore').decode('ascii')
    # Remove punctuation
    title = ''.join(c for c in title if c not in string.punctuation)
    # Remove numbers
    # title = ''.join(c for c in title if c not in '0123456789')
    # Remove unnecessary white spaces
    title = ' '.join(title.split())
    # To lower case
    title = title.lower()

    category = row.get("product_category", "")
    # To lower case
    category = category.lower()
    # Snake case
    category = "_".join(category.split(" "))

    custom_category = ""

    if category == "apparel":
        custom_category = "clothing"
    elif category == "beauty":
        custom_category = "makeup_beauty"
    elif category == "camera":
        custom_category = "cameras_video"
    elif category == "digital_video_games":
        custom_category = "game_consoles"
    elif category == "shoes":
        custom_category = "shoes"
    elif category == "video_games":
        custom_category = "game_consoles"
    elif category == "watches":
        custom_category = "clothing_accessories"

    resp = dict(
        product_title=title,
        product_category=category,
        custom_category=custom_category,
    )
    # print(resp)
    return resp


def worker_job(filename: str, input_path: str, db_host: str, db_port: int, db_user: str, db_passwd: str):
    with Database(host=db_host, port=db_port, user=db_user, password=db_passwd, db=DB_NAME) as session:
        input_file_path = os.path.join(input_path, filename)
        wrong_entries_file_path = os.path.join(input_path, WRONG_ENTRIES_FILE_NAME)
        df = pd.read_csv(input_file_path, names=COLUMN_NAMES, dtype=str, sep="\t", quoting=csv.QUOTE_NONE, skiprows=1)
        session.start_transaction()
        for index, row in df.iterrows():
            title = row.get("product_title", "")
            # We gotta check if number (NaN) otherwise it will crash
            if type(title).__name__ == "int" or type(title).__name__ == "float":
                with open(wrong_entries_file_path, "a") as we:
                    we.write("\t".join(str(x) for x in row.values) + "\n")
                continue
            try:
                mr = map_row(row)
                session.insert(mr)
            except Exception as e:
                print(f"ERROR !!!! in file => {filename}")
                pprint(row.values)
                raise e


def process_dataset(input_: str, db_host: str, db_port: int, db_user: str, db_passwd: str, processes: int):
    wrong_entries_file_path = os.path.join(input_, WRONG_ENTRIES_FILE_NAME)
    with open(wrong_entries_file_path, "w") as we:
        we.write("\t".join(COLUMN_NAMES) + "\n")

    with Database(host=db_host, port=db_port, user=db_user, password=db_passwd) as session:
        session.drop_db()
        session.create_db()

    ppool = ProcessPool(processes, maxtasksperchild=1)
    worker_job_new = partial(worker_job, input_path=input_, db_host=db_host, db_port=db_port, db_user=db_user,
                             db_passwd=db_passwd)

    for r, d, f in os.walk(input_):
        with tqdm(total=len(f)) as progress:
            for _ in tqdm(ppool.imap_unordered(worker_job_new, f)):
                progress.update()

    ppool.close()
    ppool.join()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True,
                        help="Dataset folder location")
    parser.add_argument("--db-host", type=str, default="localhost",
                        help="Database host")
    parser.add_argument("--db-port", type=int, default=3306,
                        help="Database host")
    parser.add_argument("--db-user", type=str, default="root",
                        help="Database user")
    parser.add_argument("--db-passwd", type=str, default="",
                        help="Database password")
    # Keep in mind it will load all the ds into your memory
    # I strongly suggest you pick 2 if you have <= 12 GB
    # 3 GB system, 5 GB python script process, 3 GB browser
    parser.add_argument("--processes", type=int, default=1,
                        help="Numbers of processes in the pool")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    process_dataset(args.input, args.db_host, args.db_port, args.db_user, args.db_passwd, args.processes)
