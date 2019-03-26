import argparse
import os
from functools import partial
from multiprocessing import Pool as ProcessPool
from pprint import pprint

import pandas as pd
from tqdm import tqdm

from nbpc.database import DatabaseFactory, DB_NAME

OUT_FILE_NAME = "amazon_reviews_us_unified.tsv"

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


def worker_job(filename: str, input_path: str, db_host: str, db_port: int, db_user: str, db_passwd: str):
    with Database(host=db_host, port=db_port, user=db_user, password=db_passwd, db=DB_NAME) as session:
        input_file_path = os.path.join(input_path, filename)
        df = pd.read_csv(input_file_path, names=COLUMN_NAMES, dtype=str, sep="\t", skiprows=1)
        session.start_transaction()
        for index, row in df.iterrows():
            print(row.get("product_title", ""))
            session.insert(
                dict(
                    product_title=row.get("product_title", ""),
                    product_category=row.get("product_category", "")
                )
            )


def process_dataset(input_: str, db_host: str, db_port: int, db_user: str, db_passwd: str, processes: int):
    with Database(host=db_host, port=db_port, user=db_user, password=db_passwd) as session:
        session.drop_db()
        session.create_db()

    ppool = ProcessPool(processes, maxtasksperchild=1)
    worker_job_new = partial(worker_job, input_path=input_, db_host=db_host, db_port=db_port, db_user=db_user,
                             db_passwd=db_passwd)

    for r, d, f in os.walk(input_):
        with tqdm(total=len(f[:2])) as progress:
            for _ in tqdm(ppool.imap_unordered(worker_job_new, f[:2])):
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
    # 3 GB system, 5 GB process python script, 3 GB browser
    parser.add_argument("--processes", type=int, default=1,
                        help="Numbers of processes in the pool")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    process_dataset(args.input, args.db_host, args.db_port, args.db_user, args.db_passwd, args.processes)
