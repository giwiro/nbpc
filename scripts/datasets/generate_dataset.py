import argparse
import csv
import os
import tempfile
import time

import pandas as pd

from nbpc.database import DatabaseFactory, DB_NAME

DATASET_NAME = "datasets.tsv"

# COLUMN_NAMES = [
#     "id",
#     "name",
#     "category"
# ]

Database = DatabaseFactory.get_class("maria")


# def generate_dataset(out: str, db_host: str, db_port: int, db_user: str, db_passwd: str, batch_size: str):
#     with Database(host=db_host, port=db_port, user=db_user, password=db_passwd, db=DB_NAME) as session:
#         cursor = session.fetch_all_cursor()
#         out_path = os.path.join(out, DATASET_NAME)
#         with open(out_path, 'w') as f:
#             header_writter = csv.writer(f, delimiter="\t")
#             header_writter.writerow(COLUMN_NAMES)
#             while True:
#                 # Read the data
#                 df = pd.DataFrame(cursor.fetchmany(batch_size))
#                 # We are done if there are no data
#                 if len(df) == 0:
#                     break
#                 # Let's write to the file
#                 else:
#                     df.to_csv(f, header=False, sep="\t", index=False)

def generate_dataset(db_host: str, db_port: int, db_user: str, db_passwd: str):
    with Database(host=db_host, port=db_port, user=db_user, password=db_passwd, db=DB_NAME) as session:
        # Only on Mysql
        tmp_dir = tempfile.gettempdir()
        dataset_path = os.path.join(tmp_dir, DATASET_NAME)
        print(f"Generating datasets...")
        start_time = time.time()
        # your code
        session.persist_tsv(dataset_path)
        elapsed_time = time.time() - start_time
        print(f"Elapsed time {round(elapsed_time, 2)}s")
        print(f"Dataset created at: {dataset_path}")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    # parser.add_argument("--out", type=str, required=True,
    #                     help="Output folder location")
    parser.add_argument("--db-host", type=str, default="localhost",
                        help="Database host")
    parser.add_argument("--db-port", type=int, default=3306,
                        help="Database host")
    parser.add_argument("--db-user", type=str, default="root",
                        help="Database user")
    parser.add_argument("--db-passwd", type=str, default="",
                        help="Database password")
    parser.add_argument("--batch-size", type=int, default=1000,
                        help="Batch size")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    generate_dataset(args.db_host, args.db_port, args.db_user, args.db_passwd)
