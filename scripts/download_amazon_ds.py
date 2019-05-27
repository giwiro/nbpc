import argparse
import gzip
import shutil
import os
from queue import Queue
from threading import Thread

import boto3

BUCKET_NAME = "amazon-reviews-pds"
BUCKET_FOLDER_NAME = "tsv"
url_prefix = "s3://amazon-reviews-pds/tsv"

dataset_file_names = [
    "amazon_reviews_us_Apparel_v1_00.tsv",
    "amazon_reviews_us_Video_Games_v1_00.tsv",
    "amazon_reviews_us_Books_v1_00.tsv",
    "amazon_reviews_us_Jewelry_v1_00.tsv",
    "amazon_reviews_us_Sports_v1_00.tsv",
    "amazon_reviews_us_Software_v1_00.tsv",
    "amazon_reviews_us_Digital_Software_v1_00.tsv",
    "amazon_reviews_us_Digital_Video_Games_v1_00.tsv",
    "amazon_reviews_us_Toys_v1_00.tsv",
    "amazon_reviews_us_Camera_v1_00.tsv",
    "amazon_reviews_us_Grocery_v1_00.tsv",
    "amazon_reviews_us_Mobile_Apps_v1_00.tsv",
    "amazon_reviews_us_Personal_Care_Appliances_v1_00.tsv",
    "amazon_reviews_us_Digital_Video_Download_v1_00.tsv",
    "amazon_reviews_us_Wireless_v1_00.tsv",
    "amazon_reviews_us_Watches_v1_00.tsv",
    "amazon_reviews_us_Digital_Ebook_Purchase_v1_01.tsv",
    "amazon_reviews_us_Kitchen_v1_00.tsv",
    "amazon_reviews_us_PC_v1_00.tsv",
    "amazon_reviews_us_Beauty_v1_00.tsv",
    "amazon_reviews_us_Luggage_v1_00.tsv",
    "amazon_reviews_us_Lawn_and_Garden_v1_00.tsv",
    "amazon_reviews_us_Home_v1_00.tsv",
    "amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv",
    "amazon_reviews_us_Electronics_v1_00.tsv",
    "amazon_reviews_us_Office_Products_v1_00.tsv",
    "amazon_reviews_us_Tools_v1_00.tsv",
    "amazon_reviews_us_Home_Improvement_v1_00.tsv",
    "amazon_reviews_us_Books_v1_02.tsv",
    "amazon_reviews_us_Musical_Instruments_v1_00.tsv",
    "amazon_reviews_us_Health_Personal_Care_v1_00.tsv",
    "amazon_reviews_us_Video_v1_00.tsv",
    "amazon_reviews_us_unified.tsv",
    "amazon_reviews_us_Pet_Products_v1_00.tsv",
    "amazon_reviews_us_Shoes_v1_00.tsv",
    "amazon_reviews_us_Major_Appliances_v1_00.tsv",
    "amazon_reviews_us_Mobile_Electronics_v1_00.tsv",
    "amazon_reviews_us_Video_DVD_v1_00.tsv",
    "amazon_reviews_us_Home_Entertainment_v1_00.tsv",
    "amazon_reviews_us_Gift_Card_v1_00.tsv",
    "amazon_reviews_us_Music_v1_00.tsv",
    "amazon_reviews_us_Furniture_v1_00.tsv",
    "amazon_reviews_us_Baby_v1_00.tsv",
    "amazon_reviews_us_Automotive_v1_00.tsv",
    "amazon_reviews_us_Digital_Ebook_Purchase_v1_00.tsv",
    "amazon_reviews_us_Outdoors_v1_00.tsv",
    "amazon_reviews_us_Books_v1_01.tsv",
]


def worker_job(queue, bucket):
    while True:
        (filename, out_file_path) = queue.get()
        print(f"└── downloading {filename} -> {out_file_path}")
        bucket.download_file(filename, out_file_path)
        u = out_file_path[:-3]
        with gzip.open(out_file_path, 'rb') as cfile:
            with open(u, 'wb') as ufile:
                shutil.copyfileobj(cfile, ufile)
            os.remove(out_file_path)
        queue.task_done()


def download_dataset(out_path: str, threads: int, s3):
    bucket = s3.Bucket(BUCKET_NAME)
    tsv_out_path = os.path.join(out_path, BUCKET_FOLDER_NAME)

    q = Queue()
    print(f"Set up worker for every thread available ({threads})")
    for i in range(threads):
        t = Thread(target=worker_job, args=(q, bucket,))
        t.daemon = True
        t.start()

    if not os.path.exists(tsv_out_path):
        os.makedirs(tsv_out_path)
    for obj in bucket.objects.filter(Prefix=os.path.join(BUCKET_FOLDER_NAME, "amazon_reviews_us")):
        filename = obj.key
        out_file_path = os.path.join(out_path, filename)
        q.put((filename, out_file_path))

    q.join()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, required=True,
                        help="Output folder location")
    parser.add_argument("--threads", type=int, default=7,
                        help="Numbers of threads in the queue")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    s3 = boto3.resource('s3')
    download_dataset(args.out, args.threads, s3)
