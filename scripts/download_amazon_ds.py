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
    parser.add_argument("--threads", type=int, default=2,
                        help="Numbers of threads in the queue")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    s3 = boto3.resource('s3')
    download_dataset(args.out, args.threads, s3)
