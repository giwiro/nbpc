import argparse
from functools import partial
from os import path
from multiprocessing.pool import Pool as ProcessPool
from tqdm import tqdm
from requests import get

url_prefix = "https://s3.amazonaws.com/amazon-reviews-pds/tsv"

filenames = [
    "amazon_reviews_us_Wireless_v1_00.tsv.gz",
    "amazon_reviews_us_Watches_v1_00.tsv.gz",
    "amazon_reviews_us_Video_Games_v1_00.tsv.gz",
    "amazon_reviews_us_Video_DVD_v1_00.tsv.gz",
    "amazon_reviews_us_Video_v1_00.tsv.gz",
    "amazon_reviews_us_Toys_v1_00.tsv.gz",
    "amazon_reviews_us_Tools_v1_00.tsv.gz",
    "amazon_reviews_us_Sports_v1_00.tsv.gz",
    "amazon_reviews_us_Software_v1_00.tsv.gz",
    "amazon_reviews_us_Shoes_v1_00.tsv.gz",
    "amazon_reviews_us_Pet_Products_v1_00.tsv.gz",
    "amazon_reviews_us_Personal_Care_Appliances_v1_00.tsv.gz",
    "amazon_reviews_us_PC_v1_00.tsv.gz",
    "amazon_reviews_us_Outdoors_v1_00.tsv.gz",
    "amazon_reviews_us_Office_Products_v1_00.tsv.gz",
    "amazon_reviews_us_Musical_Instruments_v1_00.tsv.gz",
    "amazon_reviews_us_Music_v1_00.tsv.gz",
    "amazon_reviews_us_Mobile_Electronics_v1_00.tsv.gz",
    "amazon_reviews_us_Mobile_Apps_v1_00.tsv.gz",
    "amazon_reviews_us_Major_Appliances_v1_00.tsv.gz",
    "amazon_reviews_us_Luggage_v1_00.tsv.gz",
    "amazon_reviews_us_Lawn_and_Garden_v1_00.tsv.gz",
    "amazon_reviews_us_Kitchen_v1_00.tsv.gz",
    "amazon_reviews_us_Jewelry_v1_00.tsv.gz",
    "amazon_reviews_us_Home_Improvement_v1_00.tsv.gz",
    "amazon_reviews_us_Home_Entertainment_v1_00.tsv.gz",
    "amazon_reviews_us_Home_v1_00.tsv.gz",
    "amazon_reviews_us_Health_Personal_Care_v1_00.tsv.gz",
    "amazon_reviews_us_Grocery_v1_00.tsv.gz",
    "amazon_reviews_us_Gift_Card_v1_00.tsv.gz",
    "amazon_reviews_us_Furniture_v1_00.tsv.gz",
    "amazon_reviews_us_Electronics_v1_00.tsv.gz",
    "amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz",
    "amazon_reviews_us_Digital_Video_Download_v1_00.tsv.gz",
    "amazon_reviews_us_Digital_Software_v1_00.tsv.gz",
    "amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv.gz",
    "amazon_reviews_us_Digital_Ebook_Purchase_v1_01.tsv.gz",
    "amazon_reviews_us_Digital_Ebook_Purchase_v1_00.tsv.gz",
    "amazon_reviews_us_Camera_v1_00.tsv.gz",
    "amazon_reviews_us_Books_v1_02.tsv.gz",
    "amazon_reviews_us_Books_v1_01.tsv.gz",
    "amazon_reviews_us_Books_v1_00.tsv.gz",
    "amazon_reviews_us_Beauty_v1_00.tsv.gz",
    "amazon_reviews_us_Baby_v1_00.tsv.gz",
    "amazon_reviews_us_Automotive_v1_00.tsv.gz",
    "amazon_reviews_us_Apparel_v1_00.tsv.gz",
]


def worker_job(filename: str, out_path: str):
    url = path.join(url_prefix, filename)
    out = path.join(out_path, filename)
    with open(out, "wb") as file:
        # get request
        response = get(url)
        # write to file
        file.write(response.content)
    return


def download_dataset(out_path: str, processes: int):
    ppool = ProcessPool(processes)
    worker_job_new = partial(worker_job, out_path=out_path)
    print("Start downloading ")
    with tqdm(total=len(filenames)) as progress:
        for _ in tqdm(ppool.imap_unordered(worker_job_new, filenames)):
            progress.update()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, required=True,
                        help="Output folder location")
    # parser.add_argument("--db-uri", type=str, default=Database.default_uri,
    #                     help="Full uri of the database")
    parser.add_argument("--processes", type=int, default=10,
                        help="Numbers of processes in the pool")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    download_dataset(args.out, args.processes)
