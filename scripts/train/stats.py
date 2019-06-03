import argparse
from typing import List, Tuple
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer

TOP_N_WORDS = 20


def show_dataset_sample_size(dataset_samples: int):
    print("(*) Dataset number of samples\n-----------------------")
    print(dataset_samples)
    print("\n")


def show_total_number_words(total_number_words: int):
    print("(*) Total number of words\n-----------------------")
    print(total_number_words)
    print("\n")


def show_top_n_words(top_words_freq: List[Tuple[str, int]]):
    print(f"(*) Top {TOP_N_WORDS} frequent words\n-----------------------")
    for w in top_words_freq:
        print(f"{w[0]}\t|\t{w[1]}")
    print("\n")


def stats(dataset_path: str):
    data = np.genfromtxt(dataset_path, delimiter="\t", dtype=str)
    texts = data[:, 1]
    cv = CountVectorizer()
    cv_fit = cv.fit(texts)

    bag_of_words = cv_fit.transform(texts)
    sum_words = bag_of_words.sum(axis=0)

    words_freq = [(word, sum_words[0, idx]) for word, idx in cv_fit.vocabulary_.items()]
    words_freq = sorted(words_freq, key=lambda x: x[1], reverse=True)

    # print(words_freq[:TOP_N_WORDS])
    print("\n")
    show_dataset_sample_size(bag_of_words.shape[0])
    show_total_number_words(bag_of_words.shape[1])
    show_top_n_words(words_freq[:TOP_N_WORDS])


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, required=True,
                        help="Dataset location")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    stats(args.dataset)
