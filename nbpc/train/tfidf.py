import os
import pickle
from typing import List
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer

DUMP_FILE = "tfidf.bin"


class TfIdf:
    def __init__(self, dump_dir_path: str):
        self.vectorizer = None
        self.sparse_matrix = None
        self.dump_dir_path = dump_dir_path
        self.dump_path = os.path.join(self.dump_dir_path, DUMP_FILE)

        if os.path.isfile(self.dump_path):
            self.load()

    @staticmethod
    def tokenize(text: str):
        return nltk.word_tokenize(text)

    @staticmethod
    def gen_label_vector(idx: int):
        return [0 if x != idx else 1 for x in range(TfIdf.get_num_categories())]

    @staticmethod
    def get_num_categories():
        return 7

    @staticmethod
    def get_category_id(category: str):
        if category == "clothing":
            return 0
        elif category == "makeup_beauty":
            return 1
        elif category == "cameras_video":
            return 2
        elif category == "game_consoles":
            return 3
        elif category == "shoes":
            return 4
        elif category == "game_consoles":
            return 5
        elif category == "clothing_accessories":
            return 6

    def load(self):
        with open(self.dump_path, "rb") as f:
            data_structure = pickle.load(f)
            self.vectorizer, self.sparse_matrix = data_structure["vectorizer"], data_structure["sparse_matrix"]

    def save(self):
        with open(self.dump_path, "wb") as f:
            data_structure = {"vectorizer": self.vectorizer, "sparse_matrix": self.sparse_matrix}
            pickle.dump(data_structure, f)

    def create_matrix(self, texts: List[str]):
        # self.vectorizer = TfidfVectorizer(stop_words="english", tokenizer=TfIdf.tokenize)
        print("Creating matrix...")
        self.vectorizer = TfidfVectorizer(stop_words="english")
        self.sparse_matrix = self.vectorizer.fit_transform(texts)
        self.save()
