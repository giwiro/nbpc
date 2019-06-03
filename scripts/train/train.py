import argparse
import numpy as np
import tensorflow as tf

from nbpc.train.model import Model
from nbpc.train.tfidf import TfIdf

TRAIN_TEST_RATIO = 0.8
BATCH_SIZE = 128


def train(dataset_path: str, bin_dumps: str):
    data = np.genfromtxt(dataset_path, delimiter="\t", dtype=str)
    texts = data[:, 1]
    tfidf = TfIdf(bin_dumps)
    model = Model(0.05)

    # Create sparse matrix if does not exist
    if tfidf.sparse_matrix is None:
        tfidf.create_matrix(texts)

    num_features = tfidf.sparse_matrix.shape[1]
    num_labels = TfIdf.get_num_categories()

    print("Mapping categories to numbers")
    labels = [TfIdf.get_category_id(x) for x in data[:, 0]]

    print("Generate train and validate indexes")
    # Split up data set into train/test
    train_idxs = np.random.choice(tfidf.sparse_matrix.shape[0],
                                  round(TRAIN_TEST_RATIO * tfidf.sparse_matrix.shape[0]),
                                  replace=False)
    validate_idxs = np.array(list(set(range(tfidf.sparse_matrix.shape[0])) - set(train_idxs)))

    print("Create train texts vector")
    # Dataset for training
    train_texts = tfidf.sparse_matrix[train_idxs]
    print("Create train categories vector")
    train_y_labels = np.array(labels)[train_idxs]

    # Dataset for validating the model
    print("Create validate texts vector")
    validate_texts = tfidf.sparse_matrix[validate_idxs]
    print("Create validate categories vector")
    validate_y_labels = np.array(labels)[validate_idxs]

    # Initialize placeholders
    x_text = tf.placeholder(shape=[BATCH_SIZE, num_features], dtype=tf.float32)
    y_label = tf.placeholder(shape=[BATCH_SIZE, num_labels], dtype=tf.float32)

    # Variables.
    weights = tf.Variable(tf.truncated_normal([num_features, num_labels]))
    biases = tf.Variable(tf.zeros([num_labels]))

    # Decision Boundary
    logits = Model.decision_boundary_fn(x_text, weights, biases)

    # Loss: instead using least mean square, we use cross entropy
    loss = Model.loss_fn(y_label, logits)

    # Optimizer
    optimizer = model.optimizer_fn(loss)

    # Predictions
    train_prediction = Model.predict(logits)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, required=True,
                        help="Dataset location")
    parser.add_argument("--bin-dumps", type=str, required=True,
                        help="Location of the binary dumps")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    train(args.dataset, args.bin_dumps)
