import tensorflow as tf
import numpy as np

DEFAULT_LEARNING_RATE = 0.05
DEFAULT_EPOCS = 6500


class Model:
    def __init__(self, learning_rate: float = None, epocs: int = None):
        self.learning_rate = learning_rate or DEFAULT_LEARNING_RATE
        self.epocs = epocs or DEFAULT_EPOCS

    @staticmethod
    def decision_boundary_fn(x_text, weights, biases):
        return tf.matmul(x_text, weights) + biases

    @staticmethod
    def loss_fn(y_label, decision_boundary):
        return tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits_v2(
            y_label, decision_boundary))

    def optimizer_fn(self, loss):
        return tf.train.GradientDescentOptimizer(self.learning_rate).minimize(loss)

    @staticmethod
    def predict(decision_boundary, biases=None):
        if biases is None:
            return tf.nn.softmax(decision_boundary)
        else:
            return tf.nn.softmax(decision_boundary + biases)

    @staticmethod
    def accuracy(predictions, labels):
        correctly_predicted = np.sum(np.argmax(predictions, 1) == np.argmax(labels, 1))
        accu = (100.0 * correctly_predicted) / predictions.shape[0]
        return accu
