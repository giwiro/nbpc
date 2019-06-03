import tensorflow as tf

DEFAULT_LEARNING_RATE = 0.05


class Model:
    def __init__(self, learning_rate: float):
        self.learning_rate = learning_rate or DEFAULT_LEARNING_RATE

    @staticmethod
    def decision_boundary_fn(x_text, weights, biases):
        return tf.matmul(x_text, weights) + biases

    @staticmethod
    def loss_fn(y_label, decision_boundary):
        return tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(
            labels=y_label, logits=decision_boundary))

    def optimizer_fn(self, loss):
        return tf.train.GradientDescentOptimizer(self.learning_rate).minimize(loss)

    @staticmethod
    def predict(decision_boundary):
        return tf.nn.softmax(decision_boundary)
