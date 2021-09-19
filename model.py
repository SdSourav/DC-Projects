import tensorflow as tf
from tensorflow.keras.models import Model
import numpy as np

X = np.array([1, 2])  # as an example

input_layer = tf.keras.Input(shape=(X.shape))
hidden_layer = tf.keras.layers.Dense(4, activation='relu')(input_layer)
output_layer = tf.keras.layers.Dense(1, activation='sigmoid')(hidden_layer)

model = Model(inputs=input_layer, outputs=output_layer)
