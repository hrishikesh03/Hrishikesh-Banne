import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from keras.models import Sequential 
from keras.layers import Dense 
from keras.optimizers import Adam 

from sklearn.metrics import f1_score
from keras.layers import Dense
from sklearn import metrics

df = pd.read_csv("Iris.csv")

df = pd.get_dummies(df, columns=['Species'])

X = df.values[:, 1:5]
y = df.values[:, 5:8]

def normalize(array):
    arr_min = array.min(axis=(0, 1))
    arr_max = array.max(axis=(0, 1))
    return (array - arr_min) / (arr_max - arr_min)
X = normalize(X)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42)

def Build_Network():
    model = Sequential()
###Input layer, input 4 values -> output 7 values using relu
    model.add(Dense(7, input_shape=(4,), activation='relu')) 
    
###Hidden layer, input 7 values (output of first layer) -> output 8 values using relu
    model.add(Dense(8, activation='relu'))
    
###Output layer, input 8 values -> output 3 values (3 are the classes) using softmax Layers are fully connected that means output of one layer is fully connected to the each node of next layer
    model.add(Dense(3, activation='softmax')) 
    
###categorical_crossentropy Computes the crossentropy loss between the labels and predictions. Using the optimizer Adam.
    model.compile(loss='categorical_crossentropy', optimizer=Adam(), metrics=['accuracy'])
    
    return model


model = Build_Network()

###Training the model, by providing input values, output labels, number of epochs, batch size define how much data should be picked up in one iteration, and defining the test data as the validation data'''

results = model.fit(X_train,y_train, epochs=100, batch_size=8, 
                    validation_data=(X_test, y_test))

results.history.keys()
#Plotting the loss of training data and validation data
plt.figure(figsize=(10, 10))
plt.title("Learning curve")
plt.plot(results.history["loss"], label="training_loss")
plt.plot(results.history["val_loss"], label="validation_loss")
plt.plot( np.argmin(results.history["val_loss"]), np.min(results.history["val_loss"]), marker="x", color="r", label="best model")
plt.xlabel("Epochs")
plt.ylabel("log_loss")
plt.legend()

plt.figure(figsize=(10, 10))
plt.plot(results.history["accuracy"], label="training_accuracy")
plt.plot(results.history["val_accuracy"], label="testing_accuracy")
plt.xlabel("Epochs")
plt.ylabel("accuracy")
plt.legend()
