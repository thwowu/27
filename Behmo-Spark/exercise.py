"""
~~~~ Instructions

Your goal, in this exercise, is to train and evaluate an image classification
model. You are going to create the following workflow:

1. Load the image labels from the training and the testing dataset: labels are
   stored in the 'data/oxford-IIIT-pet-dataset/annotations/' folder.

2. Select a random subset of training and testing images, for cross-validation.

3. Compute and save the features for each image (load in case the features were
   already computed). The code to compute the features of an image is given in the
   'image_features.py' script. Feel free to adapt this code to your particular use
   case. It is suggested to store the features in the dedicated 'features' folder.

4. Train a classification model on the training dataset (e.g: Naive Bayes for
   multi-class or Linear SVM for binary classification)

5. Evaluate the quality of the classification model on the testing dataset.

This script should be run as follows:

    ./spark-2.1.0-bin-hadoop2.7/bin/spark-submit ./src/exercise.py ./data/oxford-IIIT-pet-dataset/

~~~~ Recommendations

Steps 1-2 could be performed by hand, without Spark, since we don't have much
data, but please don't do that.

You are encouraged to prototype your code on a small subset of the full dataset;
you may achieve this by selecting a small number of random samples in step #2,
or by creating a new dataset manually.

If you do not have enough RAM to train the classification model on the full
dataset, you will have to train your model on a Spark cluster. To do so, you
will have to share data using a local, pseudo-cluster HDFS.

To run the feature computation step, you will need to install the following
requirements:

    # You probably want to do this in a virtualenv
    pip install keras tensorflow h5py pillow numpy

Do not try to obtain the best possible classification scores at all cost;
instead, focus on producing correct, optimized Spark code.

~~~ Documentation

- Spark standard library: https://spark.apache.org/docs/latest/programming-guide.html#transformations
- Classification with Spark: https://spark.apache.org/docs/latest/mllib-classification-regression.html
- Multi-label classification evaluation: https://spark.apache.org/docs/latest/mllib-evaluation-metrics.html#multilabel-classification


    please use the following code to compile:

     ./spark-2.1.0-bin-hadoop2.7/bin/spark-submit ./src/exercise.py ./data/oxford-IIIT-pet-dataset/annotations/list.txt

"""
import os
import sys
import pyspark
import numpy as np
from os import listdir
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector
import random

from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel, LogisticRegressionWithSGD
from pyspark.mllib.classification import SVMWithSGD, SVMModel

DATASET_ROOT = os.path.join(os.path.dirname(__file__), "..", "data", "oxford-IIIT-pet-dataset")
ANNOTATIONS_ROOT = os.path.join(DATASET_ROOT, "annotations")
IMAGE_ROOT = os.path.join(DATASET_ROOT, "images")
FEATURES_ROOT = os.path.join(DATASET_ROOT, "features")
directory_path = "/Users/Thwowu/spark/spark-features"
ke = "/Users/Thwowu/spark/spark-features/"

def main():
    sc = pyspark.SparkContext()


    # acquire the RDD containing with file name + label
    train = sc.textFile(sys.argv[1]) 
    train_words = train.map(lambda line: (line.split()[0], line.split()[2] ) )
  

    # extrait the files that we own its features

    #text_file = open("/Users/Thwowu/spark/spark-features/output.txt", "w")
    files = os.listdir(directory_path)
    malis = []

    """ 
    --------  This is for testing by the first two observations to fit the model -------

    ds = train_words.filter(lambda (x,y): x == files[0][:-8]).map(lambda (x,y): y).collect()
    target_string = ''.join(ds)
    target = int(target_string)
    feat = np.load(ke + files[0])
    feature = feat.tolist()
    malis.append([target,feature])
    
    ds1 = train_words.filter(lambda (x,y): x == files[1][:-8]).map(lambda (x,y): y).collect()
    target_string1 = ''.join(ds1)
    target1 = int(target_string1)
    feat1 = np.load(ke + files[1])
    feature1 = feat1.tolist()
    malis.append([target1,feature1])

    print(malis)
    kkk = sc.parallelize(malis).map(lambda x: LabeledPoint(x[0], x[1]) )
    model = LogisticRegressionWithSGD.train(kkk)

    labelsAndPreds = kkk.map(lambda p: (p.label, model.predict(p.features)))
    testErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(kkk.count())
    print("Evaluating the model ")
    print("Testing Error = " + str(testErr))

    """


    """  the loop function:
    objective: make a long list, that contains label, and features
    label: integer
    features: numpy array

    """
    lens = len(np.load(ke + files[0]))


    for name in files:

      # filter function to select the ones that we have their features
      ds = train_words.filter(lambda (x,y): x == name[:-8]).map(lambda (x,y): y).collect()
      target_string = ''.join(ds)

      # bypass the error if the label is N/A
      if target_string == "":
        continue
      target = int(target_string) - 1 # labels must start from 0
      # https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/classification/LogisticRegression.scala#L330

      #loading the npy files and check if it has the same length as others
      feat = np.load(ke + name)
      if len(feat) != lens:
        continue
      feature = feat.tolist()

      #text_file.write(name[:-8] + "\n")
      malis.append([target,feature])
    #text_file.close()


    ################### Corresponding to Question 2 #######################

    # (new) split the train/test set 90% : 10%
    tes, trai = sc.parallelize(malis).randomSplit([0.1, 0.9], seed=12345)

    # making them into Labeled Point format
    training  = trai.map(lambda x: LabeledPoint(x[0], x[1])  )
    test  = tes.map(lambda x: LabeledPoint(x[0], x[1]) )

    ################### Corresponding to Question 4 and 5 #######################

    # model training : Logistics Regression
    model1 = LogisticRegressionWithSGD.train(training)
    model2 = SVMWithSGD.train(training, iterations=100)
    
    # Evaluating the model on training data : Logistics Regression
    labelsAndPreds1 = test.map(lambda p: (p.label, model1.predict(p.features)))
    testErr1 = labelsAndPreds1.filter(lambda (v, p): v != p).count() / float(test.count())
    print("Evaluating the model !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("Testing Error = " + str(testErr1))

    # Evaluating the model on training data  : Support Vector Machines
    labelsAndPreds2 = test.map(lambda p: (p.label, model2.predict(p.features)))
    testErr2 = labelsAndPreds2.filter(lambda (v, p): v != p).count() / float(test.count())
    print("Evaluating the model !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("Testing Error = " + str(testErr2))

    # Checking and Print out the judgement which machine learning method is better
    if (testErr1 > testErr2):
      print("SVM is better")
    else:
      print("Logistics is better")

if __name__ == "__main__":
    main()

    # To profile your application with Spark UI, you want to prevent it from finishing
    # DON'T FORGET to comment this line when submitting your app on a Spark cluster!
    # raw_input("Press ctrl+c to exit")
    
"""
Deadline Notice: 

  assignment due may 31st
  sparkp@behmo.com
  send exercise.py as attachment and your name

"""
