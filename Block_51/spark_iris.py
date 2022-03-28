import numpy as np
import pandas as pd
import pyspark
import os
import urllib
import sys

from pyspark.sql.functions import *
from pyspark.ml.classification import *
from pyspark.ml.evaluation import *
from pyspark.ml.feature import *

from azureml.logging import get_azureml_logger



# start Spark session
spark = pyspark.sql.SparkSession.builder.appName('Iris').getOrCreate()


# Взять данные iris.csv
data = spark.createDataFrame(pd.read_csv('iris.csv', header=None, names=['sepal-length', 'sepal-width', 'petal-length', 'petal-width', 'class']))
print("Первые 10 строк набора данных Iris:")
data.show(10)

# векторизовать все числовые столбцы в один столбец признаков
feature_cols = data.columns[:-1]
assembler = pyspark.ml.feature.VectorAssembler(inputCols=feature_cols, outputCol='features')
data = assembler.transform(data)

# преобразовать текстовые метки в индексы
data = data.select(['features', 'class'])
label_indexer = pyspark.ml.feature.StringIndexer(inputCol='class', outputCol='label').fit(data)
data = label_indexer.transform(data)

# выбераем только функции и столбец меток
data = data.select(['features', 'label'])
print("Reading for machine learning")
data.show(10)

# изменяем скорость регуляризации, и вы, вероятно, получите другую точность.
reg = 0.01
# Загружаем скорость регуляризации из аргумента, если он присутствует
if len(sys.argv) > 1:
    reg = float(sys.argv[1])

# Частота регуляризации
run_logger.log("Частота регуляризации, reg)

# используем логистическую регрессию для обучения на тренировочном наборе
train, test = data.randomSplit([0.70, 0.30])
lr = pyspark.ml.classification.LogisticRegression(regParam=reg)
model = lr.fit(train)

# предсказывем на тестовом наборе
prediction = model.transform(test)
print("Prediction")
prediction.show(10)

# оценим точность модели с помощью набора тестов
evaluator = pyspark.ml.evaluation.MulticlassClassificationEvaluator(metricName='accuracy')
accuracy = evaluator.evaluate(prediction)

print()
print('#####################################')
print('Скорость регуляризации: {}'.format(reg))
print("Точность".format(accuracy))
print('#####################################')
print()

# log accuracy
