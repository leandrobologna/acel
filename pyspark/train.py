#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
Script de treino do modelo preditivo.

Fonte:
Fonte de dados proveniente do script 'transformation_boston.py'

Para rodar:
SHELL
$SPARK_HOME/bin/pyspark --conf spark.hadoop.hive.metastore.uris=thrift://10.30.30.21:9083

SUBMIT
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/train.py
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# HDFS root directory
#HDFS_SOURCE_FOLDER="file:///home/carlos_bologna/Dropbox/GitHub/acel_consulting/"
#HDFS_SOURCE_FOLDER = "hdfs://elephant:8020/user/labdata/"

# Spark session
spark = SparkSession.builder \
    .config(conf=SparkConf()) \
    .appName("model") \
    .config("hive.metastore.uris", "thrift://10.30.30.21:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Read Parquet Data from HDFS
#inspections = spark.read.csv('{0}/{1}/train_data'.format(HDFS_SOURCE_FOLDER, 'parquet_files'), sep=';', header=True)
inspections = spark.table('inspections_historic')

#Transformations
inspections = inspections\
    .withColumn('inspections_so_far', col('inspections_so_far').cast('int'))\
    .withColumn('days_since_last_inspect', col('days_since_last_inspect').cast('int'))\
    .withColumn('last_viol_fail', col('last_viol_fail').cast('int'))\
    .withColumn('last_viol_pass', col('last_viol_pass').cast('int'))\
    .withColumn('fail', col('fail').cast('int'))\
    .withColumn('city', when(col('city').isNull(), lit('boston')).otherwise(col('city')))\
    .withColumn('zip', when(col('zip').isNull(), lit('02116')).otherwise(col('zip')))

# Drop Columns
inspections = inspections\
    .drop('location', 'last_inspections', 'resultdttm', 'issdttm', 'property_id',
        'state', 'viol_fail', 'viol_pass')

# Split Train and Test data
train, test = inspections.randomSplit([0.7, 0.3], seed=1407)

train = train.fillna(0)

# One Hot Enconde
licensecatIndex = StringIndexer(inputCol="licensecat", outputCol="licensecatIndex")
licensecatVec = OneHotEncoder(inputCol="licensecatIndex", outputCol="licensecatVec")

descriptIndex = StringIndexer(inputCol="descript", outputCol="descriptIndex")
descriptVec = OneHotEncoder(inputCol="descriptIndex", outputCol="descriptVec")

cityIndex = StringIndexer(inputCol="city", outputCol="cityIndex")
cityVec = OneHotEncoder(inputCol="cityIndex", outputCol="cityVec")

zipIndex = StringIndexer(inputCol="zip", outputCol="zipIndex")
zipVec = OneHotEncoder(inputCol="zipIndex", outputCol="zipVec")

feature_list = ['inspections_so_far', 'days_since_last_inspect',
    'last_viol_fail', 'last_viol_pass',
    'licensecatVec', 'descriptVec', 'cityVec', 'zipVec']

assembler = VectorAssembler(inputCols=feature_list, outputCol="features")

rf = RandomForestClassifier(labelCol="fail", featuresCol="features")

# Pipeline
pipeline = Pipeline(stages=[
        licensecatIndex, licensecatVec,
        descriptIndex, descriptVec,
        cityIndex, cityVec,
        zipIndex, zipVec,
        assembler,
        rf]
)

model = pipeline.fit(train)

# Predict
predictions = model.transform(test)

# Evaluate model
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="fail")
evaluator.evaluate(predictions) #AUC

# Save model
model.save('{}/models/predict_ranking'.format(HDFS_SOURCE_FOLDER))
