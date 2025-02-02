#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
Script de treino do modelo preditivo.

Fonte:
Fonte de dados proveniente do script 'transformation_boston.py'
Modelo preditivo models/predict_ranking

Para rodar:
SHELL
$SPARK_HOME/bin/pyspark --conf spark.hadoop.hive.metastore.uris=thrift://10.30.30.21:9083

SUBMIT
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/predict.py

Columns:
'licensecat', 'descript', 'city', 'zip', 'inspections_so_far', 'days_since_last_inspect', 'last_viol_fail', 'last_viol_pass'

Comandos Uteis:
$ sudo /home/labdata/cluster-conf-labdata/scripts/restart_all_services.sh
$ sudo service hive-server2 status
$ sudo service impala status
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Window, Row
from pyspark.sql.functions import row_number, current_date, datediff, when, \
    col, lower, regexp_replace, lit, trim
from pyspark.ml import PipelineModel
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType

# HDFS root directory
HDFS_SOURCE_FOLDER = "hdfs://elephant:8020/user/labdata/"
#HDFS_SOURCE_FOLDER = "file:///home/carlos_bologna/Dropbox/GitHub/acel_consulting/parquet_files"
MODEL_SOURCE_FOLDER = "hdfs://elephant:8020/user/labdata/models"
#MODEL_SOURCE_FOLDER = "file:///home/carlos_bologna/Dropbox/GitHub/acel_consulting/models"

# Spark session
spark = SparkSession.builder \
    .config(conf=SparkConf()) \
    .appName("predict") \
    .config("hive.metastore.uris", "thrift://10.30.30.21:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Read Parquet Data from HDFS
establishment = spark.read.parquet('{0}/boston_active_food_establishment'.format(HDFS_SOURCE_FOLDER))

inspections_historic = spark.table('inspections_historic')
#inspections_historic = spark.read.csv('{0}/train_data'.format(HDFS_SOURCE_FOLDER), sep=';', header=True)

# Drops
inspections_historic = inspections_historic \
    .drop('last_viol_fail', 'last_viol_pass')

# Select
establishment = establishment\
    .withColumnRenamed('Property_ID', 'property_id') \
    .withColumnRenamed('LICENSECAT', 'licensecat') \
    .withColumnRenamed('DESCRIPT', 'descript') \
    .withColumnRenamed('CITY', 'city') \
    .withColumnRenamed('ZIP', 'zip') \
    .withColumn('zip', when(col('zip').isNull(), lit('02116')).otherwise(col('zip')))\
    .withColumn('city', lower(col('city')))\
    .withColumn('city', regexp_replace(col('city'), '\/', ''))\
    .withColumn('city', regexp_replace(col('city'), 'downtownfinancial', 'financial')) \
    .withColumn('city', regexp_replace(col('city'), '  ', ' '))\
    .withColumn('city', when(col('city') == '', lit('boston')).otherwise(col('city'))) \
    .withColumn('city', when(col('city') == ' ', lit('boston')).otherwise(col('city')))
    
    

# Window
w = Window.partitionBy("property_id").orderBy(col("resultdttm").desc())

inspections_distinct = inspections_historic \
    .withColumn('row_number', row_number().over(w)) \
    .where(col('row_number') == 1) \
    .withColumnRenamed('viol_fail', 'last_viol_fail') \
    .withColumnRenamed('viol_pass', 'last_viol_pass') \
    .withColumn('days_since_last_inspect', datediff(current_date(), col('resultdttm')).cast('int')) \
    .select('property_id', 'inspections_so_far', 'days_since_last_inspect', 'last_viol_fail', 'last_viol_pass')

# Join
data = establishment \
    .join(inspections_distinct, 'property_id', 'leftouter')

# Transform
data = data \
    .withColumn('inspections_so_far', col('inspections_so_far').cast('int')) \
    .withColumn('last_viol_fail', col('last_viol_fail').cast('int')) \
    .withColumn('last_viol_pass', col('last_viol_pass').cast('int')) \
    .fillna(99999999, subset = ['days_since_last_inspect']) \
    .fillna(0)

# Load Model
model = PipelineModel.load('{}/predict_ranking'.format(MODEL_SOURCE_FOLDER))

# Predict
predictions = model.transform(data)

# Transformation
#def getProba(v):
#    return str(v[1])
   

#firstelement=udf(getProba, StringType())

firstelement=udf(lambda x: float(x[1]), FloatType())

preds = predictions \
    .withColumn('proba', firstelement(col('probability'))) \
    .drop('inspections_so_far', 'days_since_last_inspect', 'last_viol_fail',
        'last_viol_pass', 'licensecatIndex', 'licensecatVec', 'descriptIndex',
        'descriptVec', 'cityIndex', 'cityVec', 'zipIndex', 'zipVec', 'features',
        'rawPrediction', 'prediction', 'probability') 

preds \
    .write \
    .mode("overwrite") \
    .saveAsTable("predict")