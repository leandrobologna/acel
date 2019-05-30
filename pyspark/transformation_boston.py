#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
Script de transformação dos dados de inspeções e licenças dos estabelecimentos de comida da
cidade de Boston (USA).

Fonte:
boston_active_food_establishment
boston_food_establishment_inspections

Para rodar:
SHELL
$SPARK_HOME/bin/pyspark --conf spark.hadoop.hive.metastore.uris=thrift://10.30.30.21:9083

SUBMIT
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/transformation_boston.py
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, datediff, row_number, lower, when, lit
from pyspark.sql import Window

# HDFS root directory
#HDFS_SOURCE_FOLDER="file:///home/carlos_bologna/Dropbox/GitHub/acel_consulting/parquet_files"
HDFS_SOURCE_FOLDER = "hdfs://elephant:8020/user/labdata/"

# Spark session
spark = SparkSession.builder \
    .config(conf=SparkConf()) \
    .appName("transformation_boston") \
    .config("hive.metastore.uris", "thrift://10.30.30.21:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Read Parquet Data from HDFS
#establishment = spark.read.parquet('{0}/boston_active_food_establishment'.format(HDFS_SOURCE_FOLDER))

inspections = spark.read.parquet('{0}/boston_food_establishment_inspections'.format(HDFS_SOURCE_FOLDER))

# Data Cleanning
inspections = inspections\
    .filter(
        (col('viollevel') != '1919') &
        (col('property_id').isNull() == False) &
        (col('violstatus') != ' '))\
    .withColumn('city', lower(col('city')))\
    .withColumn('state', lower(col('state')))\
    .withColumn('result', lower(col('result')))\
    .fillna('viol_unknown', subset = ['violstatus'])

# Inspections Number So Far (join with property_id and resultdttm)
w = Window.partitionBy("property_id").orderBy("resultdttm")

inspections_distinct = inspections\
	.select('property_id', 'issdttm', 'state', 'location', 'licensecat', 'descript', 'city', 'zip', 'resultdttm')\
	.distinct()\
	.withColumn('inspections_so_far', row_number().over(w))\
    .withColumn('last_inspections', lag('resultdttm').over(w))\
    .withColumn('days_since_last_inspect', datediff(col('resultdttm'), col('last_inspections')).cast('int'))\
    .fillna(99999999, subset = ['days_since_last_inspect'])

violations = inspections\
	.groupBy('property_id', 'resultdttm')\
	.pivot('violstatus')\
	.count()\
	.withColumnRenamed('Fail', 'viol_fail')\
	.withColumnRenamed('Pass', 'viol_pass')

# Joins
inspections_historic = inspections_distinct\
    .join(violations, ['property_id', 'resultdttm'], 'leftouter')\
    .fillna(0)\
    .withColumn('fail', when(col('viol_fail') > 0, 1).otherwise(0))\
    .withColumn('last_viol_fail', lag('viol_fail').over(w))\
    .withColumn('last_viol_pass', lag('viol_pass').over(w))\
    .fillna(99999999)

#inspections_historic.repartition(1).write.mode("overwrite").csv("{}/train_data".format(HDFS_SOURCE_FOLDER), sep=';', header = 'true')

inspections_historic\
    .repartition(1) \
    .write \
    .mode("overwrite") \
    .saveAsTable("inspections_historic")