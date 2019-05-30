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

"""

from pyspark import SparkConf
from pyspark.sql import SparkSession

# HDFS root directory
#HDFS_SOURCE_FOLDER="file:///home/carlos_bologna/Dropbox/GitHub/acel_consulting/"
HDFS_SOURCE_FOLDER = "hdfs://elephant:8020/user/labdata/"

# Spark session
spark = SparkSession.builder \
    .config(conf=SparkConf()) \
    .appName("predict") \
    .config("hive.metastore.uris", "thrift://10.30.30.21:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Read Parquet Data from HDFS
establishment = spark.read.parquet('{0}/boston_active_food_establishment'.format(HDFS_SOURCE_FOLDER))
#inspections = spark.read.parquet('{0}/boston_food_establishment_inspections'.format(HDFS_SOURCE_FOLDER))

inspections_historic = spark.table('inspections_historic')


# Select
establishment = establishment\
    .select('LICENSECAT', 'DESCRIPT', 'CITY', 'ZIP') \
    .withColumnRenamed('LICENSECAT', 'licensecat') \
    .withColumnRenamed('DESCRIPT', 'descript') \
    .withColumnRenamed('CITY', 'city') \
    .withColumnRenamed('ZIP', 'zip')

# parei aqui: pegar o registro mais recente da tabela inspections_historic e fazer join com establishment