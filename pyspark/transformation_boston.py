#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
Script de transformação dos dados de inspeções e licenças dos estabelecimentos de comida da
cidade de Boston (USA).

Fonte:
boston_active_food_establishment
boston_food_establishment_inspections
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, datediff, row_number, lower, when, lit
from pyspark.sql import Window

# HDFS root directory
HDFS_SOURCE_FOLDER="file:///home/cbologna/Dropbox/GitHub/acel_consulting/parquet_files"
#HDFS_SOURCE_FOLDER = "hdfs://elephant:8020/user/labdata/"

# Spark session
spark = SparkSession.builder \
    .config(conf=SparkConf()) \
    .appName("refined_consulta_por_especialidade") \
    .enableHiveSupport() \
    .getOrCreate()

# Read Parquet Data from HDFS
establishment = spark.read.parquet('{0}/boston_active_food_establishment'.format(HDFS_SOURCE_FOLDER))

inspections = spark.read.parquet('{0}/boston_food_establishment_inspections'.format(HDFS_SOURCE_FOLDER))

# Data Cleanning
inspections = inspections\
    .withColumn('city', lower(col('city')))\
    .withColumn('state', lower(col('state')))\
    .withColumn('result', lower(col('result')))\
    .withColumn('status',
        when(
            (col('result') == 'he_pass') |
            (col('result') == 'passviol') |
            (col('result') == 'pass'),
        lit('pass')).otherwise(lit('fail')))


# Inspections Number So Far (join with property_id and resultdttm)
w = Window.partitionBy("property_id").orderBy("resultdttm")

inspections_distinct = inspections\
	.select('property_id', 'issdttm', 'state', 'location', 'licensecat', 'descript', 'city', 'zip', 'resultdttm', 'status')\
	.distinct()\
	.withColumn('inspections_so_far', row_number().over(w))\
    .withColumn('last_inspections', lag('resultdttm').over(w))\
    .withColumn('days_since_last_inspect', datediff(col('resultdttm'), col('last_inspections')).cast('int'))\
    .withColumn('last_status', lag('status').over(w))\
    .fillna(99999999, subset = ['days_since_last_inspect']) \
    .fillna('unknown', subset = ['last_status'])






inspections_failled = inspections\
	.filter(col('violstatus') == 'Fail')\
	.select('property_id', 'resultdttm')\
	.distinct()\
	.groupBy('property_id')\
	.count()\
	.withColumnRenamed('count', 'inspections_failled')

violation_count = inspections\
	.filter(col('viollevel').isin('*', '**', '***'))\
	.groupBy('property_id', 'viollevel')\
	.count()\
	.groupBy('property_id')\
	.pivot('viollevel')\
	.sum('count')\
	.withColumnRenamed('*', 'violationlevel_1')\
	.withColumnRenamed('**', 'violationlevel_2')\
	.withColumnRenamed('***', 'violationlevel_3')




# Joins
inspections_historic = inspections_stablishment\
    .join(inspections_count, 'property_id', 'leftouter')
