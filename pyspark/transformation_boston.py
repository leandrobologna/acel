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
from pyspark.sql.functions import col, lag, datediff
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

# Last Inspection
w = Window.partitionBy("property_id").orderBy("resultdttm")

inspections = inspections\
	.withColumn('lastinspectiondt', lag('resultdttm').over(w))\
	.withColumn('dayssincelastinspect', datediff(col('resultdttm'), col('lastinspectiondt')).cast('int'))\
	.fillna(99999999)

# Distinct
inspections_distinct = inspections\
    .select('licenseno', 'issdttm', 'expdttm', 'licstatus',
        'licensecat', 'descript', 'city', 'zip', 'state', 'address', 'licstatus',
        'licensecat', 'location')\
    .distinct()

inspections_distinct = inspections\
    .select('property_id', 'city', 'zip', 'state', 'address')\
    .distinct()






# Groups
inspections_count = inspections\
	.select('property_id', 'resultdttm')\
	.distinct()\
	.groupBy('property_id')\
	.count()\
	.withColumnRenamed('count', 'inspections_count')

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
