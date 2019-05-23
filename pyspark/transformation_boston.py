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

# HDFS root directory
HDFS_SOURCE_FOLDER="/home/carlos_bologna/Dropbox/GitHub/acel_consulting/parquet_files"

# Spark session
spark = SparkSession.builder \
    .config(conf=SparkConf()) \
    .appName("refined_consulta_por_especialidade") \
    .enableHiveSupport() \
    .getOrCreate()

# Read Parquet Data from HDFS
establishment = spark.read.parquet('{0}/boston_active_food_establishment'.format(HDFS_SOURCE_FOLDER))
inspections = spark.read.parquet('{0}/boston_food_establishment_inspections'.format(HDFS_SOURCE_FOLDER))

inspections_grp = inspections.groupBy('property_id', 'violdttm', '')
