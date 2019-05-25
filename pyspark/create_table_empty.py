#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
Script que tem como objetivo criar na primeira vez as tabelas boston_food_establishment_inspections
e boston_active_food_establishment vazias.
"""

# Importando os pacotes
from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Inicializando a sessão do spark
spark = SparkSession \
    .builder\
    .config(conf=SparkConf())\
    .appName('exctract_tables_empty')\
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Strings de conexões
hdfs = "hdfs://elephant:8020/user/labdata/"
local = "/home/leandro/Documentos/pessoal/fia/acel_consulting/parquet_files/"

# Estruturas finais das tabelas vazias
schema_inspections = StructType([
    StructField('_id', IntegerType(), True),
    StructField('zip', StringType(), True),
    StructField('state', StringType(), True),
    StructField('resultdttm', StringType(), True),
    StructField('issdttm', StringType(), True),
    StructField('address', StringType(), True),
    StructField('licenseno', StringType(), True),
    StructField('property_id', StringType(), True),
    StructField('city', StringType(), True),
    StructField('violdttm', StringType(), True),
    StructField('violation', StringType(), True),
    StructField('licstatus', StringType(), True),
    StructField('result', StringType(), True),
    StructField('licensecat', StringType(), True),
    StructField('statusdate', StringType(), True),
    StructField('dbaname', StringType(), True),
    StructField('namefirst', StringType(), True),
    StructField('violstatus', StringType(), True),
    StructField('viollevel', StringType(), True),
    StructField('legalowner', StringType(), True),
    StructField('location', StringType(), True),
    StructField('expdttm', StringType(), True),
    StructField('comments', StringType(), True),
    StructField('violdesc', StringType(), True),
    StructField('descript', StringType(), True),
    StructField('namelast', StringType(), True)
])

schema_active = StructType([
    StructField('_id', IntegerType(), True),
    StructField('LicenseAddDtTm', StringType(), True),
    StructField('State', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('LICENSECAT', StringType(), True),
    StructField('Property_ID', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('DBAName', StringType(), True),
    StructField('DESCRIPT', StringType(), True),
    StructField('LICSTATUS', StringType(), True),
    StructField('BusinessName', StringType(), True),
    StructField('dayphn', StringType(), True),
    StructField('CITY', StringType(), True),
    StructField('ZIP', StringType(), True)
])

# Criando os dataframes
df_inspections = spark.createDataFrame([], schema_inspections)
df_active = spark.createDataFrame([], schema_active)

# Gravando os arquivos no HDFS
df_inspections \
    .write \
    .mode("overwrite")\
    .option('path', hdfs + 'boston_food_establishment_inspections')\
    .saveAsTable('boston_food_establishment_inspections')

df_active \
    .write \
    .mode("overwrite")\
    .option('path', hdfs + 'boston_active_food_establishment')\
    .saveAsTable('boston_active_food_establishment')
