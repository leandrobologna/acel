#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
Script de extração dos dados de inspeções dos estabelecimentos de comida da
cidade de Boston (USA).

Fonte:
https://data.boston.gov/dataset/food-establishment-inspections/resource/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c
"""

# Importando os pacotes
from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_timestamp
hdfs
from requests import get

# Definindo as constantes
url = "https://data.boston.gov/datastore/odata3.0/4582bec6-2b4f-4f9e-bc55-cbaa73117f4c?$top=5&$format=json"
hdfs = "hdfs://elephant:8020/user/labdata/boston_food_establishment_inspections"

# Inicializando a sessão do spark
spark = SparkSession \
    .builder\
    .config(conf=SparkConf())\
    .appName('extract_boston_food_establishment_inspections')\
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Definindo a estrutura final dos dados
schema = StructType([
    StructField('_id', IntegerType(), True),
    StructField('businessname', StringType(), True),
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

# Conectando a url onde os dados estão disponíveis
response = get(url)

# Transformando os dados da url no formato JSON
json_data = response.json()

# Criando um dataframe a partir do JSON
df = spark.createDataFrame(json_data['value'], schema)

# Alterando os campos de datas para timestamp
df = df\
  .withColumn('resultdttm',  to_timestamp('resultdttm', 'yyyy-MM-dd HH:mm:ss')) \
  .withColumn('issdttm',  to_timestamp('issdttm', 'yyyy-MM-dd HH:mm:ss')) \
  .withColumn('violdttm',  to_timestamp('issdttm', 'yyyy-MM-dd HH:mm:ss')) \
  .withColumn('expdttm',  to_timestamp('issdttm', 'yyyy-MM-dd HH:mm:ss'))

# Particionando dataframe para que grave apenas 10 arquivos
df = df.repartition(10)

# Gravar os dados no HDFS
df\
    .write\
    .mode("overwrite")\
    .option("path",hdfs)\
    .saveAsTable("extract_boston_food_establishment_inspections")
