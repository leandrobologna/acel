#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
Script de extração dos dados de inspeções dos estabelecimentos de comida da
cidade de Boston (USA).

Fonte:
https://data.boston.gov/datastore/odata3.0/f1e13724-284d-478c-b8bc-ef042aa5b70b?$format=json"""

# Importando os pacotes
from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_timestamp

from requests import get

# Definindo as constantes
url = 'https://data.boston.gov/datastore/odata3.0/f1e13724-284d-478c-b8bc-ef042aa5b70b?$format=json'
# Inicializando a sessão do spark
spark = SparkSession \
    .builder\
    .config(conf=SparkConf())\
    .appName('extract_boston_active_food_establishment')\
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Definindo a estrutura final dos dados (pensar em uma forma dinâmica)
schema = StructType([
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

# Conectando a url onde os dados estão disponíveis
response = get(url)

# Transformando os dados da url no formato JSON
json_data = response.json()

# Criando um dataframe a partir do JSON
df = sqlContext.createDataFrame(json_data['value'], schema)

# Alterando os campos de datas para timestamp
df = df\
  .withColumn('LicenseAddDtTm',  to_timestamp('LicenseAddDtTm', 'yyyy-MM-dd HH:mm:ss'))

# Particionando dataframe para que grave apenas 10 arquivos
df = df.repartition(10)

# Gravar os dados no HDFS
df\
    .write\
    .mode("overwrite")\
    .saveAsTable("extract_boston_active_food_establishment")
