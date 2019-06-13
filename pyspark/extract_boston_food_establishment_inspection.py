#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
Script de extração dos dados de inspeções dos estabelecimentos de comida da
cidade de Boston (USA).

Fonte:
https://data.boston.gov/dataset/food-establishment-inspections

Para rodar:
SHELL
$SPARK_HOME/bin/pyspark --conf spark.hadoop.hive.metastore.uris=thrift://10.30.30.21:9083

SUBMIT
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/extract_boston_food_establishment_inspection.py

"""

# Importando os pacotes
DEMO=True

from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_timestamp, max, col

from requests import get

# Strings de conexões
url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=4582bec6-2b4f-4f9e-bc55-cbaa73117f4c'
hdfs = "hdfs://elephant:8020/user/labdata/"
local = "/home/cbologna/Dropbox/GitHub/acel_consulting/parquet_files/"

# Inicializando a sessão do spark
spark = SparkSession \
    .builder\
    .config(conf=SparkConf())\
    .appName('extract_boston_food_establishment_inspections')\
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Read tables
#df_empty = spark.read.parquet(local + 'boston_food_establishment_inspections')

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

df_empty = spark.createDataFrame([], schema)

# Criando um dataframe vazio com o esquema criado acima
df_empty = spark.createDataFrame([], schema)

# Carregando os metadados da API
metadados_json = get(url).json()

# Carregando alguns metados da API em constantes
limit = 5000
maxInteractions = 3 if DEMO else int(metadados_json['result']['total'] / limit + 1)
offset = 0
interaction = 0

while (interaction <= maxInteractions):
    # Carregando os dados da API em um dicionário
    if (df_empty.count() == 0):
        dados_json = get(url, params={'limit': limit}).json()
        dados_json = dados_json['result']
        dados_json = dados_json['records']
    else:
        dados_json = get(url, params={'limit': limit, 'offset': offset}).json()
        dados_json = dados_json['result']
        dados_json = dados_json['records']
    # Criando um dataframe com o dicionário anteriormente criado
    df_json = spark.createDataFrame(dados_json, schema)
    # Adicionando os dados do dataframe no dataframe vazio anteriormente Criando
    df_empty = df_empty.unionAll(df_json)
    # Incrementando a interação
    interaction += 1
    # Incrementando o offset
    offset = limit * interaction

# Alterando os campos de datas para timestamp
df = df_empty\
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
   .option("path",hdfs + 'boston_food_establishment_inspections')\
   .saveAsTable("boston_food_establishment_inspections")

# df\
#     .write\
#     .mode("overwrite")\
#     .option("path",hdfs + 'boston_food_establishment_inspections')\
#     .saveAsTable("boston_food_establishment_inspections")
