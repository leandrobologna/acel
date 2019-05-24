#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
Script de extração dos dados de licenças ativas dos estabelecimentos de comida da
cidade de Boston (USA).

Fonte:
https://data.boston.gov/datastore/
"""

# Importando os pacotes
from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_timestamp

from requests import get

# Definindo as constantes
url = "https://data.boston.gov/api/3/action/datastore_search?resource_id=f1e13724-284d-478c-b8bc-ef042aa5b70b"
hdfs = "hdfs://elephant:8020/user/labdata/boston_active_food_establishment"

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

# Criando um dataframe vazio com o esquema criado acima
df_empty = spark.createDataFrame([], schema)

# Carregando os metadados da API
metadados_json = get(url).json()

# Carregando alguns metados da API em constantes
limit = 300
maxInteractions = int(metadados_json['result']['total'] / limit + 1)
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
  .withColumn('LicenseAddDtTm',  to_timestamp('LicenseAddDtTm', 'yyyy-MM-dd HH:mm:ss'))

# Particionando dataframe para que grave apenas 10 arquivos
df = df.repartition(10)

# Gravar os dados no HDFS
# df \
# 	.write\
# 	.mode("overwrite")\
# 	.option("path", hdfs)\
# 	.saveAsTable("boston_active_food_establishment")

df\
    .write\
    .mode("overwrite")\
    .option("path",'/home/leandro/Documentos/pessoal/fia/acel_consulting/parquet_files/boston_active_food_establishment')\
    .saveAsTable("boston_active_food_establishment")
