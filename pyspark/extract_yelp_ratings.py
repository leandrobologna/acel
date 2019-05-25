#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
Script de extração dos dados de avaliação dos estabelecimentos de comida da cidade
de Boston (USA).

Fonte:
yelp (fusion api)
"""
# Importando os pacotes
from pyspark import SparkConf
from pyspark.sql import SparkSession

from json import load
from requests import get

# Inicializando a sessão
spark = SparkSession \
    .builder\
    .config(conf=SparkConf())\
    .appName('extract_yelp_ratings')\
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Lendo o arquivo JSON com os metadados da API do yelp
with open('/home/labdata/acel_consulting/document.json') as json_file:
    json_file = load(json_file)

# Instanciando as contantes
api_host = json_file['apiHost']
api_client = json_file['client']
api_key = json_file['key']

state = "MA"

# Carregando os dataframes
active_establishment = spark.read.parquet("hdfs://elephant:8020/user/labdata/extract_boston_active_food_establishment")
