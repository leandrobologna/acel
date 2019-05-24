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

# Inicializando a sessão do spark
spark = SparkSession \
    .builder\
    .config(conf=SparkConf())\
    .appName('exctract_tables_empty')\
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')
