#!/bin/bash
# init

function pause(){
   read -p "$*"
}

clear

echo "Extração dos dados de inspeção dos estabelecimentos"
echo
pause "Pressione ENTER continuar"


$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/extract_boston_food_establishment_inspection.py

echo "Extração dos estabelecimentos ativos"
echo
pause "Pressione ENTER continuar"

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/extract_boston_active_food_establishment.py

echo "Tratamento dos Dados Extraídos"
echo
pause "Pressione ENTER continuar"

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/transformation_boston.py

echo "Aplicação do Modelo nos Dados Tratados"
echo
pause "Pressione ENTER continuar"

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/predict.py

echo "Exibição dos Dados Previstos Gravados no HIVE"
echo
pause "Pressione ENTER continuar"

beeline -u jdbc:hive2://elephant:10000 -n labdata -e "SELECT * FROM predict"
