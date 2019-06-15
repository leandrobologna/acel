#!/bin/bash
# init

function pause(){
   read -p "$*"
}

clear

figlet "Extract :"
echo
pause "Pressione ENTER continuar"


$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/extract_boston_food_establishment_inspection.py

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/extract_boston_active_food_establishment.py

hdfs dfs -ls boston_food_establishment_inspections

hdfs dfs -ls boston_active_food_establishment

echo
figlet "Transform :"
echo
pause "Pressione ENTER continuar"

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/transformation_boston.py

beeline -u jdbc:hive2://elephant:10000 -n labdata -e "SELECT * FROM inspections_historic LIMIT 10"

echo
figlet "Predict :"
echo
pause "Pressione ENTER continuar"

$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster /home/labdata/acel_consulting/pyspark/predict.py

beeline -u jdbc:hive2://elephant:10000 -n labdata -e "SELECT businessname, address, descript, city, proba FROM predict LIMIT 30"
