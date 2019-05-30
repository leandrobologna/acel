DRIVER_MEMORY=4g
EXECUTOR_MEMORY=4g

sudo aws s3 rm --recursive s3://prevent-datalake-us/keyed/consensus/prod-dadosprocesso
sudo spark-submit \
  --name "keyed_processos_consensus" \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory $DRIVER_MEMORY \
  --executor-memory $EXECUTOR_MEMORY \
  /tmp/keyed_processos_consensus.py
