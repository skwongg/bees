# BEES Data Engineering test

docker-compose build
docker-compose up -d

## example syntax, spark job can be run from master or worker after connecting to it via docker exec -it
docker exec -it bees_spark-master_1 bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 1G --executor-memory 1G /opt/spark-apps/main.py