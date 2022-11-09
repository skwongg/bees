# BEES Data Engineering test

## Run doc:

### make .env file with your marvel API keys:<br>
MARVEL_PUBLIC_KEY="&lt;your public key&gt;"<br>
MARVEL_PRIVATE_KEY"&lt;your private key&gt;"<br>

### build the containers
docker-compose build<br>

### start the containers
docker-compose up -d

### connecting to spark container (master or worker(s))
docker exec -it bees_spark-master_1 bash

### execute task
/opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 1G --executor-memory 1G /opt/spark-apps/main.py