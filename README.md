# BEES Data Engineering test

## Run doc:

### make a .env file with:<br> 
MARVEL_PUBLIC_KEY="asdfghjkl1234567890"<br>
MARVEL_PRIVATE_KEY"qwertyuiop0987654321"<br>

### build the containers
docker-compose build<br>

### start the containers
docker-compose up -d

### connecting to spark container (master or worker(s))
docker exec -it bees_spark-master_1 bash

### execute task
/opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 1G --executor-memory 1G /opt/spark-apps/main.py