# BEES Data Engineering test run doc:

### make .env file with your marvel API keys:<br>
MARVEL_PUBLIC_KEY="&lt;your public key&gt;"<br>
MARVEL_PRIVATE_KEY"&lt;your private key&gt;"<br>

### build the containers
docker-compose build<br>

### start the containers
docker-compose up -d<br>

### connecting to spark container (master or worker(s))
docker exec -it bees_spark-master_1 bash<br>

### execute task (from inside the bees_spark-master_1 container)
/opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 1G --executor-memory 1G /opt/spark-apps/main.py<br>

### generate visualizations/tables
python3 dev_viz.py<br>

cd visualizations dir<br>

open each generated table in browser
