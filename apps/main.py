from pyspark.sql import SparkSession
import requests
import hashlib
import time
import json
import os

PAGE_LIMIT = 100
PUBLIC_KEY = os.environ["MARVEL_PUBLIC_KEY"]
PRIVATE_KEY = os.environ["MARVEL_PRIVATE_KEY"]


def fetch_marvel(endpoint, save_dir):
    ts = int(time.time())
    hashed = hashlib.md5('{0}{1}{2}'.format(ts, PRIVATE_KEY, PUBLIC_KEY).encode('utf-8')).hexdigest()
    offset = 0
    total = 1

    while offset < total:
        full_url = 'https://gateway.marvel.com/v1/public/{0}?apikey={1}&hash={2}&ts={3}&limit={4}&offset={5}'.format(endpoint, PUBLIC_KEY, hashed, ts, PAGE_LIMIT, offset)
        res = requests.get(full_url).json()
        file_write_time = int(time.time())
        char_ts_dir = '{0}/case/landing/{1}/uploaded_at={2}/'.format(save_dir, endpoint, file_write_time)
        char_ts_file = '{0}/case/landing/{1}/uploaded_at={2}/{3}.json'.format(save_dir, endpoint, file_write_time, endpoint)
        
        os.makedirs(char_ts_dir, exist_ok=True)
        with open(char_ts_file, 'w') as f:
            f.write(json.dumps(res) + '\n')
        offset+=PAGE_LIMIT
        total = max(total, res.get('data').get('total'))

def init_spark():
    spksql = SparkSession.builder\
        .appName("marvel-bodyguard")\
        .getOrCreate()
    sc = spksql.sparkContext
    return spksql,sc

def main():
    print("spark job begin...")
    file = "/opt/spark-data/marvel_test_data.json"
    sql,sc = init_spark()
    fetch_marvel("characters", "/opt/spark-data")
    fetch_marvel("events", "/opt/spark-data")
    
    print("spark job completed...")
  
if __name__ == '__main__':
  main()