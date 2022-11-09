from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, explode, datediff, year, expr, to_date\
    , collect_set, countDistinct, desc

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
        full_url = 'https://gateway.marvel.com/v1/public/{0}?apikey={1}&hash={2}&ts={3}&limit={4}&offset={5}'\
            .format(endpoint, PUBLIC_KEY, hashed, ts, PAGE_LIMIT, offset)
        res = requests.get(full_url).json()
        file_write_time = int(time.time())
        ts_dir_name = '{0}/case/landing/{1}/uploaded_at={2}/'.format(save_dir, endpoint, file_write_time)
        ts_file_name = '{0}/case/landing/{1}/uploaded_at={2}/{3}.json'.format(save_dir, endpoint, file_write_time, endpoint)

        os.makedirs(ts_dir_name, exist_ok=True)
        with open(ts_file_name, 'w') as f:
            f.write(json.dumps(res) + '\n')
        offset+=PAGE_LIMIT
        total = max(total, res.get('data').get('total'))


def silver_layer(spark_session, endpoint, save_dir):
    print("Silver layer processing start...")

    silver_dir_name = '{0}/case/silver/{1}/'.format(save_dir, endpoint)
    silver_file_name = '{0}/case/silver/{1}/{2}.parquet'.format(save_dir, endpoint, endpoint)
    
    os.makedirs(silver_dir_name, exist_ok=True)


    mypath = '{0}/case/landing/{1}/'.format(save_dir, endpoint) #this is all timestamp dirs
    try: 
        for endpoint_dir in os.listdir(mypath):
            f = open(mypath+endpoint_dir+"/{0}.json".format(endpoint))
            results = json.load(f).get("data").get("results")

            if endpoint == "characters":
                columns = ["id", "name", "description", "comics_count", "events_count", "stories_count",\
                    "series_count", "date_of_upload"]
                data = []
                for result in results:
                    data.append((
                        result.get("id"),
                        result.get("name"),
                        result.get("description"),
                        result.get("comics").get("available"),
                        result.get("events").get("available"),
                        result.get("stories").get("available"),
                        result.get("series").get("available"),
                        endpoint_dir.split("=")[-1]
                    ))
                df = spark_session.createDataFrame(data, columns)
                df.write.mode('append').parquet(silver_file_name)
            
            elif endpoint == "events":
                columns = ["id", "title", "description", "list_of_characters", "end", "start", "date_of_upload"]
                data = []
                for result in results:
                    data.append((
                        result.get("id"),
                        result.get("title"),
                        result.get("description"),
                        [char.get("name") for char in result.get("characters").get("items")],
                        result.get("end"),
                        result.get("start"),
                        endpoint_dir.split("=")[-1]   
                    ))
                df = spark_session.createDataFrame(data, columns)
                df.write.mode('append').parquet(silver_file_name)
                
    except FileNotFoundError:
        print("No files found to process...")
    
    print("Silver layer processing complete...")

def gold_layer(spark_session, save_dir):
    print("Gold layer processing start...")
    gold_dir_name = '{0}/case/gold/characters'.format(save_dir)
    char_participation = '{0}/case/gold/characters/characters_participation.parquet'.format(save_dir)
    char_count_each_year = '{0}/case/gold/characters/characters_count_each_year.parquet'.format(save_dir)
    total_years_top10_chars = '{0}/case/gold/characters/total_years_top10_appeared.parquet'.format(save_dir)

    os.makedirs(gold_dir_name, exist_ok=True)

    ## first one -- total days in events for each of the top 10 most appearing characters
    ep = spark_session.read.parquet("/opt/spark-data/case/silver/events/events.parquet")
    ep.createOrReplaceTempView("ep")
    event_df = ep.select(explode(ep.list_of_characters).alias("character_name"), datediff(ep.end, ep.start)\
        .alias("datediff")).groupBy("character_name").sum("datediff")

    cp = spark_session.read.parquet("/opt/spark-data/case/silver/characters/characters.parquet")
    cp.createOrReplaceTempView("cp")
    cp.select(cp.id, cp.name, cp.comics_count, cp.series_count, cp.stories_count, cp.events_count)\
        .join(event_df, cp.name == event_df.character_name).orderBy(cp.comics_count, ascending=False).limit(10).show()
    cp.write.mode('overwrite').parquet(char_participation)


    ## second one -- the number of characters appearing each year in events
    eventParquet = spark_session.read.parquet("/opt/spark-data/case/silver/events/events.parquet")
    eventParquet.createOrReplaceTempView("eventParquet")
    eventParquet.select(explode(eventParquet.list_of_characters).alias("character_name"),\
        eventParquet.id, eventParquet.title, eventParquet.description, to_date(eventParquet.start)\
        .alias("start_year"), to_date(eventParquet.end).alias("end_year"))\
        .withColumn('generanged_date', explode(expr('sequence(start_year,  end_year, interval 1 year)')))\
        .groupBy(year("generanged_date").alias("generanged_year")).agg(countDistinct("character_name")\
            .alias("character_count")).orderBy("generanged_year").show(n=100)
    eventParquet.write.mode('overwrite').parquet(char_count_each_year)
    
    # replace countDistinct with collect_set for character names (replace line 120+121 with line 125-126 below)
    # .groupBy(year("generanged_date").alias("generanged_year")).agg(collect_set("character_name")\
    #       .alias("character_set")).orderBy("generanged_year").show(n=1000)


    ## third one -- the total number of years each of the top 10 most appearing characters appeared in events
    cpdf = spark_session.read.parquet("/opt/spark-data/case/silver/characters/characters.parquet")
    cpdf.createOrReplaceTempView("cpdf")
    char_p = cpdf.select(cpdf.name.alias("character_name")).orderBy(cpdf.comics_count, ascending=False).limit(10)

    epdf = spark_session.read.parquet("/opt/spark-data/case/silver/events/events.parquet")
    epdf.createOrReplaceTempView("epdf")
    epff = epdf.select(explode(epdf.list_of_characters).alias("character_name"), epdf.id, epdf.title, epdf.description, to_date(epdf.start)\
        .alias("start_year"), to_date(epdf.end).alias("end_year"))\
        .withColumn('generanged_date', explode(expr('sequence(start_year,  end_year, interval 1 year)')))
    epff.join(char_p, 'character_name', 'inner').groupBy("character_name").count().show()
    epff.write.mode('overwrite').parquet(total_years_top10_chars)
    print("Gold layer processing end...")




def main():
    print("Spark job begin...")
    sp_sess = SparkSession.builder.appName("marvel-bodyguard").getOrCreate()

    # landing code
    fetch_marvel("characters", "/opt/spark-data")
    fetch_marvel("events", "/opt/spark-data")

    # silver code
    silver_layer(sp_sess, "characters", "/opt/spark-data")
    silver_layer(sp_sess, "events", "/opt/spark-data")

    # gold layer
    gold_layer(sp_sess, "/opt/spark-data")

    print("Spark job completed...")
  
if __name__ == '__main__':
  main()