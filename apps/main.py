from pyspark.sql import SparkSession
from pyspark.sql.functions import col,date_format

def init_spark():
    sql = SparkSession.builder\
        .appName("marvel-bodyguard")\
        .config("spark.jars", "/opt/spark-apps/postgresql-42.2.22.jar")\
        .getOrCreate()
    sc = sql.sparkContext
    return sql,sc

def main():
    print("spark job ran begin...")
    url = "jdbc:postgresql://demo-database:5432/marvel_lake"
    properties = {
        "user": "postgres",
        "password": "casa1234",
        "driver": "org.postgresql.Driver"
    }
    file = "/opt/spark-data/marvel_test_data.json"
    sql,sc = init_spark()
    
    print("spark job completed...")
  
if __name__ == '__main__':
  main()