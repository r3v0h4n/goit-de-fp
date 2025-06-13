import requests
import os
import re

from datetime import datetime

from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, avg, current_timestamp, col

from airflow.operators.python import PythonOperator
from airflow import DAG


def download_data(filename):
    url = f"https://ftp.goit.study/neoversity/{filename}.csv"
    local = os.path.join("./data/landing/", f"{filename}.csv")

    os.makedirs("./data/landing/", exist_ok=True)

    response = requests.get(url)
    if response.status_code == 200:
        with open(local, "wb") as file:
            file.write(response.content)

        print(f"Downloaded {url}: {local}")
    else:
        raise Exception(f"Failed to download {url}: {response.status_code}")
    return local

def process_to_bronze(filename):
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    local = os.path.join("./data/landing/", f"{filename}.csv")
    bronze = os.path.join("./data/bronze/", filename)

    os.makedirs(bronze, exist_ok=True)

    data = spark.read.option("header", "true").csv(local)
    data.write.parquet(bronze, mode="overwrite")

    print(f"Saved to Bronze: {bronze}")
    data.show()

def process_to_silver(filename):
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    bronze = os.path.join("./data/bronze/", filename)
    silver = os.path.join("./data/silver/", filename)

    os.makedirs(silver, exist_ok=True)

    def clear_text(text):
        return re.sub(r"[^a-zA-Z0-9,.\'\" ]", "", str(text))

    cleared_text_udf = udf(clear_text, StringType())
    data = spark.read.parquet(bronze)
    for col_name in data.columns:
        if data.schema[col_name].dataType == StringType():
            data = data.withColumn(col_name, cleared_text_udf(data[col_name]))

    data = data.dropDuplicates()
    data.write.parquet(silver, mode="overwrite")
    print(f"Saved to Silver: {silver}")
    data.show()

def process_to_gold():
    athlete_bio = spark.read.format("parquet").load("silver/athlete_bio")
    athlete_events = spark.read.format("parquet").load("silver/athlete_event_results")

    join = athlete_events.alias("events").join(
        athlete_bio.alias("bio"),
        col("events.athlete_id") == col("bio.athlete_id")
    )

    selected = join.select(
        col("events.sport").alias("sport"),
        col("events.medal").alias("medal"),
        col("bio.sex").alias("sex"),
        col("bio.country_noc").alias("country_noc"),
        col("bio.height").alias("height"),
        col("bio.weight").alias("weight")
    )

    aggregated = selected.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

    aggregated.show()
    aggregated.write.mode("overwrite").parquet("gold/avg_stats")


default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="kachurovskyi_data_lake_etl",
    default_args=default_args,
    description="Kachurovskyi ETL pipeline for Data Lake",
    schedule_interval=None,
    start_date=datetime(2025, 6, 13),
    catchup=False,
) as dag:

    def landing_to_bronze():
        files = ["athlete_bio", "athlete_event_results"]
        for file in files:
            download_data(file)
            process_to_bronze(file)

    def bronze_to_silver():
        files = ["athlete_bio", "athlete_event_results"]
        for file in files:
            process_to_silver(file)

    def silver_to_gold():
        process_to_gold()

    task_landing_to_bronze = PythonOperator(
        task_id="landing_to_bronze",
        python_callable=landing_to_bronze,
    )

    task_bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=bronze_to_silver,
    )

    task_silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
    )

    task_landing_to_bronze >> task_bronze_to_silver >> task_silver_to_gold
