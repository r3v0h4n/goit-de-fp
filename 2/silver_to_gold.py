import os

from pyspark.sql.functions import avg, current_timestamp
from pyspark.sql import SparkSession


def process_to_gold():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    athlete_bio = os.path.join("./data/silver/", "athlete_bio")
    athlete_event = os.path.join("./data/silver/", "athlete_event_results")
    gold = os.path.join("./data/gold/", "avg_stats")
    
    os.makedirs(gold, exist_ok=True)

    bio_data = spark.read.parquet(athlete_bio)
    event_data = spark.read.parquet(athlete_event)

    result_data = event_data.join(bio_data, "athlete_id").groupBy(
        "sport", "medal", "sex", "country_noc"
    ).agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )
    
    result_data.write.parquet(gold, mode="overwrite")
    print(f"Saved to Gold: {gold}")


if __name__ == "__main__":
    process_to_gold()