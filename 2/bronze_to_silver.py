import os
import re


from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType



def clear_text(text):
    return re.sub(r"[^a-zA-Z0-9,.\'\" ]", "", str(text))

def process_to_silver(filename):
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    bronze = os.path.join("./data/bronze/", filename)
    silver = os.path.join("./data/silver/", filename)

    os.makedirs(silver, exist_ok=True)

    cleared_text_udf = udf(clear_text, StringType())
    data = spark.read.parquet(bronze)

    for col in data.columns:
        if data.schema[col].dataType == StringType():
            data = data.withColumn(col, cleared_text_udf(data[col]))

    data = data.dropDuplicates()
    data.write.parquet(silver, mode="overwrite")

    print(f"Saved to Silver: {silver}")

if __name__ == "__main__":
    files = ["athlete_bio", "athlete_event_results"]
    for file in files:
        process_to_silver(file)