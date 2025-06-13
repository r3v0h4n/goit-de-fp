import requests
import os
from pyspark.sql import SparkSession


def download_data(filename):
    url = f"https://ftp.goit.study/neoversity/{filename}.csv"
    local_path = os.path.join("./data/landing/", f"{filename}.csv")
    os.makedirs("./data/landing/", exist_ok=True)
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_path, "wb") as file:
            file.write(response.content)
        print(f"Downloaded: {url} -> {local_path}")
    else:
        raise Exception(f"Failed to download {url}: {response.status_code}")
    return local_path

def process_to_bronze(filename):
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
    local = os.path.join("./data/landing/", f"{filename}.csv")
    bronze = os.path.join("./data/bronze/", filename)

    os.makedirs(bronze, exist_ok=True)

    data = spark.read.option("header", "true").csv(local)
    data.write.parquet(bronze, mode="overwrite")
    
    print(f"Saved to Bronze: {bronze}")

if __name__ == "__main__":
    files = ["athlete_bio", "athlete_event_results"]
    for file in files:
        download_data(file)
        process_to_bronze(file)