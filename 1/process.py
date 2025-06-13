import os
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'


@dataclass
class DatabaseSettings:
    host: str = "217.61.57.46"
    port: int = 3306
    name: str = "olympic_dataset"
    username: str = "neo_data_admin"
    password: str = "Proyahaxuqithab9oplp"
    driver: str = "com.mysql.cj.jdbc.Driver"

    @property
    def jdbc_url(self):
        return f"jdbc:mysql://{self.host}:{self.port}/{self.name}"

    
@dataclass
class KafkaSettings:
    servers = ["77.81.230.104:9092"]
    user: str = "admin"
    password: str = "VawEzo1ikLtrA8Ug8THa"
    protocol: str = "SASL_PLAINTEXT"
    mechanism: str = "PLAIN"

    @property
    def jaas_config(self):
        return (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{self.user}" password="{self.password}";'
        )


class SparkProcessor:
    def __init__(self, kafka: KafkaSettings, database: DatabaseSettings, checkpoint_dir: str = "checkpoint"):
        self.kafka = kafka
        self.database = database
        self.checkpoint_dir = checkpoint_dir
        self.spark = self._initialize_spark_session()
        self.schema = self._define_schema()

    def _initialize_spark_session(self):
        jar_path = os.path.abspath("mysql-connector.jar")
        if not os.path.exists(jar_path):
            raise FileNotFoundError(f"MySQL Connector JAR not found at {jar_path}")

        return (
            SparkSession.builder
            .config("spark.jars", jar_path)
            .config("spark.driver.extraClassPath", jar_path)
            .config("spark.executor.extraClassPath", jar_path)
            .appName("KafkaToSparkProcessor")
            .master("local[*]")
            .getOrCreate()
        )

    def _define_schema(self):
        return StructType([
            StructField("athlete_id", IntegerType(), True),
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("timestamp", StringType(), True),
        ])

    def read_from_mysql(self, table: str, partition_column: str) -> DataFrame:
        try:
            return (
                self.spark.read.format("jdbc")
                .option("url", self.database.jdbc_url)
                .option("driver", self.database.driver)
                .option("dbtable", table)
                .option("user", self.database.username)
                .option("password", self.database.password)
                .option("partitionColumn", partition_column)
                .option("lowerBound", 1)
                .option("upperBound", 1000000)
                .option("numPartitions", 10)
                .load()
            )
        except Exception as e:
            print(f"Error reading from table {table}: {e}")
            raise

    def write_to_mysql(self, df: DataFrame, table: str):
        try:
            (df.write
             .format("jdbc")
             .option("url", self.database.jdbc_url)
             .option("driver", self.database.driver)
             .option("dbtable", table)
             .option("user", self.database.username)
             .option("password", self.database.password)
             .mode("append")
             .save())
            
            print(f"Data successfully written to table {table}")
        except Exception as e:
            print(f"Error writing to table {table}: {e}")
            raise

    def write_to_kafka(self, df: DataFrame, topic: str):
        try:
            (df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
             .write
             .format("kafka")
             .option("kafka.bootstrap.servers", ",".join(self.kafka.servers))
             .option("kafka.security.protocol", self.kafka.protocol)
             .option("kafka.sasl.mechanism", self.kafka.mechanism)
             .option("kafka.sasl.jaas.config", self.kafka.jaas_config)
             .option("topic", topic)
             .save())
        except Exception as e:
            print(f"Error writing to Kafka topic {topic}: {e}")
            raise

    def process_stream(self):
        data = self.read_from_mysql("athlete_bio", "athlete_id")
        self.write_to_kafka(data, "kachurovskyi_athlete_event_results")
        self.write_to_mysql(data, "kachurovskyi_enriched_athlete_avg")

def main():
    kafka_settings = KafkaSettings()
    db_settings = DatabaseSettings()
    processor = SparkProcessor(kafka_settings, db_settings)
    processor.process_stream()

if __name__ == "__main__":
    main()