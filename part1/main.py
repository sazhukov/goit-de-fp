import findspark

findspark.init()
import json
import logging
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
'''
from pyspark.sql.functions import (
    col, avg, count, when, isnan, isnull,
    from_json, to_json, struct, lit, current_timestamp,
    to_date, try_cast
)
'''
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, DateType, \
    LongType
from pyspark.sql.streaming import StreamingQuery
import os
import subprocess
from config import MYSQL_CONFIG, KAFKA_CONFIG
from confluent_kafka.admin import AdminClient, NewTopic

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 pyspark-shell"
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Main execution starts here
try:
    logger.info("Starting Olympic Athlete Streaming Processor...")

    # Create Spark session
    logger.info("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("OlympicAthleteStreaming") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark session created successfully")

    # Create Kafka topics if they don't exist
    logger.info("Creating Kafka topics if they don't exist...")


    # [Функції create_kafka_topic та delete_kafka_topic залишаються без змін]
    def create_kafka_topic(topic_name):
        """Create Kafka topic if it doesn't exist"""
        try:
            # Create AdminClient
            admin_client = AdminClient(
                {
                    "bootstrap.servers": KAFKA_CONFIG["bootstrap_servers"],
                    "security.protocol": "SASL_PLAINTEXT",
                    "sasl.mechanisms": "PLAIN",
                    "sasl.username": KAFKA_CONFIG["username"],
                    "sasl.password": KAFKA_CONFIG["password"],
                }
            )

            # Check if topic exists
            metadata = admin_client.list_topics(timeout=10)
            if topic_name in metadata.topics:
                logger.info(f"Topic '{topic_name}' already exists")
                return True

            # Create topic if it doesn't exist
            new_topic = NewTopic(topic_name, num_partitions=2, replication_factor=1)
            fs = admin_client.create_topics([new_topic])

            # Wait for topic creation to complete
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logger.info(f"Successfully created topic '{topic_name}'")
                    return True
                except Exception as e:
                    logger.warning(f"Failed to create topic '{topic_name}': {str(e)}")
                    return False

        except Exception as e:
            logger.warning(f"Error creating topic '{topic_name}': {str(e)}")
            return False


    def delete_kafka_topic(topic_name):
        """Delete Kafka topic if it exists"""
        try:
            # Create AdminClient
            admin_client = AdminClient(
                {
                    "bootstrap.servers": KAFKA_CONFIG["bootstrap_servers"],
                    "security.protocol": "SASL_PLAINTEXT",
                    "sasl.mechanisms": "PLAIN",
                    "sasl.username": KAFKA_CONFIG["username"],
                    "sasl.password": KAFKA_CONFIG["password"],
                }
            )

            # Check if topic exists
            metadata = admin_client.list_topics(timeout=10)
            if topic_name not in metadata.topics:
                logger.info(f"Topic '{topic_name}' does not exist, skipping deletion")
                return True

            # Delete topic if it exists
            fs = admin_client.delete_topics([topic_name])

            # Wait for topic deletion to complete
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logger.info(f"Successfully deleted topic '{topic_name}'")
                    return True
                except Exception as e:
                    logger.warning(f"Failed to delete topic '{topic_name}': {str(e)}")
                    return False

        except Exception as e:
            logger.warning(f"Error deleting topic '{topic_name}': {str(e)}")
            return False


    # Create input and output topics
    input_topic_created = create_kafka_topic(KAFKA_CONFIG['input_topic'])
    output_topic_created = create_kafka_topic(KAFKA_CONFIG['output_topic'])

    if not input_topic_created or not output_topic_created:
        logger.warning("Some topics could not be created. Continuing anyway...")

    # Етап 1: Зчитати дані фізичних показників атлетів з MySQL
    logger.info("Reading athlete bio data from MySQL...")

    # Define the schema for athlete bio data
    athlete_bio_schema = StructType([
        StructField("athlete_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("born", StringType(), True),
        StructField("height", StringType(), True),  # Читаємо як String
        StructField("weight", StringType(), True),  # Читаємо як String
        StructField("country", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("description", StringType(), True),
        StructField("special_notes", StringType(), True),
    ])

    # Read from MySQL
    athlete_bio_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}") \
        .option("dbtable", "olympic_dataset.athlete_bio") \
        .option("user", MYSQL_CONFIG['user']) \
        .option("password", MYSQL_CONFIG['password']) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .schema(athlete_bio_schema) \
        .load()

    # Етап 2: Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами.

    NUMERIC_PATTERN = r'^-?(\d+(\.\d*)?|\.\d+)$'
    # 1. Спроба безпечного перетворення string у Double
    athlete_bio_df = athlete_bio_df.withColumn(
        "height_clean",
        # Якщо рядок відповідає числовому шаблону (rlike), перетворити його на Double
        F.when(
            F.col("height").rlike(NUMERIC_PATTERN),
            F.col("height").cast(DoubleType())
        )
        # Інакше (якщо це "N/A" або будь-який нечисловий рядок), встановити NULL
        .otherwise(F.lit(None).cast(DoubleType()))
    ).withColumn(
        "weight_clean",
        # Повторюємо те саме для ваги
        F.when(
            F.col("weight").rlike(NUMERIC_PATTERN),
            F.col("weight").cast(DoubleType())
        )
        .otherwise(F.lit(None).cast(DoubleType()))
    )

    # 2. Фільтрація: NULLs (нечислові або початкові null) та значення <= 0
    filtered_df = athlete_bio_df.filter(
        F.col("height_clean").isNotNull() & (F.col("height_clean") > 0) &
        F.col("weight_clean").isNotNull() & (F.col("weight_clean") > 0)
    )

    # 3. Фіналізація: формат дати та заміна старих колонок на чисті числові значення
    athlete_bio_df = filtered_df.withColumn("born", F.to_date(F.col("born"), "ddMMMyyyy")) \
        .withColumn("height", F.col("height_clean")) \
        .withColumn("weight", F.col("weight_clean")) \
        .drop("height_clean", "weight_clean")  # Видаляємо тимчасові колонки

    logger.info(f"Loaded {athlete_bio_df.count()} athlete records after filtering")

    # Етап 3: Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік

    logger.info("Reading athlete event results from MySQL and writing to Kafka...")

    # Define schema for event results (matching actual MySQL schema)
    event_results_schema = StructType([
        StructField("edition", StringType(), True),
        StructField("edition_id", IntegerType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", LongType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", IntegerType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", StringType(), True),
    ])

    # Read from MySQL
    event_results_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}") \
        .option("dbtable", "olympic_dataset.athlete_event_results") \
        .option("user", MYSQL_CONFIG['user']) \
        .option("password", MYSQL_CONFIG['password']) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .schema(event_results_schema) \
        .load()

    # Convert to JSON and write to Kafka
    event_results_json = event_results_df.select(
        F.to_json(F.struct("*")).alias("value")
    )

    event_results_json.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("kafka.security.protocol", KAFKA_CONFIG["security_protocol"]) \
        .option("kafka.sasl.mechanism", KAFKA_CONFIG["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config", KAFKA_CONFIG["sasl_jaas_config"]) \
        .option("topic", KAFKA_CONFIG['input_topic']) \
        .mode("append") \
        .save()

    logger.info("Successfully wrote athlete event results to Kafka topic")

    # Етап 4: Зчитати дані з результатами змагань з Kafka-топіку
    logger.info("Starting streaming data processing...")

    # Define schema for JSON data from Kafka (matching actual MySQL schema)
    json_schema = event_results_schema  # Використовуємо ту саму схему

    # Read streaming data from Kafka
    streaming_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("kafka.security.protocol", KAFKA_CONFIG["security_protocol"]) \
        .option("kafka.sasl.mechanism", KAFKA_CONFIG["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config", KAFKA_CONFIG["sasl_jaas_config"]) \
        .option("subscribe", KAFKA_CONFIG['input_topic']) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "10000") \
        .option("failOnDataLoss", "false") \
        .load()

    # Етап 5: Дані з json-формату необхідно перевести в dataframe-формат
    parsed_df = streaming_df.select(
        F.from_json(F.col("value").cast("string"), json_schema).alias("data")
    ).select("data.*")

    # Етап 6: Об’єднати дані з результатами змагань з біологічними даними
    logger.info("Joining streaming data with athlete bio data...")
    joined_df = parsed_df.join(
        athlete_bio_df.select("athlete_id", "name", "sex", "born", "height", "weight", "country",
                              F.col("country_noc").alias("athlete_country_noc")),
        on="athlete_id",
        how="inner"
    )

    # Етап 7: Знайти середній зріст і вагу атлетів
    logger.info("Calculating average height and weight statistics...")
    aggregated_df = joined_df.groupBy(
        "sport", "medal", "sex", "athlete_country_noc"
    ).agg(
        F.avg("height").alias("avg_height"),
        F.avg("weight").alias("avg_weight"),
        F.count("*").alias("athlete_count"),
        F.current_timestamp().alias("calculation_timestamp")  # Додано timestamp
    )


    # Define batch processing function inline
    def process_batch(df, epoch_id):
        if df.count() > 0:
            logger.info(f"Processing batch {epoch_id} with {df.count()} records")

            # Етап 6.а): Запис у вихідний Kafka-топік
            logger.info(f"Writing batch {epoch_id} to Kafka output topic...")
            kafka_df = df.select(
                F.to_json(F.struct("*")).alias("value")
            )
            kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
                .option("kafka.security.protocol", KAFKA_CONFIG["security_protocol"]) \
                .option("kafka.sasl.mechanism", KAFKA_CONFIG["sasl_mechanism"]) \
                .option("kafka.sasl.jaas.config", KAFKA_CONFIG["sasl_jaas_config"]) \
                .option("topic", KAFKA_CONFIG['output_topic']) \
                .save()

            # Етап 6.b): Запис у базу даних
            logger.info(f"Writing batch {epoch_id} to MySQL database...")
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}") \
                .option("dbtable", "sazhukov_athlete_stats") \
                .option("user", MYSQL_CONFIG['user']) \
                .option("password", MYSQL_CONFIG['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("append") \
                .save()
        else:
            logger.info(f"Batch {epoch_id} is empty, skipping...")


    # Process streaming data with forEachBatch
    query = aggregated_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()

    logger.info("Streaming query started. Waiting for termination...")

    # Створення другого запиту для виведення в консоль (для перевірки)
    console_query = aggregated_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()
    console_query.awaitTermination()

except Exception as e:
    logger.error(f"Error in processing: {str(e)}")
    # Якщо виникає помилка при запуску, обов'язково видаляємо топіки (але тільки після успішного запуску)
    # Зважаючи на те, що помилка виникла на етапі ініціалізації, цей блок можна залишити без змін.
    raise
finally:
    # Clean up Kafka topics
    logger.info("Cleaning up Kafka topics...")
    try:
        delete_kafka_topic(KAFKA_CONFIG['input_topic'])
        delete_kafka_topic(KAFKA_CONFIG['output_topic'])
    except Exception as e:
        logger.warning(f"Error during topic cleanup: {str(e)}")

    if 'spark' in locals():
        spark.stop()
        logger.info("Spark session stopped")