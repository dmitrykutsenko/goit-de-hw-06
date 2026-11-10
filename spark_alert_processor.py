from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    avg,
    struct,
    current_timestamp,
    to_json
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


KAFKA_BROKER = "localhost:9092"
SENSOR_TOPIC = "building_sensors_dmytro"
ALERT_TOPIC = "alerts_dmytro"

spark = (
    SparkSession.builder
    .appName("SensorAlerts")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------
# Schema of sensor data
# -------------------

sensor_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

# -------------------
# Read sensor stream
# -------------------

raw_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BROKER
    )
    .option(
        "subscribe",
        SENSOR_TOPIC
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING)")
    .select(
        from_json(
            col("value"),
            sensor_schema
        ).alias("data")
    )
    .select("data.*")
    .withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )
)

# -------------------
# Sliding Window
# -------------------

avg_df = (
    parsed_df
    .withWatermark(
        "timestamp",
        "10 seconds"
    )
    .groupBy(
        window(
            col("timestamp"),
            "1 minute",
            "30 seconds"
        )
    )
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    )
)

# -------------------
# Read alerts config
# -------------------

alerts_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("alerts_conditions.csv")
)

# -------------------
# Cross Join
# -------------------

joined_df = avg_df.crossJoin(alerts_df)

# -------------------
# Alert filtering
# -------------------

alerts_result = joined_df.filter(

    (
        (col("humidity_min") == -999)
        |
        (
            (col("h_avg") >= col("humidity_min"))
            &
            (col("h_avg") <= col("humidity_max"))
        )
    )

    &

    (
        (col("temperature_min") == -999)
        |
        (
            (col("t_avg") >= col("temperature_min"))
            &
            (col("t_avg") <= col("temperature_max"))
        )
    )
)

# -------------------
# Alert message
# -------------------

result_df = alerts_result.select(
    struct(
        col("window.start").alias("start"),
        col("window.end").alias("end")
    ).alias("window"),

    col("t_avg"),
    col("h_avg"),

    col("code"),
    col("message"),

    current_timestamp().alias("timestamp")
)

# -------------------
# Convert to JSON
# -------------------

kafka_output = result_df.select(
    to_json(
        struct("*")
    ).alias("value")
)

# -------------------
# Write to Kafka
# -------------------

query = (
    kafka_output.writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BROKER
    )
    .option(
        "topic",
        ALERT_TOPIC
    )
    .option(
        "checkpointLocation",
        "checkpoints/alerts"
    )
    .outputMode("append")
    .start()
)

query.awaitTermination()
