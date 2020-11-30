from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    max,
    mean,
    stddev,
)
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.window import Window

from pipelines.utility import load_table


def create_batch_writer(
    dataframe: DataFrame,
    path: str,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "append",
    format: str = "delta",
) -> DataFrame:
    return (
        dataframe.drop(*exclude_columns)
        .write.format(format)
        .mode(mode)
        .option("path", path)
        .partitionBy(partition_column)
    )


def create_stream_writer(
    dataframe: DataFrame,
    path: str,
    checkpoint: str,
    name: str,
    partition_column: str,
    mode: str = "append",
    format: str = "delta",
    mergeSchema: bool = False,
) -> DataStreamWriter:

    stream_writer = (
        dataframe.writeStream.format(format)
        .outputMode(mode)
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .partitionBy(partition_column)
        .queryName(name)
    )

    if mergeSchema:
        stream_writer = stream_writer.option("mergeSchema", True)
    if partition_column is not None:
        stream_writer = stream_writer.partitionBy(partition_column)
    return stream_writer


def prepare_interpolation_dataframe(
    spark: SparkSession, silverDF: DataFrame
) -> DataFrame:
    dateWindow = Window.orderBy("p_eventdate")

    return silverDF.select(
        "*",
        lag(col("heartrate")).over(dateWindow).alias("prev_amt"),
        lead(col("heartrate")).over(dateWindow).alias("next_amt"),
    )


def update_silver_table(spark: SparkSession, silverPath: str) -> bool:
    from delta.tables import DeltaTable

    silverDF = load_table(spark, format="delta", path=silverPath)
    silverTable = DeltaTable.forPath(spark, silverPath)

    update_match = """
    health_tracker.eventtime = updates.eventtime
    AND
    health_tracker.device_id = updates.device_id
    """

    update = {"heartrate": "updates.heartrate"}

    interpolatedDF = prepare_interpolation_dataframe(spark, silverDF)

    updatesDF = interpolatedDF.where(col("heartrate") < 0).select(
        "device_id",
        ((col("prev_amt") + col("next_amt")) / 2).alias("heartrate"),
        "eventtime",
        "name",
        "p_eventdate",
    )

    (
        silverTable.alias("health_tracker")
        .merge(updatesDF.alias("updates"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True


def transform_bronze(spark: SparkSession, bronze: DataFrame) -> DataFrame:

    json_schema = """
        device_id INTEGER,
        heartrate DOUBLE,
        device_type STRING,
        name STRING,
        time FLOAT
    """

    return (
        bronze.select(from_json(col("value"), json_schema).alias("nested_json"))
        .select("nested_json.*")
        .select(
            "device_id",
            "device_type",
            "heartrate",
            from_unixtime("time").cast("timestamp").alias("eventtime"),
            "name",
            from_unixtime("time").cast("date").alias("p_eventdate"),
        )
    )


def transform_raw(spark: SparkSession, raw: DataFrame) -> DataFrame:
    return raw.select(
        lit("files.training.databricks.com").alias("datasource"),
        current_timestamp().alias("ingesttime"),
        "value",
        current_timestamp().cast("date").alias("p_ingestdate"),
    )


def transform_silver_mean_agg(silver: DataFrame) -> DataFrame:
    return silver.groupBy("device_id").agg(
        mean(col("heartrate")).alias("mean_heartrate"),
        stddev(col("heartrate")).alias("std_heartrate"),
        max(col("heartrate")).alias("max_heartrate"),
    )
