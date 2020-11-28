import os
import time
from shutil import rmtree
from pandas import DataFrame as pdDataFrame
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def clear_path(path: str) -> bool:
    try:
        rmtree(path)
        return True
    except FileNotFoundError:
        return False


def generate_spark_session() -> SparkSession:
    pyspark_submit_args = '--packages "io.delta:delta-core_2.12:0.7.0" '
    pyspark_submit_args += "pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
    return SparkSession.builder.master("local[8]").getOrCreate()


def initialize_delta_table(
    spark: SparkSession, path: str, schema: str, partitionBy: str = ""
):
    df = spark.createDataFrame([], schema)
    writer = df.write.format("delta")
    if partitionBy != "":
        writer = writer.partitionBy(partitionBy)
    writer.save(path)


def load_delta_table(spark: SparkSession, path: str, alias: str = None) -> DataFrame:
    if alias is not None:
        return spark.read.format("delta").load(path).alias(alias)
    return spark.read.format("delta").load(path)


def read_stream_delta(
    spark: SparkSession, deltaPath: str, alias: str = None
) -> DataFrame:
    return spark.readStream.format("delta").load(deltaPath).alias(alias)


def read_stream_json(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return spark.readStream.format("json").schema(schema).load(path)


def spark_display(df: DataFrame, n: int = 10) -> pdDataFrame:
    return df.limit(n).toPandas()


def until_stream_is_ready(
    spark: SparkSession, named_stream: str, progressions: int = 3
) -> bool:
    queries = [stream for stream in spark.streams.active if stream.name == named_stream]
    while len(queries) == 0 or len(queries[0].recentProgress) < progressions:
        time.sleep(5)
        queries = [
            stream for stream in spark.streams.active if stream.name == named_stream
        ]
    return True
