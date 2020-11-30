import os
import pytest
from shutil import rmtree
from pyspark.sql import DataFrame, SparkSession

from pipelines.config import paths, schemas
from pipelines.operations import create_stream_writer, transform_bronze, transform_raw
from pipelines.utility import (
    generate_spark_session,
    load_delta_table,
    read_stream_json,
    until_stream_is_ready,
)


@pytest.fixture(scope="module")
def env() -> str:
    yield os.environ["STAGE"]


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    yield generate_spark_session()


@pytest.fixture()
def raw_df(spark: SparkSession, env: str) -> DataFrame:
    yield read_stream_json(spark, paths.test_raw, schemas.raw)


@pytest.fixture()
def bronze_df(spark: SparkSession, env: str) -> DataFrame:
    yield read_stream_json(spark, paths.test_bronze, schemas.bronze)


@pytest.fixture()
def silver_df(spark: SparkSession, env: str) -> DataFrame:
    stream_name = "create_silver"
    silver_json_df = read_stream_json(spark, paths.test_silver, schemas.silver)
    (
        silver_json_df.writeStream.format("delta")
        .partitionBy("p_eventdate")
        .outputMode("append")
        .option("checkpointLocation", paths.silver_checkpoint)
        .option("path", paths.silver)
        .queryName(stream_name)
        .start()
    )
    until_stream_is_ready(spark, stream_name)
    yield load_delta_table(spark, paths.silver)
    rmtree(paths.silver)
    rmtree(paths.silver_checkpoint)


class TestSparkDataframeOperations:
    def test_create_stream_write(self, env, spark, raw_df):
        transformed_raw_df = transform_raw(spark, raw_df)
        raw_to_bronze_writer = create_stream_writer(
            dataframe=transformed_raw_df,
            path=paths.bronze,
            checkpoint=paths.bronze_checkpoint,
            name="write_raw_to_bronze",
            partition_column="p_ingestdate",
        )
        assert raw_to_bronze_writer._df.schema == schemas.bronze

    def test_transform_raw(self, spark, raw_df):
        transformed_raw_df = transform_raw(spark, raw_df)
        assert transformed_raw_df.schema == schemas.bronze

    def test_transform_bronze(self, spark, bronze_df):
        transformed_bronze_df = transform_bronze(spark, bronze_df)
        assert transformed_bronze_df.schema == schemas.silver

    def test_prepare_interpolation_dataframe(self, spark, silver_df):
        # TODO: write tests
        assert False
