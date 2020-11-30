import os
import pytest
from shutil import rmtree
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from pipelines.config import generate_paths, generate_schemas
from pipelines.operations import create_stream_writer, transform_raw, transform_bronze
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
def bronze_df(spark: SparkSession, env: str, bronze_schema: StructType) -> DataFrame:
    bronze_path = generate_paths(env, "test_bronze")
    yield read_stream_json(spark, bronze_path, bronze_schema)


@pytest.fixture(scope="module")
def bronze_checkpoint(env: str) -> StructType:
    path = generate_paths(env, "bronze_checkpoint")
    yield path
    rmtree(path)


@pytest.fixture(scope="module")
def bronze_path(env: str) -> StructType:
    path = generate_paths(env, "bronze")
    yield path
    rmtree(path)


@pytest.fixture(scope="module")
def bronze_schema() -> StructType:
    yield generate_schemas("bronze_schema")


@pytest.fixture(scope="module")
def raw_schema() -> StructType:
    yield generate_schemas("raw_schema")


@pytest.fixture()
def raw_df(spark: SparkSession, env: str, raw_schema: StructType) -> DataFrame:
    raw_path = generate_paths(env, "test_raw")
    yield read_stream_json(spark, raw_path, raw_schema)


@pytest.fixture(scope="module")
def silver_checkpoint(env: str) -> StructType:
    path = generate_paths(env, "silver_checkpoint")
    yield path
    rmtree(path)


@pytest.fixture(scope="module")
def silver_path(env: str) -> StructType:
    path = generate_paths(env, "silver")
    yield path
    rmtree(path)


@pytest.fixture(scope="module")
def silver_schema() -> StructType:
    yield generate_schemas("silver_schema")


class TestSparkIntegrations:
    def test_raw_to_bronze(
        self, env, spark, raw_df, bronze_checkpoint, bronze_path, bronze_schema
    ):
        stream_name = "write_raw_to_bronze"
        transformed_raw_df = transform_raw(spark, raw_df)
        raw_to_bronze_writer = create_stream_writer(
            dataframe=transformed_raw_df,
            path=bronze_path,
            checkpoint=bronze_checkpoint,
            name=stream_name,
            partition_column="p_ingestdate",
        )
        raw_to_bronze_writer.start()

        until_stream_is_ready(spark, stream_name)
        assert load_delta_table(spark, bronze_path).count() == 7320

    def test_bronze_to_silver(
        self, env, spark, bronze_df, silver_checkpoint, silver_path, silver_schema
    ):
        stream_name = "write_bronze_to_silver"
        transformed_bronze_df = transform_bronze(spark, bronze_df)
        bronze_to_silver_writer = create_stream_writer(
            dataframe=transformed_bronze_df,
            path=silver_path,
            checkpoint=silver_checkpoint,
            name=stream_name,
            partition_column="p_eventdate",
        )
        bronze_to_silver_writer.start()

        until_stream_is_ready(spark, stream_name)
        assert load_delta_table(spark, silver_path).count() == 7320
