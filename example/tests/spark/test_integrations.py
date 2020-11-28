import os
import pytest
from shutil import rmtree
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.config import generate_paths, generate_schemas
from src.operations import create_stream_writer, transform_raw
from src.utility import (
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
    yield generate_schemas("BRONZE_SCHEMA")


@pytest.fixture()
def raw_df(spark: SparkSession, env: str) -> DataFrame:
    raw_path = generate_paths(env, "test_raw")
    yield read_stream_json(spark, raw_path)


class TestSparkIntegrations:
    def test_raw_to_bronze(
        self, env, spark, raw_df, bronze_checkpoint, bronze_path, bronze_schema
    ):
        stream_name = "write_raw_to_bronze"
        transformed_raw_df = transform_raw(spark, raw_df)
        raw_to_bronze_writer = create_stream_writer(
            dataframe=transformed_raw_df,
            checkpoint=bronze_checkpoint,
            name=stream_name,
            partition_column="p_ingestdate",
        )
        raw_to_bronze_writer.start(bronze_path)

        until_stream_is_ready(spark, stream_name)
        assert load_delta_table(spark, bronze_path).count() == 7320
