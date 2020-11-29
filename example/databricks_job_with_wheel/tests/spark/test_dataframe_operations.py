import os
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.config import generate_paths, generate_schemas
from src.operations import create_stream_writer, transform_bronze, transform_raw
from src.utility import generate_spark_session, read_stream_json


@pytest.fixture(scope="module")
def env() -> str:
    yield os.environ["STAGE"]


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    yield generate_spark_session()


@pytest.fixture(scope="module")
def raw_schema() -> StructType:
    yield generate_schemas("RAW_SCHEMA")


@pytest.fixture(scope="module")
def bronze_schema() -> StructType:
    yield generate_schemas("BRONZE_SCHEMA")


@pytest.fixture(scope="module")
def silver_schema() -> StructType:
    yield generate_schemas("SILVER_SCHEMA")


@pytest.fixture()
def raw_df(spark: SparkSession, env: str, raw_schema: StructType) -> DataFrame:
    raw_path = generate_paths(env, "test_raw")
    yield read_stream_json(spark, raw_path, raw_schema)


@pytest.fixture()
def bronze_df(spark: SparkSession, env: str, bronze_schema: StructType) -> DataFrame:
    bronze_path = generate_paths(env, "test_bronze")
    yield read_stream_json(spark, bronze_path, bronze_schema)


class TestSparkDataframeOperations:
    def test_create_stream_write(self, env, spark, raw_df, bronze_schema):
        transformed_raw_df = transform_raw(spark, raw_df)
        bronze_checkpoint = generate_paths(env, "bronze_checkpoint")
        raw_to_bronze_writer = create_stream_writer(
            dataframe=transformed_raw_df,
            checkpoint=bronze_checkpoint,
            name="write_raw_to_bronze",
            partition_column="p_ingestdate",
        )
        assert raw_to_bronze_writer._df.schema == bronze_schema

    def test_transform_raw(self, spark, raw_df, bronze_schema):
        transformed_raw_df = transform_raw(spark, raw_df)
        assert transformed_raw_df.schema == bronze_schema

    def test_transform_bronze(self, spark, bronze_df, silver_schema):
        transformed_bronze_df = transform_bronze(spark, bronze_df)
        assert transformed_bronze_df.schema == silver_schema
