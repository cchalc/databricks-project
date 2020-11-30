import os
import pytest
from shutil import rmtree
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from pipelines.config import generate_paths, generate_schemas
from pipelines.operations import create_stream_writer, transform_bronze, transform_raw
from pipelines.utility import generate_spark_session, load_delta_table, read_stream_json


@pytest.fixture(scope="module")
def env() -> str:
    yield os.environ["STAGE"]


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    yield generate_spark_session()


@pytest.fixture(scope="module")
def raw_schema() -> StructType:
    yield generate_schemas("raw_schema")


@pytest.fixture(scope="module")
def bronze_path(env: str) -> StructType:
    path = generate_paths(env, "bronze")
    yield path


@pytest.fixture(scope="module")
def bronze_schema() -> StructType:
    yield generate_schemas("bronze_schema")


@pytest.fixture(scope="module")
def silver_schema() -> StructType:
    yield generate_schemas("silver_schema")


@pytest.fixture()
def raw_df(spark: SparkSession, env: str, raw_schema: StructType) -> DataFrame:
    raw_path = generate_paths(env, "test_raw")
    yield read_stream_json(spark, raw_path, raw_schema)


@pytest.fixture()
def bronze_df(spark: SparkSession, env: str, bronze_schema: StructType) -> DataFrame:
    bronze_path = generate_paths(env, "test_bronze")
    yield read_stream_json(spark, bronze_path, bronze_schema)


@pytest.fixture()
def silver_df(spark: SparkSession, env: str, silver_schema: StructType) -> DataFrame:
    test_silver_path = generate_paths(env, "test_silver")
    silver_path = generate_paths(env, "silver")
    silver_json_df = read_stream_json(spark, test_silver_path, silver_schema)
    (
        silver_json_df.writeStream.format("delta")
        .partitionBy("p_eventdate")
        .option("path", silver_path)
        .save()
    )
    yield load_delta_table(spark, silver_path)
    rmtree(silver_path)


class TestSparkDataframeOperations:
    def test_create_stream_write(self, env, spark, raw_df, bronze_path, bronze_schema):
        transformed_raw_df = transform_raw(spark, raw_df)
        bronze_checkpoint = generate_paths(env, "bronze_checkpoint")
        raw_to_bronze_writer = create_stream_writer(
            dataframe=transformed_raw_df,
            path=bronze_path,
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

    def test_prepare_interpolation_dataframe(self, spark, silver_df):
        pass
