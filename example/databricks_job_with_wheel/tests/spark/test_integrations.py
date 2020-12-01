import pytest
from shutil import rmtree
from pyspark.sql import DataFrame, SparkSession

from pipelines.config import paths, schemas
from pipelines.operations import create_stream_writer, transform_raw, transform_bronze
from pipelines.utility import (
    initialize_delta_table,
    load_dataframe,
    until_stream_is_ready,
)


@pytest.fixture()
def bronze_df(spark: SparkSession) -> DataFrame:
    initialize_delta_table(
        spark, paths.bronze, schemas.bronze, partitionBy="p_ingestdate"
    )
    yield load_dataframe(spark, format="delta", path=paths.bronze)
    rmtree(paths.bronze)
    rmtree(paths.bronze_checkpoint)


@pytest.fixture()
def silver_df(spark: SparkSession) -> DataFrame:
    initialize_delta_table(
        spark, paths.silver, schemas.silver, partitionBy="p_eventdate"
    )
    yield load_dataframe(spark, format="delta", path=paths.silver)
    rmtree(paths.silver)
    rmtree(paths.silver_checkpoint)


class TestSparkIntegrations:
    def test_raw_to_bronze(self, spark, test_raw_df, bronze_df):
        stream_name = "write_raw_to_bronze"
        transformed_raw_df = transform_raw(spark, test_raw_df)
        raw_to_bronze_writer = create_stream_writer(
            dataframe=transformed_raw_df,
            path=paths.bronze,
            checkpoint=paths.bronze_checkpoint,
            name=stream_name,
            partition_column="p_ingestdate",
        )
        raw_to_bronze_writer.start()

        until_stream_is_ready(spark, stream_name)
        assert bronze_df.count() == 7320

    def test_bronze_to_silver(self, spark, test_bronze_df, silver_df):
        stream_name = "write_bronze_to_silver"
        transformed_bronze_df = transform_bronze(spark, test_bronze_df)
        bronze_to_silver_writer = create_stream_writer(
            dataframe=transformed_bronze_df,
            path=paths.silver,
            checkpoint=paths.silver_checkpoint,
            name=stream_name,
            partition_column="p_eventdate",
        )
        bronze_to_silver_writer.start()

        until_stream_is_ready(spark, stream_name)
        assert silver_df.count() == 7320
