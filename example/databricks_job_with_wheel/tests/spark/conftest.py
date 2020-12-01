import pytest

from pyspark.sql import DataFrame, SparkSession
from pipelines.config import paths, schemas
from pipelines.utility import generate_spark_session, load_dataframe


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    yield generate_spark_session()


@pytest.fixture(scope="module")
def test_raw_df(spark: SparkSession) -> DataFrame:
    yield load_dataframe(
        spark, format="text", path=paths.test_raw, schema=schemas.raw, streaming=True
    )


@pytest.fixture(scope="module")
def test_bronze_df(spark: SparkSession) -> DataFrame:
    yield load_dataframe(
        spark,
        format="json",
        path=paths.test_bronze,
        schema=schemas.bronze,
        streaming=True,
    )
