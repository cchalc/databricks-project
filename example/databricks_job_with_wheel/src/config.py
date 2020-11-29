from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    TimestampType,
)


def generate_paths(env: str, path: str) -> str:
    bases = {
        "local": "/home/jovyan/tests/data/",
        "development": "/mnt/dbacademy/",
        "production": None,
    }
    base = bases[env]

    paths = {
        "test_raw": base + "test/raw/",
        "test_bronze": base + "test/bronze/",
        "test_silver": base + "test/silver/",
        "test_gold": base + "test/gold/",
        "bronze": base + "bronze/",
        "silver": base + "silver/",
        "gold": base + "gold/",
        "bronze_checkpoint": base + "checkpoints/bronze/",
        "silver_checkpoint": base + "checkpoints/silver/",
        "gold_checkpoint": base + "checkpoints/gold/",
    }
    return paths[path]


def generate_schemas(table):
    schemas = {
        "RAW_SCHEMA": StructType([StructField("value", StringType(), False)]),
        "BRONZE_SCHEMA": StructType(
            [
                StructField("datasource", StringType(), False),
                StructField("ingesttime", TimestampType(), False),
                StructField("value", StringType(), True),
                StructField("p_ingestdate", DateType(), False),
            ]
        ),
        "SILVER_SCHEMA": StructType(
            [
                StructField("device_id", IntegerType(), True),
                StructField("device_type", StringType(), True),
                StructField("heartrate", DoubleType(), True),
                StructField("eventtime", TimestampType(), True),
                StructField("name", StringType(), True),
                StructField("p_eventdate", DateType(), True),
            ]
        ),
        "DIM_COMPANY_SCHEMA": StructType(
            [
                StructField("search_industry", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("domain", StringType(), True),
                StructField("industries", ArrayType(StringType()), True),
                StructField("insert_time", TimestampType(), True),
            ]
        ),
        "GEO_LOOKUP_SCHEMA": StructType(
            [
                StructField("input_location", StringType(), True),
                StructField("standardized_location", StringType(), True),
                StructField("full_result", StringType(), True),
            ]
        ),
    }
    return schemas[table]
