from src.operations import create_stream_writer, transform_bronze, transform_raw
from src.utility import (
    generate_paths,
    generate_schemas,
    generate_spark_session,
    load_delta_table,
    read_stream_json,
    until_stream_is_ready,
)

if __name__ == "__main__":
    spark = generate_spark_session()
    test_raw = generate_paths(env, "test_raw")
    test_bronze = generate_paths(env, "test_bronze")
    test_silver = generate_paths(env, "test_silver")

    raw_schema = generate_schema("raw_schema")

    bronze_path = generate_paths(env, "bronze")

    raw_df = read_stream_json(spark, test_raw, raw_schema)

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
