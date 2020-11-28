# Databricks notebook source
from src.utility import generate_spark_session
from src.operations import read_stream_raw, transform_raw, create_stream_writer
from src.config import paths

spark = generate_spark_session()
raw_path = paths["unit-testing"]["raw"]
rawDF = read_stream_raw(spark, raw_path)
