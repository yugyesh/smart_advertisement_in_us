import configparser

import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from datetime import datetime, timedelta
import json
from pyspark.sql.functions import (
    desc,
    monotonically_increasing_id,
    udf,
    to_date,
    from_unixtime,
    trim,
    col,
)


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """Returns spark session object
    Returns:
        [Object]: pyspark.sql.session.SparkSession object
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_immigration_data(spark, input_data):

    # Read data from the s3
    input_data = os.path.join(
        input_data,
        "sas_data/part-00000-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet",
    )
    immigration_df = spark.read.parquet(input_data)

    # Convert decimal columns to integer
    int_cols = [
        "cicid",
        "i94cit",
        "i94res",
        "arrdate",
        "i94mode",
        "depdate",
        "i94bir",
        "i94visa",
    ]
    for col_name in int_cols:
        immigration_df = immigration_df.withColumn(
            col_name, immigration_df[col_name].cast(IntegerType())
        )

def main():
    spark = create_spark_session()
    input_data = "./data"
    process_immigration_data(spark=spark, input_data=input_data)


if __name__ == "__main__":
    main()
