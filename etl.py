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


def main():
    spark = create_spark_session()
    input_data = "./data"


if __name__ == "__main__":
    main()
