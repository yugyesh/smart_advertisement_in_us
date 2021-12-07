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
from custom_udf import *

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


def process_immigration_data(spark, input_data, output_data):
    """This method transfors the immigration data using following steps:
        - Convert decimal columns to integer
        - Drop duplicates excluding cicid
        - Assign 0 to null values for integer
        - Assign real values
        - Rename columns
        - Order the columns in proper sequence
        - Convert arrival_date and departure_date in "YYYY-MM-DD" format

    Args:
        [Object]: pyspark.sql.session.SparkSession object
        input_data (string): path of s3 for input json file
        output_data (string): s3 path to write parquet tables

    Returns:
        [type]: [description]
    """

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

    #  Drop duplicate by excluding cicid
    immigration_df = immigration_df.drop("cicid")
    immigration_df = immigration_df.dropDuplicates()
    immigration_df = immigration_df.withColumn("cicid", monotonically_increasing_id())

    # Assign 0 to null values for integer
    immigration_df = immigration_df.fillna(0, int_cols)

    # Assign real values
    # Retrieve transporation mode using i94mode
    immigration_df = immigration_df.withColumn(
        "transportation_mode", get_mode_udf(immigration_df.i94mode)
    )

    # Retrieve arrived city
    immigration_df = immigration_df.withColumn(
        "arrived_city", get_city_udf(immigration_df.i94port)
    )

    # Retrieve us_address
    immigration_df = immigration_df.withColumn(
        "us_address", get_state_udf(immigration_df.i94addr)
    )

    # Retrieve origin city and travelled from using i94CIT and i94res
    immigration_df = immigration_df.withColumn(
        "origin_city", get_origin_udf(immigration_df.i94cit)
    ).withColumn("traveled_from", get_origin_udf(immigration_df.i94res))

    # Retrive i94visa with value
    immigration_df = immigration_df.withColumn(
        "visa_status", get_visa_udf(immigration_df.i94visa)
    )

    # Exclude unused columns
    unused_cols = [
        "i94yr",
        "i94mon",
        "count",
        "fltno",
        "insnum",
        "entdepd",
        "biryear",
        "dtadfile",
        "biryear",
        "visapost",
        "entdepu",
        "admnum",
        "i94cit",
        "i94res",
        "i94port",
        "i94addr",
        "i94mode",
        "i94visa",
        "entdepa",
        "dtaddto",
    ]
    immigration_df = immigration_df.drop(*unused_cols)

    # Rename columns
    immigration_df = (
        immigration_df.withColumnRenamed("arrdate", "arrival_date")
        .withColumnRenamed("depdate", "departure_date")
        .withColumnRenamed("i94bir", "age")
        .withColumnRenamed("occup", "occupation")
        .withColumnRenamed("matflag", "matched_flag")
    )

    # Order the columns in proper sequence
    immigration_df = immigration_df.select(
        [
            "cicid",
            "origin_city",
            "traveled_from",
            "arrived_city",
            "us_address",
            "arrival_date",
            "departure_date",
            "transportation_mode",
            "age",
            "gender",
            "visa_status",
            "occupation",
            "airline",
        ]
    )

    # Convert arrival_date and departure_date in proper format
    immigration_df = immigration_df.withColumn(
        "arrival_date", udf_datetime_from_sas(immigration_df.arrival_date)
    ).withColumn("departure_date", udf_datetime_from_sas(immigration_df.departure_date))

    # Write parquet data to parking
    immigration_df.write.mode("overwrite").parquet(
        os.path.join(output_data, "immigration")
    )
    return immigration_df


def process_cities_demographics(spark, input_data, output_data):
    """This method transforms the demographics data using following steps:
        - Remove duplicate using pivoting column
        - Remove unwanted columns from schema
        - Dropping duplicate
        - Convert column names
        - Fill null with 0
        - Append auto increment id column
        - Reorder the schema

    Args:
        [Object]: pyspark.sql.session.SparkSession object
        input_data (string): path of s3 for input json file
        output_data (string): s3 path to write parquet tables
    """
    # Read data from the s3
    input_data = os.path.join(
        input_data,
        "us_cities_demographics.csv",
    )
    demographic_df = spark.read.csv(input_data, inferSchema=True, header=True, sep=";")

    # Remove duplicate using pivoting column
    # Pivot column Race to different columns
    pivot_cols = ["City", "State"]
    pivot_df = demographic_df.groupBy(pivot_cols).pivot("Race").sum("Count")

    # Joining the pivot
    demographic_df = demographic_df.join(other=pivot_df, on=pivot_cols)

    # Remove unwanted columns from schema
    del_cols = [
        "median age",
        "Number of Veterans",
        "foreign-born",
        "Average Household Size",
        "State Code",
        "race",
        "count",
    ]
    demographic_df = demographic_df.drop(*del_cols)

    # Droping duplicates
    demographic_df = demographic_df.dropDuplicates()

    # Convert columns name
    demographic_df = demographic_df.toDF(
        "city",
        "state",
        "male_population",
        "female_population",
        "total_population",
        "american_indian_alaska_native",
        "asian",
        "black_african_american",
        "hispanic_latino",
        "white",
    )

    # Fill null with 0 for integer columns
    num_cols = [
        "male_population",
        "female_population",
        "total_population",
        "american_indian_alaska_native",
        "asian",
        "black_african_american",
        "hispanic_latino",
        "white",
    ]
    demographic_df = demographic_df.fillna(0, num_cols)

    demographic_df = demographic_df.withColumn(
        "demographic_id", monotonically_increasing_id()
    )

    # Reorder the schema
    demographic_df = demographic_df.select(
        [
            "demographic_id",
            "city",
            "state",
            "american_indian_alaska_native",
            "asian",
            "black_african_american",
            "hispanic_latino",
            "white",
            "male_population",
            "female_population",
            "total_population",
        ]
    )

    # TODO: Partation the data
    demographic_df.write.mode("overwrite").parquet(output_data)


def process_airports_data(spark, input_data, output_data):

    # Read data from the s3
    # TODO: Take filename from environment
    input_data = os.path.join(
        input_data,
        "airport_codes.csv",
    )
    airport_df = spark.read.csv(input_data, header=True, sep=",")

    # Remove extra whitespace in column
    airport_df = airport_df.select(
        [F.col(col).alias(col.replace(" ", "")) for col in airport_df.columns]
    )

    # Exclude airports outside of us
    airport_df = airport_df.filter(airport_df.iso_country.like("%US%"))

    # Create state column code using iso_region
    airport_df = airport_df.withColumn("state_code", get_state_code_udf("iso_region"))

    # Retrive state_name from state code
    airport_df = airport_df.withColumn("state", get_state_udf(airport_df.state_code))

    # Remove unwanted columns
    airport_delete_cols = [
        "elevation_ft",
        "continent",
        "iso_country",
        "iso_region",
        "gps_code",
        "iata_code",
        "local_code",
        "coordinates",
        "state_code",
    ]
    airport_df = airport_df.drop(*airport_delete_cols)

    # Dropping duplicate by excluding ident
    airport_df = airport_df.dropDuplicates()
    airport_df = airport_df.withColumn("airport_id", monotonically_increasing_id())

    # Rename munacipality to city
    airport_df = airport_df.withColumnRenamed("municipality", "city")


def main():
    spark = create_spark_session()
    input_data = "./data"
    process_immigration_data(
        spark=spark, input_data=input_data, output_data="/data/processed_data/"
    )

    process_cities_demographics(
        spark=spark, input_data=input_data, output_data="/data/processed_data/"
    )


if __name__ == "__main__":
    main()
