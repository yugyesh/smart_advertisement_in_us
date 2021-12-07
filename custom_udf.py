import os
import json
from pyspark.sql.types import IntegerType, StringType, DateType
from pyspark.sql.functions import udf
from datetime import datetime, timedelta


def assign_value(col_name, key):
    # TODO read root from cfg file
    ROOT = "./data/lookup/"
    key = str(key).strip()
    col_name = col_name.lower()

    filepath = os.path.join(ROOT, f"{col_name}.json")
    with open(filepath, "r") as f:
        data = json.load(f)

    if col_name == "i94port":
        return data[key].split(",")[0] if key in data else None

    return data[key] if key in data else None


def convert_datetime(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None


get_mode_udf = udf(lambda x: assign_value(key=x, col_name="i94mode"), StringType())

get_city_udf = udf(lambda x: assign_value(key=x, col_name="i94port"), StringType())

get_state_udf = udf(lambda x: assign_value(key=x, col_name="i94addr"), StringType())

get_origin_udf = udf(lambda x: assign_value(key=x, col_name="i94cit"), StringType())

get_visa_udf = udf(lambda x: assign_value(key=x, col_name="i94visa"), StringType())

udf_datetime_from_sas = udf(lambda x: convert_datetime(x), DateType())

get_state_code_udf = udf(lambda x: x if x is None else x.split("-")[1])

remove_whitespace_udf = udf(lambda x: str(x).strip())
