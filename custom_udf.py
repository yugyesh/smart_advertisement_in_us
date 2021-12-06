import os
import json
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import udf


def assign_value(col_name, key):
    ROOT = "./lookup/"
    key = str(key)
    col_name = col_name.lower()

    filepath = os.path.join(ROOT, f"{col_name}.json")
    with open(filepath, "r") as f:
        data = json.load(f)

    if col_name == "i94port":
        return data[key].split(",")[0] if key in data else None

    return data[key] if key in data else None


get_mode_udf = udf(lambda x: assign_value(key=x, col_name="i94mode"), StringType())

get_city_udf = udf(lambda x: assign_value(key=x, col_name="i94port"), StringType())

get_state_udf = udf(lambda x: assign_value(key=x, col_name="i94addr"), StringType())

get_origin_udf = udf(lambda x: assign_value(key=x, col_name="i94cit"), StringType())

get_visa_udf = udf(lambda x: assign_value(key=x, col_name="i94visa"), StringType())
