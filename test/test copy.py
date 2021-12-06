import os
import csv
import json

ROOT = "./lookup/"


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


assign_value(key="FRB", col_name="i94port")
