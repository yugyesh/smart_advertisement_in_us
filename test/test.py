import os
import csv
import json

ROOT = "./lookup/"

for root, _, files in sorted(os.walk(ROOT)):
    for file in files:
        filename = file.split(".")[0]
        file_path = os.path.join(root, file)

        with open(file_path, "r") as csv_file:
            key_values = list(csv.reader(csv_file))
            key_values = key_values[1:]

        out_json_path = os.path.join(ROOT, f"{filename}.json")

        json_lookup = {}
        for key_value in key_values:
            json_lookup[key_value[0]] = key_value[1]

        with open(out_json_path, "w") as json_file:
            json.dump(json_lookup, json_file)
