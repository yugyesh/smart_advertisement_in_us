from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class SmartAdd(AirflowPlugin):
    name = "smart_add"
    operators = [
        operators.DataQualityOperator,
        operators.LoadToRedshiftOperator,
    ]
