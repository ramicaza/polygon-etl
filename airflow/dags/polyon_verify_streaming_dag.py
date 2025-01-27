from __future__ import print_function

import logging

from polygonetl_airflow.build_verify_streaming_dag import build_verify_streaming_dag
from polygonetl_airflow.variables import read_verify_streaming_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# When searching for DAGs, Airflow will only consider files where the string "airflow" and "DAG" both appear in the
# contents of the .py file.
"""
DAG = build_verify_streaming_dag(
    dag_id='polygon_verify_streaming_dag',
    **read_verify_streaming_dag_vars(
        var_prefix='polygon_',
        max_lag_in_minutes=15,
    )
)
"""