from __future__ import print_function

import logging

from polygonetl_airflow.build_patch_load_dag import build_patch_load_dag
from polygonetl_airflow.variables import read_load_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_patch_load_dag(
    dag_id='patch_ethereum_load_dag',
    chain='ethereum',
    blocks_dataset='bigquery-public-data.crypto_ethereum',
    **read_load_dag_vars(
        var_prefix='polygon_',
        load_schedule_interval='0 9 * * *'
    )
)
