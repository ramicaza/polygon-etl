from polygonetl_airflow.build_patch_export_dag import build_patch_export_dag
from polygonetl_airflow.variables import read_export_dag_vars

# airflow DAG
DAG = build_patch_export_dag(
    dag_id='patch_ethereum_export_dag',
    provider_uri='http://35.164.63.43:8545',
    original_dataset='bigquery-public-data.crypto_ethereum',
    chain='ethereum',
    **read_export_dag_vars(
        var_prefix='polygon_',
        export_schedule_interval='0 8 * * *',
        # export_start_date='2023-01-13',
        export_max_active_runs=3,
        export_max_workers=5,
        export_traces_max_workers=10,
    )
)
