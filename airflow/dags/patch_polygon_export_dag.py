from polygonetl_airflow.build_patch_export_dag import build_patch_export_dag
from polygonetl_airflow.variables import read_export_dag_vars

# airflow DAG
DAG = build_patch_export_dag(
    dag_id='patch_polygon_export_dag',
    provider_uri='https://nd-741-989-856.p2pify.com/a110bcd64460ebcbdddbb2739d347cf0',
    original_dataset='public-data-finance.crypto_polygon',
    chain='polygon',
    **read_export_dag_vars(
        var_prefix='polygon_',
        export_schedule_interval='0 8 * * *',
        # export_start_date='2023-01-13',
        export_max_active_runs=3,
        export_max_workers=5,
        export_traces_max_workers=10,
    )
)
