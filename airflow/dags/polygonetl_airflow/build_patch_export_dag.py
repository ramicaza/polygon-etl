from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
from airflow.decorators import dag, task
from utils.error_handling import handle_dag_failure
import pandas_gbq # todo update env to have this package
import os
from polygonetl.cli import export_contracts, extract_tokens
from pathlib import Path
import logging
from polygonetl_airflow.gcs_utils import upload_to_gcs

COMPOSER_DATA_FOLDER = Path("/home/airflow/gcs/data/")
TEMP_DIR = COMPOSER_DATA_FOLDER if COMPOSER_DATA_FOLDER.exists() else None

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

def build_patch_export_dag(
    dag_id,
    provider_uri,
    original_dataset,
    chain,
    output_bucket,
    export_start_date,
    notification_emails=None,
    export_schedule_interval='0 0 * * *',
    export_max_workers=10,
    export_batch_size=200,
    export_max_active_runs=None,
    export_retries=5,
    **kwargs
):

    default_dag_args = {
        "depends_on_past": False,
        # "start_date": '2023-01-13' if chain == 'ethereum' else '2023-01-03', # export_start_date,
        "start_date": export_start_date,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": export_retries,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": handle_dag_failure,
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    cloud_storage_hook = GCSHook(gcp_conn_id="google_cloud_default")

    def copy_to_export_path(file_path, export_path):
        logging.info('Calling copy_to_export_path({}, {})'.format(file_path, export_path))
        filename = os.path.basename(file_path)

        upload_to_gcs(
            gcs_hook=cloud_storage_hook,
            bucket=output_bucket,
            object=export_path + filename,
            filename=file_path)

    # NOTE: this was adapted from contracts_patch.py in the agave fork of polygonetl
    @task(task_id='export_contracts_and_tokens')
    def process_date(**kwargs):
        date = kwargs['logical_date'].date()

        sql = f"""
    SELECT address as contract_address, block_number
    FROM `{original_dataset}.contracts`
    WHERE DATE(block_timestamp)='{date.strftime("%Y-%m-%d")}';
        """
        df = pandas_gbq.read_gbq(sql)

        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            tmp_contracts_output_path = os.path.join(tempdir, f"{date}.json")
            tmp_tokens_output_path = os.path.join(tempdir, f"{date}.csv")
            addresses_path = os.path.join(tempdir, 'contract_addresses.csv')
            df.to_csv(addresses_path)
            
            print(f"Exporting contracts to {tmp_contracts_output_path}")
            export_contracts.callback(
                batch_size=export_batch_size,
                receipts=addresses_path,
                output=tmp_contracts_output_path,
                max_workers=export_max_workers,
                provider_uri=provider_uri,
            )
            # TODO: split these up into 2 tasks. Haven't done this since this was just copied from contracts_patch.py
            
            print(f"Extracting tokens to {tmp_tokens_output_path}")
            extract_tokens.callback(
                contracts=tmp_contracts_output_path,
                output=tmp_tokens_output_path,
                max_workers=export_max_workers,
                provider_uri=provider_uri,
            )

            export_location_contracts = f'export/patch_{chain}/contracts/'
            export_location_tokens = f'export/patch_{chain}/tokens/'

            print(f"Copying contracts to export path {export_location_contracts}")
            print(f"Copying tokens to export path {export_location_tokens}")

            copy_to_export_path(tmp_contracts_output_path, export_location_contracts)
            copy_to_export_path(tmp_tokens_output_path, export_location_tokens)

    @dag(
        dag_id,
        # catchup=False,
        schedule_interval=export_schedule_interval,
        default_args=default_dag_args,
        max_active_runs=export_max_active_runs
    )
    def build_dag():
        process_date()

    return build_dag()
