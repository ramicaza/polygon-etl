from __future__ import print_function

import os
import logging
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow import DAG, configuration
from airflow.operators.python import PythonOperator

from polygonetl.cli import (
    get_block_range_for_date,
    extract_csv_column,
    export_blocks_and_transactions,
    export_receipts_and_logs,
    extract_contracts,
    export_contracts,
    extract_tokens,
    extract_token_transfers,
    export_geth_traces,
    extract_geth_traces
)
from polygonetl_airflow.gcs_utils import download_from_gcs, upload_to_gcs
from utils.error_handling import handle_dag_failure

# Use Composer's suggested Data folder for temp storage
# This is a folder in the Composer Bucket, mounted locally using gcsfuse
# Overcomes the 10GB ephemerol storage limit on workers (imposed by GKE Autopilot)
# https://cloud.google.com/composer/docs/composer-2/cloud-storage
COMPOSER_DATA_FOLDER = Path("/home/airflow/gcs/data/")
TEMP_DIR = COMPOSER_DATA_FOLDER if COMPOSER_DATA_FOLDER.exists() else None


def build_export_dag(
        dag_id,
        provider_uris,
        provider_uris_archival,
        output_bucket,
        export_start_date,
        notification_emails=None,
        export_schedule_interval='0 0 * * *',
        export_max_workers=10,
        export_traces_max_workers=10,
        export_batch_size=200,
        export_max_active_runs=None,
        export_retries=5,
        **kwargs
):
    default_dag_args = {
        "depends_on_past": False,
        "start_date": export_start_date,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": export_retries,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": handle_dag_failure,
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    # export_daofork_traces_option = False
    # export_genesis_traces_option = True
    export_blocks_and_transactions_toggle = True
    export_receipts_and_logs_toggle = True
    extract_contracts_toggle = False
    export_contracts_toggle = True
    extract_tokens_toggle = True
    extract_token_transfers_toggle = True
    export_traces_toggle = False
    export_traces_from_gcs = False

    if export_max_active_runs is None:
        export_max_active_runs = configuration.conf.getint('core', 'max_active_runs_per_dag')

    dag = DAG(
        dag_id,
        schedule_interval=export_schedule_interval,
        default_args=default_dag_args,
        max_active_runs=export_max_active_runs
    )

    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    cloud_storage_hook = GCSHook(gcp_conn_id="google_cloud_default")

    # Export
    def export_path(directory, date):
        return "export/{directory}/block_date={block_date}/".format(
            directory=directory, block_date=date.strftime("%Y-%m-%d")
        )

    def copy_to_export_path(file_path, export_path):
        logging.info('Calling copy_to_export_path({}, {})'.format(file_path, export_path))
        filename = os.path.basename(file_path)

   
       
        upload_to_gcs(
            gcs_hook=cloud_storage_hook,
            bucket=output_bucket,
            object=export_path + filename,
            filename=file_path)

    def copy_from_export_path(export_path, file_path):
        logging.info('Calling copy_from_export_path({}, {})'.format(export_path, file_path))
        filename = os.path.basename(file_path)
        
        download_from_gcs(bucket=output_bucket, object=export_path + filename, filename=file_path)

    def get_block_range(tempdir, date, provider_uri):
        logging.info('Calling get_block_range_for_date({}, {}, ...)'.format(provider_uri, date))
        get_block_range_for_date.callback(
            provider_uri=provider_uri, date=date, output=os.path.join(tempdir, "blocks_meta.txt")
        )

        with open(os.path.join(tempdir, "blocks_meta.txt")) as block_range_file:
            block_range = block_range_file.read()
            start_block, end_block = block_range.split(",")

        return int(start_block), int(end_block)

    def export_blocks_and_transactions_command(logical_date, provider_uri, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            start_block, end_block = get_block_range(tempdir, logical_date, provider_uri)

            logging.info('Calling export_blocks_and_transactions({}, {}, {}, {}, {}, ...)'.format(
                start_block, end_block, export_batch_size, provider_uri, export_max_workers))

            export_blocks_and_transactions.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=export_batch_size,
                provider_uri=provider_uri,
                max_workers=export_max_workers,
                blocks_output=os.path.join(tempdir, "blocks.csv"),
                transactions_output=os.path.join(tempdir, "transactions.csv"),
            )

            copy_to_export_path(
                os.path.join(tempdir, "blocks_meta.txt"), export_path("blocks_meta", logical_date)
            )

            copy_to_export_path(
                os.path.join(tempdir, "blocks.csv"), export_path("blocks", logical_date)
            )

            copy_to_export_path(
                os.path.join(tempdir, "transactions.csv"), export_path("transactions", logical_date)
            )

    def export_receipts_and_logs_command(logical_date, provider_uri, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            start_block, end_block = get_block_range(tempdir, logical_date, provider_uri)

            logging.info('Calling export_receipts_and_logs({}, ..., {}, {}, {}, {} ...)'.format(
                export_batch_size, provider_uri, export_max_workers, start_block, end_block))
            export_receipts_and_logs.callback(
                batch_size=export_batch_size,
                provider_uri=provider_uri,
                max_workers=export_max_workers,
                receipts_output=os.path.join(tempdir, "receipts.csv"),
                logs_output=os.path.join(tempdir, "logs.json"),
                start_block=start_block,
                end_block=end_block
            )

            copy_to_export_path(
                os.path.join(tempdir, "receipts.csv"), export_path("receipts", logical_date)
            )
            copy_to_export_path(os.path.join(tempdir, "logs.json"), export_path("logs", logical_date))

    def extract_contracts_command(logical_date, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            copy_from_export_path(
                export_path("traces", logical_date), os.path.join(tempdir, "traces.csv")
            )

            logging.info('Calling extract_contracts(..., {}, {})'.format(
                export_batch_size, export_max_workers
            ))
            extract_contracts.callback(
                traces=os.path.join(tempdir, "traces.csv"),
                output=os.path.join(tempdir, "contracts.json"),
                batch_size=export_batch_size,
                max_workers=export_max_workers,
            )

            copy_to_export_path(
                os.path.join(tempdir, "contracts.json"), export_path("contracts", logical_date)
            )

    # taken from old commit right before they switched to "extract_contracts"
    # https://github.com/blockchain-etl/ethereum-etl-airflow/blob/8a5f8fdb31382bd90ef70ef85976c1955c8bab08/dags/ethereumetl_airflow/build_export_dag.py
    def export_contracts_command(execution_date, provider_uri, **kwargs):
        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                export_path("receipts", execution_date), os.path.join(tempdir, "receipts.csv")
            )

            logging.info('Calling export_contracts({}, ..., {}, {})'.format(
                export_batch_size, export_max_workers, provider_uri
            ))
            export_contracts.callback(
                batch_size=export_batch_size,
                receipts=os.path.join(tempdir, "receipts.csv"),
                output=os.path.join(tempdir, "contracts.json"),
                max_workers=export_max_workers,
                provider_uri=provider_uri,
            )

            copy_to_export_path(
                os.path.join(tempdir, "contracts.json"), export_path("contracts", execution_date)
            )


    def extract_tokens_command(logical_date, provider_uri, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            copy_from_export_path(
                export_path("contracts", logical_date), os.path.join(tempdir, "contracts.json")
            )

            logging.info('Calling extract_tokens(..., {}, {})'.format(export_max_workers, provider_uri))
            extract_tokens.callback(
                contracts=os.path.join(tempdir, "contracts.json"),
                output=os.path.join(tempdir, "tokens.csv"),
                max_workers=export_max_workers,
                provider_uri=provider_uri,
            )

            copy_to_export_path(
                os.path.join(tempdir, "tokens.csv"), export_path("tokens", logical_date)
            )

    def extract_token_transfers_command(logical_date, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            copy_from_export_path(
                export_path("logs", logical_date), os.path.join(tempdir, "logs.json")
            )

            logging.info('Calling extract_token_transfers(..., {}, ..., {})'.format(
                export_batch_size, export_max_workers
            ))
            extract_token_transfers.callback(
                logs=os.path.join(tempdir, "logs.json"),
                batch_size=export_batch_size,
                output=os.path.join(tempdir, "token_transfers.csv"),
                max_workers=export_max_workers,
            )

            copy_to_export_path(
                os.path.join(tempdir, "token_transfers.csv"),
                export_path("token_transfers", logical_date),
            )

    def export_traces_command(logical_date, provider_uri, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            start_block, end_block = get_block_range(tempdir, logical_date, provider_uri)
            if start_block == 0:
                start_block = 1

            export_traces_batch_size = 1
            logging.info('Calling export_geth_traces({}, {}, {}, ...,{}, {}, {}, {})'.format(
                start_block, end_block, export_traces_batch_size, export_max_workers, provider_uri,
                # export_genesis_traces_option, export_daofork_traces_option
            ))

            if not export_traces_from_gcs:
                export_geth_traces.callback(
                    start_block=start_block,
                    end_block=end_block,
                    batch_size=export_traces_batch_size,
                    output=os.path.join(tempdir, "geth_traces.json"),
                    max_workers=export_traces_max_workers,
                    provider_uri=provider_uri
                )
                copy_to_export_path(
                    os.path.join(tempdir, "geth_traces.json"), export_path("traces", logical_date)
                )
            else:
                copy_from_export_path(
                   export_path("traces", logical_date), os.path.join(tempdir, "geth_traces.json"), 
                )

            extract_geth_traces.callback(
                input=os.path.join(tempdir, "geth_traces.json"),
                output=os.path.join(tempdir, 'traces.csv'),
                max_workers=1
            )

            copy_to_export_path(
                os.path.join(tempdir, "traces.csv"), export_path("traces", logical_date)
            )

    def add_export_task(toggle, task_id, python_callable, dependencies=None):
        if toggle:
            operator = PythonOperator(
                task_id=task_id,
                python_callable=python_callable,
                execution_timeout=timedelta(hours=24),
                dag=dag,
            )
            if dependencies is not None and len(dependencies) > 0:
                for dependency in dependencies:
                    if dependency is not None:
                        dependency >> operator
            return operator
        else:
            return None

    # Operators

    export_blocks_and_transactions_operator = add_export_task(
        export_blocks_and_transactions_toggle,
        "export_blocks_and_transactions",
        add_provider_uri_fallback_loop(export_blocks_and_transactions_command, provider_uris_archival),
    )

    export_receipts_and_logs_operator = add_export_task(
        export_receipts_and_logs_toggle,
        "export_receipts_and_logs",
        add_provider_uri_fallback_loop(export_receipts_and_logs_command, provider_uris_archival),
        dependencies=[export_blocks_and_transactions_operator],
    )

    extract_token_transfers_operator = add_export_task(
        extract_token_transfers_toggle,
        "extract_token_transfers",
        extract_token_transfers_command,
        dependencies=[export_receipts_and_logs_operator],
    )

    export_traces_operator = add_export_task(
        export_traces_toggle,
        "export_geth_traces",
        add_provider_uri_fallback_loop(export_traces_command, provider_uris_archival)
    )

    extract_contracts_operator = add_export_task(
        extract_contracts_toggle,
        "extract_contracts",
        extract_contracts_command,
        dependencies=[export_traces_operator],
    )

    export_contracts_operator = add_export_task(
        export_contracts_toggle,
        "export_contracts",
        add_provider_uri_fallback_loop(export_contracts_command, provider_uris),
        dependencies=[export_receipts_and_logs_operator],
    )

    if not extract_contracts_toggle and export_contracts_toggle:
        extract_tokens_deps = [export_contracts_operator]
    else:
        extract_tokens_deps = [extract_contracts_operator]

    extract_tokens_operator = add_export_task(
        extract_tokens_toggle,
        "extract_tokens",
        add_provider_uri_fallback_loop(extract_tokens_command, provider_uris),
        dependencies=extract_tokens_deps,
    )

    return dag


def add_provider_uri_fallback_loop(python_callable, provider_uris):
    """Tries each provider uri in provider_uris until the command succeeds"""

    def python_callable_with_fallback(**kwargs):
        for index, provider_uri in enumerate(provider_uris):
            kwargs['provider_uri'] = provider_uri
            try:
                python_callable(**kwargs)
                break
            except Exception as e:
                if index < (len(provider_uris) - 1):
                    logging.exception('An exception occurred. Trying another uri')
                else:
                    raise e

    return python_callable_with_fallback
