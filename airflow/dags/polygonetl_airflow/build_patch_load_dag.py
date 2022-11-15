from __future__ import print_function

import json
import logging
import os
import time
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

from airflow import models
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning

from polygonetl_airflow.bigquery_utils import submit_bigquery_job

from polygonetl_airflow.gcs_utils import upload_to_gcs

from utils.error_handling import handle_dag_failure

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


enrich_contracts_sql =lambda blocks_dataset: """
SELECT
    contracts.address,
    contracts.bytecode,
    contracts.function_sighashes,
    contracts.is_erc20,
    contracts.is_erc721,
    blocks.timestamp AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM {{params.dataset_name_raw}}.contracts AS contracts
    JOIN {{BLOCKS_DATASET}}.blocks AS blocks ON contracts.block_number = blocks.number
where true
    {% if not params.load_all_partitions %}
    and date(timestamp_seconds(blocks.timestamp)) = '{{ds}}'
    {% endif %}

""".replace("{{BLOCKS_DATASET}}", f'`{blocks_dataset}`')

enrich_tokens_sql = lambda blocks_dataset: """
SELECT
    address,
    symbol,
    name,
    decimals,
    total_supply,
    blocks.timestamp AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM {{BLOCKS_DATASET}}.blocks AS blocks
    JOIN {{params.dataset_name_raw}}.tokens AS tokens ON blocks.number = tokens.block_number
where true
    {% if not params.load_all_partitions %}
    and date(timestamp_seconds(blocks.timestamp)) = '{{ds}}'
    {% endif %}

""".replace("{{BLOCKS_DATASET}}", f'`{blocks_dataset}`')

def build_patch_load_dag(
    dag_id,
    output_bucket,
    checkpoint_bucket,
    destination_dataset_project_id,
    chain='polygon',
    notification_emails=None,
    load_start_date=datetime(2018, 7, 1),
    load_schedule_interval='0 0 * * *',
    load_all_partitions=True,
    blocks_dataset=None
):
    # The following datasets must be created in BigQuery:
    dataset_name = f'patch_{chain}'
    dataset_name_raw = f'patch_{chain}_raw'
    dataset_name_temp = f'patch_{chain}_temp'

    if not destination_dataset_project_id:
        raise ValueError('destination_dataset_project_id is required')

    environment = {
        'dataset_name': dataset_name,
        'dataset_name_raw': dataset_name_raw,
        'dataset_name_temp': dataset_name_temp,
        'destination_dataset_project_id': destination_dataset_project_id,
        'load_all_partitions': load_all_partitions
    }

    def read_bigquery_schema_from_file(filepath):
        result = []
        file_content = read_file(filepath)
        json_content = json.loads(file_content)
        for field in json_content:
            result.append(bigquery.SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description')))
        return result

    def read_file(filepath):
        with open(filepath) as file_handle:
            content = file_handle.read()
            return content

    default_dag_args = {
        'depends_on_past': False,
        'start_date': load_start_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': handle_dag_failure,
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    # Define a DAG (directed acyclic graph) of tasks.
    dag = models.DAG(
        dag_id,
        catchup=False,
        schedule_interval=load_schedule_interval,
        default_args=default_dag_args)

    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')

    def add_load_tasks(task, file_format, allow_quoted_newlines=False):
        wait_sensor = GCSObjectExistenceSensor(
            task_id='wait_latest_{task}'.format(task=task),
            timeout=12 * 60 * 60,
            poke_interval=60,
            bucket=output_bucket,
            # object='export/patch_{chain}/{task}/block_date={datestamp}/{task}.{file_format}'.format(
            #     chain=chain, task=task, datestamp='{{ds}}', file_format=file_format),
            object='export/patch_{chain}/{task}/{datestamp}.{file_format}'.format(
                chain=chain, task=task, datestamp='{{ds}}', file_format=file_format),
            dag=dag
        )

        def load_task():
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig()
            schema_path = os.path.join(dags_folder, 'resources/stages/raw/schemas/{task}.json'.format(task=task))
            job_config.schema = read_bigquery_schema_from_file(schema_path)
            job_config.source_format = bigquery.SourceFormat.CSV if file_format == 'csv' else bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            if file_format == 'csv':
                job_config.skip_leading_rows = 1
            job_config.write_disposition = 'WRITE_TRUNCATE'
            job_config.allow_quoted_newlines = allow_quoted_newlines
            job_config.ignore_unknown_values = True

            export_location_uri = 'gs://{bucket}/export/patch_{chain}'.format(bucket=output_bucket, chain=chain)
            uri = '{export_location_uri}/{task}/*.{file_format}'.format(
                export_location_uri=export_location_uri, task=task, file_format=file_format)
            table_ref = client.dataset(dataset_name_raw).table(task)
            load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
            submit_bigquery_job(load_job, job_config)
            assert load_job.state == 'DONE'

        load_operator = PythonOperator(
            task_id='load_{task}'.format(task=task),
            python_callable=load_task,
            execution_timeout=timedelta(minutes=30),
            dag=dag
        )

        wait_sensor >> load_operator
        return load_operator

    def add_enrich_tasks(task, time_partitioning_field='block_timestamp', dependencies=None, always_load_all_partitions=False):
        def enrich_task(ds, **kwargs):
            template_context = kwargs.copy()
            template_context['ds'] = ds
            template_context['params'] = environment

            client = bigquery.Client()

            # Need to use a temporary table because bq query sets field modes to NULLABLE and descriptions to null
            # when writeDisposition is WRITE_TRUNCATE

            # Create a temporary table
            temp_table_name = '{task}_{milliseconds}'.format(task=task, milliseconds=int(round(time.time() * 1000)))
            temp_table_ref = client.dataset(dataset_name_temp).table(temp_table_name)

            schema_path = os.path.join(dags_folder, 'resources/stages/enrich/schemas/{task}.json'.format(task=task))
            schema = read_bigquery_schema_from_file(schema_path)
            table = bigquery.Table(temp_table_ref, schema=schema)

            description_path = os.path.join(
                dags_folder, 'resources/stages/enrich/descriptions/{task}.txt'.format(task=task))
            table.description = read_file(description_path)
            if time_partitioning_field is not None:
                table.time_partitioning = TimePartitioning(field=time_partitioning_field)
            logging.info('Creating table: ' + json.dumps(table.to_api_repr()))
            table = client.create_table(table)
            assert table.table_id == temp_table_name

            # Query from raw to temporary table
            query_job_config = bigquery.QueryJobConfig()
            # Finishes faster, query limit for concurrent interactive queries is 50
            query_job_config.priority = bigquery.QueryPriority.INTERACTIVE
            query_job_config.destination = temp_table_ref

            
            if task == 'contracts':
                sql_template = enrich_contracts_sql(blocks_dataset)
            elif task == 'tokens':
                sql_template = enrich_tokens_sql(blocks_dataset)
            else:
                raise Exception("Dis shit fucked")

                
            sql = kwargs['task'].render_template(sql_template, template_context)
            print('Enrichment sql:')
            print(sql)

            query_job = client.query(sql, location='US', job_config=query_job_config)
            submit_bigquery_job(query_job, query_job_config)
            assert query_job.state == 'DONE'

            if load_all_partitions or always_load_all_partitions:
                # Copy temporary table to destination
                copy_job_config = bigquery.CopyJobConfig()
                copy_job_config.write_disposition = 'WRITE_TRUNCATE'
                dest_table_name = '{task}'.format(task=task)
                dest_table_ref = client.dataset(dataset_name, project=destination_dataset_project_id).table(dest_table_name)
                copy_job = client.copy_table(temp_table_ref, dest_table_ref, location='US', job_config=copy_job_config)
                submit_bigquery_job(copy_job, copy_job_config)
                assert copy_job.state == 'DONE'
            else:
                # Merge
                # https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
                merge_job_config = bigquery.QueryJobConfig()
                # Finishes faster, query limit for concurrent interactive queries is 50
                merge_job_config.priority = bigquery.QueryPriority.INTERACTIVE

                merge_sql_path = os.path.join(
                    dags_folder, 'resources/stages/enrich/sqls/merge/merge_{task}.sql'.format(task=task))
                merge_sql_template = read_file(merge_sql_path)

                merge_template_context = template_context.copy()
                merge_template_context['params']['source_table'] = temp_table_name
                merge_template_context['params']['destination_dataset_project_id'] = destination_dataset_project_id
                merge_template_context['params']['destination_dataset_name'] = dataset_name
                merge_sql = kwargs['task'].render_template(merge_sql_template, merge_template_context)
                print('Merge sql:')
                print(merge_sql)
                merge_job = client.query(merge_sql, location='US', job_config=merge_job_config)
                submit_bigquery_job(merge_job, merge_job_config)
                assert merge_job.state == 'DONE'

            # Delete temp table
            client.delete_table(temp_table_ref)

        enrich_operator = PythonOperator(
            task_id='enrich_{task}'.format(task=task),
            python_callable=enrich_task,
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> enrich_operator
        return enrich_operator

    def add_save_checkpoint_tasks(dependencies=None):
        def save_checkpoint(logical_date, **kwargs):
            with TemporaryDirectory() as tempdir:
                local_path = os.path.join(tempdir, "checkpoint.txt")
                remote_path = "patch_load/checkpoint/block_date={block_date}/checkpoint.txt".format(
                    block_date=logical_date.strftime("%Y-%m-%d")
                )
                open(local_path, mode='a').close()
                upload_to_gcs(
                    gcs_hook=GCSHook(gcp_conn_id="google_cloud_default"),
                    bucket=checkpoint_bucket,
                    object=remote_path,
                    filename=local_path,
                )

        save_checkpoint_task = PythonOperator(
            task_id='save_checkpoint',
            python_callable=save_checkpoint,
            execution_timeout=timedelta(hours=1),
            dag=dag,
        )
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> save_checkpoint_task
        return save_checkpoint_task

    load_contracts_task = add_load_tasks('contracts', 'json')
    load_tokens_task = add_load_tasks('tokens', 'csv', allow_quoted_newlines=True)

    enrich_contracts_task = add_enrich_tasks(
        'contracts', dependencies=[load_contracts_task])
    enrich_tokens_task = add_enrich_tasks(
        'tokens', dependencies=[load_tokens_task])
    # Save checkpoint tasks #

    save_checkpoint_task = add_save_checkpoint_tasks(dependencies=[
        enrich_contracts_task,
        enrich_tokens_task,
    ])

    return dag
