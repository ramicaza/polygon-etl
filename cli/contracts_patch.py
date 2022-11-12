# from google.cloud import bigquery
import logging
import datetime as dt
import pandas_gbq
from google.oauth2 import service_account
import os
from polygonetl.cli import export_contracts
import pandas as pd

# DATASET_NAME = 'public-data-finance.crypto_polygon'
DATASET_NAME = 'bigquery-public-data.crypto_ethereum'
PROJECT_ID = 'agave-pipeline'
PROVIDER_URI = 'https://wild-restless-valley.quiknode.pro/c60f24249de4b4912bc95bbeb8f1cd6afe87588e/'
OUTPUT_DIR = 'patch'


def setup_pandas_gbq(service_account_file='./service_account.json'):
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = 'agave-pipeline'


def process_date(date: dt.date):
    sql = f"""
SELECT address as contract_address, block_number
FROM `{DATASET_NAME}.contracts`
WHERE DATE(block_timestamp)='{date.strftime("%Y-%m-%d")}';
    """
    df = pandas_gbq.read_gbq(sql)

    day_directory = os.path.join(OUTPUT_DIR, f'block_date={date.strftime("%Y-%m-%d")}')
    os.mkdir(day_directory)

    addresses_path = os.path.join(day_directory, 'contract_addresses.csv')
    df.to_csv(addresses_path)
    export_contracts.callback(
        batch_size=200,
        receipts=addresses_path,
        output=os.path.join(day_directory, "contracts.json"),
        max_workers=5,
        provider_uri=PROVIDER_URI,
    )
    os.remove(addresses_path)


def main():
    setup_pandas_gbq('./agave-pipeline-9ae294d3b02a.json')
    for date in pd.date_range(dt.date(2015, 8, 7), dt.date(2022, 11, 9)):
        # print(date.date())
        process_date(date)
    # test_date = dt.date(2022, 11, 6)
    # 2015-08-07 is first eth contract date
    # process_date(test_date)


if __name__ == '__main__':
    main()
