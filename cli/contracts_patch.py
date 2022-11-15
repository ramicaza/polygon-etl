# from google.cloud import bigquery
import logging
import datetime as dt
import pandas_gbq
from google.oauth2 import service_account
import os
from polygonetl.cli import export_contracts
from polygonetl.cli import extract_tokens
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback
import json
import argparse
from tempfile import TemporaryDirectory


def parse_date(d): return dt.datetime.strptime(d, '%Y%m%d').date()


parser = argparse.ArgumentParser(description='Does big dump')
parser.add_argument('--output-dir', '-o', type=str, required=True)
parser.add_argument('--provider-uri', '-p', type=str, required=True)
# 2015-08-07 is first eth contract date
parser.add_argument('--start-date', '-s', type=parse_date, required=True)
parser.add_argument('--end-date', '-e', type=parse_date, required=True)
parser.add_argument('--dataset-name', '-d', type=str, required=True)
parser.add_argument('--workers', '-w', type=int, default=32)
args = parser.parse_args()

# public-data-finance.crypto_polygon
# bigquery-public-data.crypto_ethereum
# 'https://wild-restless-valley.quiknode.pro/c60f24249de4b4912bc95bbeb8f1cd6afe87588e/'
# 'http://54.187.135.32:8545'


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
FROM `{args.dataset_name}.contracts`
WHERE DATE(block_timestamp)='{date.strftime("%Y-%m-%d")}';
    """
    df = pandas_gbq.read_gbq(sql)

    contracts_path = os.path.join(args.output_dir, 'contracts', f'{date.strftime("%Y-%m-%d")}.json')
    with TemporaryDirectory() as tempdir:
        addresses_path = os.path.join(tempdir, 'contract_addresses.csv')
        df.to_csv(addresses_path)

        export_contracts.callback(
            batch_size=200,
            receipts=addresses_path,
            output=contracts_path,
            max_workers=5,
            provider_uri=args.provider_uri,
        )
    
    tokens_path = os.path.join(args.output_dir, 'tokens', f'{date.strftime("%Y-%m-%d")}.csv')
    extract_tokens.callback(
        contracts=contracts_path,
        output=tokens_path,
        max_workers=5,
        provider_uri=args.provider_uri,
    )
    


def main():
    setup_pandas_gbq('./agave-pipeline-9ae294d3b02a.json')

    date_range = list(pd.date_range(args.start_date, args.end_date))
    with tqdm(total=len(date_range)) as pbar:
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {}
            for date in date_range:
                future = executor.submit(process_date, date)
                futures[future] = date
            exception_batches = []
            for future in as_completed(futures):
                date = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    print(f'Error processing batch {date}')
                    exception_text = ''.join(traceback.format_exception(None, e, e.__traceback__))
                    exception_batches.append({'batch': f'{date}', 'traceback': exception_text})
                    with open(os.path.join(args.output_dir, 'exception_batches.json'), 'w') as fh:
                        json.dump(exception_batches, fh)

                pbar.update(1)


if __name__ == '__main__':
    main()
