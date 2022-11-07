from tqdm import tqdm
import argparse
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from polygonetl.cli.export_blocks_and_transactions import export_blocks_and_transactions
from polygonetl.cli.export_receipts_and_logs import export_receipts_and_logs
from polygonetl.cli.extract_field import extract_field
from polygonetl.cli.export_contracts import export_contracts
from polygonetl.cli.extract_tokens import extract_tokens
from polygonetl.cli.extract_token_transfers import extract_token_transfers
import json
import traceback
import time

parser = argparse.ArgumentParser(description='Does big dump')
parser.add_argument('--output-dir', '-o', type=str, required=True)
parser.add_argument('--provider-uri', '-p', type=str, required=True)
parser.add_argument('--start-block', '-s', type=int, required=True)
parser.add_argument('--end-block', '-e', type=int, required=True)
parser.add_argument('--batch-size', '-b', type=int, default=int(24 * 60 * 60 / 3))
parser.add_argument('--workers', '-w', type=int, default=32)
parser.add_argument('--only-print', help='Used to plan runs', action='store_true')
args = parser.parse_args()

RPC_CALL_BATCH_SIZE = 200
THREAD_WORKERS = 5


def is_in_succesful_batches(start_block, end_block, succesful_batches):
    sb_set = {(sb['start_block'], sb['end_block']) for sb in succesful_batches}
    return (start_block, end_block) in sb_set


def make_if_not_exist(name):
    path = os.path.join(args.output_dir, name)
    if not os.path.exists(path):
        os.mkdir(path)


def main():
    os.mkdir(args.output_dir) if not os.path.exists(args.output_dir) else None
    make_if_not_exist('blocks')
    make_if_not_exist('transactions')
    make_if_not_exist('receipts')
    make_if_not_exist('logs')
    make_if_not_exist('token_transfers')
    make_if_not_exist('tokens')
    make_if_not_exist('tmpdir')

    success_path = os.path.join(args.output_dir, 'succesful_batches.json')
    if os.path.exists(success_path):
        with open(success_path, 'r') as fh:
            successful_batches = json.load(fh)
    else:
        successful_batches = []

    total_blocks = args.end_block - args.start_block + 1
    with tqdm(total=total_blocks) as pbar:
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {}
            for batch_start_block in range(args.start_block, args.end_block, args.batch_size):
                batch_end_block = min(args.batch_size + batch_start_block - 1, args.end_block)
                if is_in_succesful_batches(batch_start_block, batch_end_block, successful_batches):
                    print(f'{batch_start_block}-{batch_end_block} is in succesful_batches, skipping')
                    continue
                if args.only_print:
                    print(batch_start_block, batch_end_block)
                    continue

                future = executor.submit(batch_worker,
                                         batch_start_block,
                                         batch_end_block)
                futures[future] = (batch_start_block, batch_end_block)
            exception_batches = []
            for future in as_completed(futures):
                batch_start_block, batch_end_block = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    print(f'Error processing batch {batch_start_block}-{batch_end_block}')
                    exception_text = ''.join(traceback.format_exception(None, e, e.__traceback__))
                    exception_batches.append({'batch': f'{batch_start_block}-{batch_end_block}', 'traceback': exception_text})
                    with open(os.path.join(args.output_dir, 'exception_batches.json'), 'w') as fh:
                        json.dump(exception_batches, fh)
                else:
                    if result == 'success':
                        successful_batches.append({'start_block': batch_start_block, 'end_block': batch_end_block})
                        with open(os.path.join(args.output_dir, 'succesful_batches.json'), 'w') as fh:
                            json.dump(successful_batches, fh)

                pbar.update(batch_end_block - batch_start_block + 1)

def add_runtime(runtimes):
    runtimes.append(time.time())

def format_runtimes(runtimes):
    deltas = [runtimes[i] - runtimes[i-1] for i in range(1, len(runtimes))]
    return [round(dt, 2) for dt in deltas]

def batch_worker(start_block, end_block):
    runtimes = [time.time()]
    blocks_output = os.path.join(args.output_dir, 'blocks', f'{start_block}-{end_block}.csv')
    transactions_output = os.path.join(args.output_dir, 'transactions', f'{start_block}-{end_block}.csv')
    export_blocks_and_transactions.callback(
        start_block=start_block,
        end_block=end_block,
        batch_size=RPC_CALL_BATCH_SIZE,
        provider_uri=args.provider_uri,
        max_workers=THREAD_WORKERS,
        blocks_output=blocks_output,
        transactions_output=transactions_output
    )
    add_runtime(runtimes)

    receipts_output = os.path.join(args.output_dir, 'receipts', f'{start_block}-{end_block}.csv')
    logs_output = os.path.join(args.output_dir, 'logs', f'{start_block}-{end_block}.json')
    export_receipts_and_logs.callback(
        batch_size=RPC_CALL_BATCH_SIZE,
        provider_uri=args.provider_uri,
        max_workers=THREAD_WORKERS,
        receipts_output=receipts_output,
        logs_output=logs_output,
        start_block=start_block,
        end_block=end_block
    )
    add_runtime(runtimes)

    # NOTE: depends on receipts (from export_receipts_and_logs)
    contracts_output = os.path.join(args.output_dir, 'contracts', f'{start_block}-{end_block}.json')
    export_contracts.callback(
        batch_size=RPC_CALL_BATCH_SIZE,
        receipts=receipts_output,
        output=contracts_output,
        max_workers=THREAD_WORKERS,
        provider_uri=args.provider_uri,
    )
    add_runtime(runtimes)

    # NOTE: depends on contracts (from export_contracts)
    tokens_output = os.path.join(args.output_dir, 'tokens', f'{start_block}-{end_block}.csv')
    extract_tokens.callback(
        contracts=contracts_output,
        output=tokens_output,
        max_workers=THREAD_WORKERS,
        provider_uri=args.provider_uri,
    )
    add_runtime(runtimes)

    # NOTE: depends on logs (from export_receipts_and_logs)
    token_transfers = os.path.join(args.output_dir, 'token_transfers', f'{start_block}-{end_block}.csv')
    extract_token_transfers.callback(
        logs=logs_output,
        batch_size=RPC_CALL_BATCH_SIZE,
        output=token_transfers,
        max_workers=THREAD_WORKERS,
    )
    add_runtime(runtimes)

    print(f"SUCCESS FOR BATCH {start_block}-{end_block}... RUNTIMES: {format_runtimes(runtimes)}")
    return 'success'


if __name__ == '__main__':
    main()
