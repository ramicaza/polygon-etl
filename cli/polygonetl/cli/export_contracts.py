# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import click

from blockchainetl_common.file_utils import smart_open
from polygonetl.jobs.export_contracts_job import ExportContractsJob
from polygonetl.jobs.exporters.contracts_item_exporter import contracts_item_exporter
from blockchainetl_common.logging_utils import logging_basic_config
from polygonetl.thread_local_proxy import ThreadLocalProxy
from polygonetl.providers.auto import get_provider_from_uri
import csv

logging_basic_config()


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='The number of blocks to filter at a time.')
@click.option('-r', '--receipts', required=True, type=str,
              help='The receipts file, this script looks for lines that define contract-address')
@click.option('-o', '--output', default='-', show_default=True, type=str, help='The output file. If not specified stdout is used.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Bor/geth.ipc or https://mainnet.infura.io')
@click.option('-c', '--chain', default='polygon', show_default=True, type=str, help='The chain network to connect to.')
def export_contracts(batch_size, receipts, output, max_workers, provider_uri, chain='polygon'):
    """Exports contracts bytecode and sighashes."""
    with smart_open(receipts, 'r') as receipts_file:
        receipts_iterable = csv.DictReader(receipts_file)
        creation_receipts_iterable = [r for r in receipts_iterable if r['contract_address'] is not None and r['contract_address'] != '']
        job = ExportContractsJob(
            receipts_iterable=creation_receipts_iterable,
            batch_size=batch_size,
            batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
            item_exporter=contracts_item_exporter(output),
            max_workers=max_workers)

        job.run()
