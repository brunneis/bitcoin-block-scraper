#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import requests
import json
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor


THREADS_NO = 8

def get_block_url(block_index, format='json'):
    return f'https://blockchain.info/block-index/{block_index}?format={format}'

def get_block_data(block_index, format='json'):
    try:
        block_url = get_block_url(block_index, format)

        for i in range(0, 5):
            try:
                response = requests.get(url=block_url)
                block = response.json()
                break
            except json.JSONDecodeError as e:
                block = None
                if response.text[0] == 'B': # Block not found
                    return None, False

        return block, True

    except requests.exceptions.RequestException as e:
        logging.exception(e)

def get_tx_url(tx_hash, format='hex'):
    return f'https://blockchain.info/tx/{tx_hash}?format={format}'

def get_tx_data(tx_hash, format='hex'):
    try:
        tx_url = get_tx_url(tx_hash, format)

        for i in range(0, THREADS_NO * 10):
            tx = requests.get(url=tx_url).text

            # Retry if the response is an HTML/plain text error message:
            # - (<)html>[...]429 Too Many Requests[...]
            # - (M)aximum concurrent requests[...]
            # - (T)ransaction not found
            # - (I)nternal Server Error
            # - (An) attempt by a client[...]
            if tx[0] != '<' \
                and tx[0] != 'M' \
                and tx[0] != 'T' \
                and tx[0] != 'I' \
                and (tx[0] != 'A' and tx[1] != 'n'):
                break
            else:
                tx = None
                time.sleep(1)

        return tx
    except requests.exceptions.RequestException:
        logging.exception()

def download_txs(tx_hash):
    logging.info(f'Downloading tx: {tx_hash}')
    tx_hex = get_tx_data(tx_hash)
    if not tx_hex:
        raise RuntimeError()
    return tx_hex

def dump_block(block):
    if not os.path.exists('blocks'):
        os.mkdir('blocks')
    with open(f"./blocks/{block['height']}.json", 'w') as output:
        json.dump(block, output)

def exists_block(height):
    return os.path.isfile(f"./blocks/{height}.json")


def download(height=0):
    if len(sys.argv) == 2:
        height = int(sys.argv[1])

    block_index = 14849 # Genesis block index
    block_index += height

    running = True
    while(running):
        if exists_block(height):
            block_index += 1
            logging.info(f'Skipped block {height}')
            height += 1
            continue

        logging.info(f'Retrieving index #{block_index}')
        block, found = get_block_data(block_index)

        if found:
            logging.info(f'Retrieved block #{height}\n * * *')

        if not block:
            block_index += 1
            logging.warn('Skipping block index...')
            continue

        height = block['height'] + 1
        block_index += 1

        with ThreadPoolExecutor(max_workers=THREADS_NO) as executor:
            futures = []
            for tx in block['tx']:
                tx_hash = tx['hash']
                future = executor.submit(download_txs, tx_hash)
                futures.append(future)

            try:
                for future, tx in zip(futures, block['tx']):
                    tx['hex'] = future.result()
            except RuntimeError:
                logging.exception('Error retrieving transaction. Skipping block...')
                continue

        dump_block(block)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    download()
