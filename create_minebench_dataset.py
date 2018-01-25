#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import logging
from tqdm import tqdm


def get_int_name(name):
    return int(os.path.basename(name).split('.')[0])

def blocks_to_csv():
    block_files = sorted(os.listdir('./blocks'), key=get_int_name)
    files_no = len(block_files)
    with open(f"./{files_no}_blocks.csv", 'w') as output:
        # output.write("ver,prev_block,time,tx\n")
        
        for block_file in tqdm(block_files):
            block = json.load(open(f'./blocks/{block_file}'))
            tx = ':'.join([tx['hex'] for tx in block['tx']])

            output.write(f"{block['ver']}," \
                            + f"{block['prev_block']}," \
                            + f"{block['time']}," \
                            + f"{tx}\n")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    blocks_to_csv()
