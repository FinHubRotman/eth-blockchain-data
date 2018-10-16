'''
a helper module for eth_blockchain_tools
this module contains many simple functions on getting data off the ethereum blockchain
'''
import itertools

import numpy as np

from web3 import Web3
from functools import wraps

provider = Web3.IPCProvider('\\\\.\\pipe\\geth.ipc')
w3 = Web3(provider)

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# retry wrapper
# this wrapper assumes only one single error will be made which is the error from overloading the IPC
# if anyother error happens it will continue to loop something indefinitely, so please watch out
def retry(f): 
    @wraps(f)
    def wrapped(*args, **kwargs):
        while True:
            try:
                return f(*args, **kwargs)
            except:
                pass
    return wrapped

def days_diff_btwn(current_time, block_time):
    return (current_time - block_time) / 86400  

def blocks_diff_btwn(current_time, block_time, days):
    return int(np.ceil((days - days_diff_btwn(current_time, block_time)) * (86400 / 40)))

def get_current_block():
    return w3.eth.syncing['currentBlock']

def get_highest_block():
    return w3.eth.syncing['highestBlock']

def get_day_start_block(block, reach_timestamp, counts=0):
    block_left = block - 1 
    block_timestamp = get_block(block)['timestamp']
    block_left_timestamp = get_block(block_left)['timestamp']
    if block_left_timestamp < reach_timestamp and block_timestamp >= reach_timestamp:
        return block
    else:
        time_delta = reach_timestamp - block_timestamp # difference in time between current block timestamp vs readtimestamp
        block_delta = time_delta // 15 # if alternating == False else int(time_delta/abs(time_delta))
        if counts > 10:
            block_delta = int(time_delta/abs(time_delta))
            return get_day_start_block(block + block_delta, reach_timestamp, counts=counts)
        else:
            return get_day_start_block(block + block_delta, reach_timestamp, counts=counts+1)

#partitions the block into [x,y] where x-y = interval_length
def block_partitions(start_block, end_block, interval_length): 
    all_blocks = [start_block + i for i in range(end_block-start_block+1)]
    starts = all_blocks[::interval_length]
    ends = all_blocks[interval_length-1:][::interval_length] + all_blocks[-1:]
    return list(zip(starts,ends))

def get_log_partitions(from_block, to_block, addr, interval_length=100):
    partitions = block_partitions(from_block, to_block, interval_length)
    log_parts = []
    for start_block, end_block in partitions:
        log_part = get_log(start_block, end_block, addr)
        log_parts.append(log_part)
    log_parts = list(itertools.chain.from_iterable(log_parts))
    return log_parts

def get_tx_times(block_numbers):
    tmp = []
    for block_num in block_numbers:
        tmp.append(get_block(block_num)['timestamp'])
    return tmp

def get_tx_data(tx_hashes):
    tmp = []
    for tx_hash in tx_hashes:
        tmp.append(get_tx(tx_hash))
    return tmp

def get_receipts_data(tx_hashes):
    tmp = []
    for tx_hash in tx_hashes:
        tmp.append(get_receipt(tx_hash))
    return tmp

@retry
def get_tx(tx_hash):
    return w3.eth.getTransaction(tx_hash)

@retry
def get_receipt(tx_hash):
    return w3.eth.getTransactionReceipt(tx_hash)

@retry
def get_block(block_hash):
    return w3.eth.getBlock(block_hash)

@retry
def past_days(current_time, block_time, days):
    return True if (current_time - block_time) / 86400 > days else False

@retry
def get_log(from_block, to_block, addr):
    logs = w3.eth.filter({
                'fromBlock': from_block,
                'toBlock': to_block,
                'address': addr
    }).get_all_entries()
    return logs

@retry
def worker(tx_hash):
    got_hash = w3.eth.getTransaction(tx_hash)
    return got_hash