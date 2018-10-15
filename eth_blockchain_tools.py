import csv
import itertools
import subprocess
import time
import datetime
import calendar
import threading
import numpy as np
import multiprocessing as mp

from multiprocessing import Pool
from operator import itemgetter
from functools import partial
from helpers import *
from pathlib import Path
from web3 import Web3
from collections import deque
from subprocess import Popen, CREATE_NEW_CONSOLE

provider = Web3.IPCProvider('\\\\.\\pipe\\geth.ipc')
w3 = Web3(provider)

#initialize the block 
if w3.isConnected() == False:
    process = Popen([r'C:\Program Files\Geth\geth.exe', '--fast'], encoding='utf8', creationflags=CREATE_NEW_CONSOLE)
    while w3.isConnected == False:
        time.sleep(0.5)

current_block = get_current_block() # current block is the highest block in our chain
highest_block = get_highest_block()

tx_data_filter = [
    'blockNumber',
    'gas',
    'gasUsed',
    'gasPrice'
]

tx_filter = lambda x: itemgetter(*tx_data_filter)(x)

'''
Blocks is a class that represents blocks on the ETH blockchain of some token adddress
'''
class Blocks(): 
    def __init__(self, addr, tx_data_filter=tx_data_filter): 
        self.current_block = current_block
        self.current_timestamp = get_block(self.current_block)['timestamp'] 
        self.current_datetime = datetime.datetime.utcfromtimestamp(self.current_timestamp)        
        self.filter = [ #filtering the data to be selected
            'blockHash',
            'transactionHash',
            'gas',
            'gasPrice',
            'gasUsed',
            'cumulativeGasUsed'
        ]         
        self.addr = addr

    # get the day data that's days away 
    def days_away_data(self, addr, days):
        pass

    # get the cumulative day data that's days away 
    def cum_days_away_data(self, addr, days):
        pass

    # get the data between these blocks using parallel
    def par_get_data(self, start_block, end_block):
        pass
    
    # get the data between these blocks normally
    def reg_get_data(self, start_block, end_block):
        pass

    # get block number that's starts the day 
    def day_start_block(self, end_block):
        # get end_block timestamp/date
        end_block_timestamp = get_block(end_block)['timestamp']
        end_block_date = datetime.datetime.utcfromtimestamp(end_block_timestamp)
        day_start_block_timestamp = calendar.timegm(end_block_date.date().timetuple())
        # finding the block which is the earliest in the day
        day_start_block = end_block - (end_block_timestamp - day_start_block_timestamp) // 15 # initial block estimate
        return get_day_start_block(day_start_block, day_start_block_timestamp)

    # get day length block numbers that is days away from some starting block number
    def days_from_block(self, end_block, days): # end block is most recent block
        if days == 0: # if days == 0 then we return the days_start_block of end_block
            return self.day_start_block(end_block)
        # get end_block timestamp/date
        end_block_timestamp = get_block(end_block)['timestamp']
        end_block_date = datetime.datetime.utcfromtimestamp(end_block_timestamp)
        # get open timestamps
        open_timestamp = calendar.timegm((end_block_date - datetime.timedelta(days)).date().timetuple())
        # estimates for block numbers
        open_block = end_block - (end_block_timestamp - open_timestamp) // 15        
        return get_day_start_block(open_block, open_timestamp)

    # returns the data on some UTC day 
    def par_data_on_date(self, date_time):
        # compute the start/end blocks
        days_away = (self.current_datetime - date_time).days
        start_block = self.days_from_block(self.current_block, days_away)
        end_block = self.days_from_block(self.current_block, days_away-1) - 1
        # sources of iteration
        logs = get_log_partitions(start_block, end_block, self.addr)
        tx_hashes = [x['transactionHash'] for x in logs]
        block_numbers = [x['blockNumber'] for x in logs]

        pool = Pool(processes=4)

        # get tx times, a job
        
        tx_times = pool.map(get_tx_times_single, block_numbers)

        # get tx data, a job

        tx_data = pool.map(get_tx_data_single, tx_hashes)
        
        # getting receipts data, a job 

        receipts_data = pool.map(get_receipts_data_single, tx_hashes)

        #aggregating data
        raw_data_per_tx = [{**x, **y} for x,y in zip(tx_data, receipts_data)]
        filt_data_per_tx = [dict(zip(tx_data_filter, tx_filter(x))) for x in raw_data_per_tx]
        
        # last step of combining tx times with tx data
        final_data_per_tx = [{**x, **{'timestamp':y}} for x,y in zip(filt_data_per_tx, tx_times)]
        return final_data_per_tx        

    def data_on_date(self, date_time):
        # compute the start/end blocks
        days_away = (self.current_datetime - date_time).days
        start_block = self.days_from_block(self.current_block, days_away)
        end_block = self.days_from_block(self.current_block, days_away-1) - 1
        # sources of iteration
        
        logs = get_log_partitions(start_block, end_block, self.addr)
        tx_hashes = [x['transactionHash'] for x in logs]
        block_numbers = [x['blockNumber'] for x in logs]

        # get tx times, a job
        tx_times = []
        get_tx_times(block_numbers, tx_times)

        # get tx data, a job
        tx_data = []
        get_tx_data(tx_hashes, tx_data)
        
        # getting receipts data, a job 
        receipts_data = []
        get_receipts_data(tx_hashes, receipts_data)
        
        #aggregating data
        raw_data_per_tx = [{**x, **y} for x,y in zip(tx_data, receipts_data)]
        filt_data_per_tx = [dict(zip(tx_data_filter, tx_filter(x))) for x in raw_data_per_tx]
        
        # last step of combining tx times with tx data
        final_data_per_tx = [{**x, **{'timestamp':y}} for x,y in zip(filt_data_per_tx, tx_times)]
        return final_data_per_tx

    # pull data between block numbers 
    def data_between(self, start_block, end_block):
        if abs(end_block-start_block)*15 // 86400 < 25: # less than 25 days worth of blocks
            return self._data_between(start_block, end_block)
        else:
            return self._par_data_between(start_block, end_block)
    
    # non parallel version of getting data
    def _data_between(self, start_block, end_block):
        return get_log(start_block, end_block, self.addr) # need to parallelize this 
    
    # parallel version of getting data
    def _par_data_between(self, start_block, end_block):
        pass

    # def par_data_on_date(self, date_time): #date_time must be type datetime in utc
    #     # compute the start/end blocks
    #     days_away = (self.current_datetime - date_time).days
    #     start_block = self.days_from_block(self.current_block, days_away)
    #     end_block = self.days_from_block(self.current_block, days_away-1) - 1
    #     # sources of iteration
    #     logs = get_log_partitions(start_block, end_block, self.addr)
    #     tx_hashes = [x['transactionHash'] for x in logs]
    #     block_numbers = [x['blockNumber'] for x in logs]

    #     # get tx times, a job
    #     tx_times = mp.Manager().list()
    #     getting_tx_times = threading.Thread(target=get_tx_times, args=(block_numbers, tx_times))

    #     # get tx data, a job
    #     tx_data = mp.Manager().list()
    #     getting_tx_data = threading.Thread(target=get_tx_data, args=(tx_hashes, tx_data))
        
    #     # getting receipts data, a job 
    #     receipts_data = mp.Manager().list()
    #     getting_receipts_data = threading.Thread(target=get_receipts_data, args=(tx_hashes, receipts_data))

    #     # starting jobs
    #     getting_tx_times.start()
    #     getting_tx_data.start()
    #     getting_receipts_data.start()
    
    #     # joining
    #     getting_tx_times.join()
    #     getting_tx_data.join()
    #     getting_receipts_data.join()

    #     #aggregating data
    #     raw_data_per_tx = [{**x, **y} for x,y in zip(tx_data, receipts_data)]
    #     filt_data_per_tx = [dict(zip(tx_data_filter, tx_filter(x))) for x in raw_data_per_tx]
        
    #     # last step of combining tx times with tx data
    #     final_data_per_tx = [{**x, **{'timestamp':y}} for x,y in zip(filt_data_per_tx, tx_times)]
    #     return final_data_per_tx
