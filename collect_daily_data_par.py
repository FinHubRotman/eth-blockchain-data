def agg_data(final_data):
    total_tx = len(final_data)
    total_gas = sum([x['gasUsed'] for x in final_data])
    gw_gasprice = sum([x['gasPrice']*(x['gasUsed']/total_gas) for x in final_data])
    gw_mean_gasprice = np.mean([x['gasPrice']*(x['gasUsed']/total_gas) for x in final_data])
    return {'total_tx':total_tx, 'total_gas':total_gas, 'gw_gasprice':gw_gasprice, 'gw_mean_gasprice':gw_mean_gasprice}

def rm_inv_filename(filename):
    return re.sub('[<>:"\\|?*]', '', filename) 

def write_token_data(start_date, date, token_list):
    for name, sym, addr in token_list:
        filename = "./andreas_daily_data/" + rm_inv_filename(name) + "-" + rm_inv_filename(sym) + ".csv"
        # check if we already have said date written on the file
        already_written = False
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            reader_list = list(reader)
            file_dates = [x[0] for x in reader_list]
            try:
                if date.strftime('%Y-%m-%d') in file_dates:
                    already_written = True
            except IndexError:
                pass
        if not already_written:
            with open(filename, 'a', newline="") as f:
                writer = csv.DictWriter(f, fieldnames=['date', 'total_tx', 'total_gas', 'gw_gasprice', 'gw_mean_gasprice'])
                blocks = Blocks(addr)
                if (start_date - date).days < 100: # if we're 100
                    date_data = blocks.data_on_date(date)
                    date = date.strftime('%Y-%m-%d')
                    data_with_date = {**{'date':date}, **agg_data(date_data)}
                    writer.writerow(data_with_date)
                    print(name, data_with_date)
                else:
                    date_data = blocks.par_data_on_date(date)
                    date = date.strftime('%Y-%m-%d')
                    data_with_date = {**{'date':date}, **agg_data(date_data)}
                    writer.writerow(data_with_date)

if __name__ == '__main__':
    import datetime
    import csv
    import time
    import os
    import re
    import functools
    from multiprocessing.dummy import threading
    from eth_blockchain_tools import Blocks
    from helpers import *

    # open the token addresses file
    token_file = open('token_data_nameaddr.csv', 'r')
    reader = csv.reader(token_file)
    token_list = list(reader)[1:]
    token_file.close()

    # filter token_list
    new_lists = []
    for token in token_list:
        new_lists.append(
            [token[1], token[2], token[3][7:]]
        )

    # create the files
    token_writers = [] 
    for _,name,sym,addr in token_list[:]: # for each 
        filename = "./andreas_daily_data/" + rm_inv_filename(name) + "-" + rm_inv_filename(sym) + ".csv"    
        if os.path.isfile(filename):
            pass
        else:
            # making new file
            with open(filename, 'w', newline="") as f:
                writer=csv.DictWriter(f, fieldnames=['date', 'total_tx', 'total_gas', 'gw_gasprice', 'gw_mean_gasprice'])
                writer.writeheader()

    start_date = datetime.datetime(2018,10,1)
    # for i in list(range(273))[:]: # go backwards in time
    dates = [start_date + datetime.timedelta(days=-1*i) for i in range(273)]
    fix_start = functools.partial(write_token_data, start_date)
    procs = []
    for date in dates:
        fix_date = functools.partial(fix_start, date)
        t = threading.Thread(target=fix_start, args=(new_lists,))
        procs.append(t)

    for proc in procs:
        proc.start()

    for proc in procs:
        proc.join()