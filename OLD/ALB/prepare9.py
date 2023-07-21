#!/usr/bin/env python3

import os
import glob
import dask.dataframe as dd
from concurrent.futures import ProcessPoolExecutor
import datetime

def read_csv_file(file):
    print(f"Processing file: {file}")
    return dd.read_csv(file, compression='gzip', sep=' ', quotechar='"', header=None, names=columns, dtype=dtype_dict, engine='python', blocksize=None)

def get_dataframe_and_columns():
    print(f"{datetime.datetime.now()}: Starting the process...")
    global columns
    columns = ['timestamp', 'elb', 'client:port', 'backend:port', 'request_processing_time', 'backend_processing_time', 'response_processing_time', 
               'elb_status_code', 'backend_status_code', 'received_bytes', 'sent_bytes', 'request', 'user_agent', 'ssl_cipher', 
               'ssl_protocol', 'target_group_arn', 'trace_id', 'domain_name', 'chosen_cert_arn', 'matched_rule_priority', 
               'request_creation_time', 'actions_executed', 'redirect_url', 'error_reason', 'target:port_list', 'target_status_code_list', 
               'classification', 'classification_reason']
    global dtype_dict
    dtype_dict = dict(zip(columns, ['object'] * len(columns)))

    #files = glob.glob('/BigData/ELB/2018/10/28/*.log.gz')
    files = '/BigData/ELB/2018/12/11/647933830095_elasticloadbalancing_eu-central-1_app.awseb-AWSEB-18IHG6R3W1A7K.ac573d33cdf8d1a2_20181211T2355Z_35.157.194.235_tbc04iw9.log.gz'
    print(f"Number of files: {len(files)}")

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        dfs = list(executor.map(read_csv_file, files))

    df = dd.concat(dfs, axis=0, interleave_partitions=True)
    df = df.compute() 

    df['timestamp'] = dd.to_datetime(df['timestamp'])
    df['request_processing_time'] = df['request_processing_time'].astype('float16')
    df['backend_processing_time'] = df['backend_processing_time'].astype('float16')
    df['response_processing_time'] = df['response_processing_time'].astype('float16')
    df['received_bytes'] = df['received_bytes'].astype('uint32')
    df['sent_bytes'] = df['sent_bytes'].astype('uint32')
    df['request'] = df['request'].astype(str)
    df['user_agent'] = df['user_agent'].astype(str)
    df['ssl_cipher'] = df['ssl_cipher'].astype(str)
    df['ssl_protocol'] = df['ssl_protocol'].astype(str)

    return df, columns
