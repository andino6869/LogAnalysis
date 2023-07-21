#!/usr/bin/env python3
from concurrent.futures import ProcessPoolExecutor
import dask.dataframe as dd
import glob

columns = ['type', 'time', 'elb', 'client:port', 'target:port', 'request_processing_time',
           'target_processing_time', 'response_processing_time', 'elb_status_code', 'target_status_code', 
           'received_bytes', 'sent_bytes', 'request', 'user_agent', 'ssl_cipher', 'ssl_protocol', 
           'target_group_arn', 'trace_id',  'domain_name', 'chosen_cert_arn',  'matched_rule_priority',
           'request_creation_time', 'actions_executed', 'redirect_url', 'error_reason', 'target_ip_list',
           'target_status_code_list', 'classification', 'classification_reason']

def read_csv_file(file):
    return dd.read_csv(file, compression='gzip', sep=' ', quotechar='"', header=None, names=columns, engine='python', 
                        blocksize=None, dtype={'target_status_code': 'object'})

def get_dataframe_and_columns(file_path=None):
    if file_path is None:
        files = glob.glob('/BigData/ELB/2018/12/*/*.log.gz')
    else:
        files = [file_path]
        
    with ProcessPoolExecutor() as executor:
        dfs = list(executor.map(read_csv_file, files))

    df = dd.concat(dfs)

    df = df.drop(["type", "elb", "target:port", "request_processing_time", "target_processing_time", 
                  "response_processing_time", "elb_status_code", "target_status_code", "user_agent", 
                  "ssl_cipher", "ssl_protocol", "target_group_arn", "trace_id",  "domain_name", 
                  "chosen_cert_arn",  "matched_rule_priority", "request_creation_time", "actions_executed",
                  "redirect_url", "error_reason", "target_ip_list", "target_status_code_list", 
                  "classification", "classification_reason"], axis=1)
    
    modified_columns = df.columns.tolist()

    return df, modified_columns
