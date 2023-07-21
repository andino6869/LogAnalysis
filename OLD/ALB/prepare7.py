#!/usr/bin/env python3

import os
import pandas as pd
import gzip
import glob
from concurrent.futures import ProcessPoolExecutor

import os
import dask.dataframe as dd
import glob

# define the columns of your DataFrame
columns = [ "proto", "timestamp", "elb", "client:port", "target:port", "request_processing_time",
            "latency", "response_processing_time", "status", "target_status", "r_bytes", "s_bytes",
            "request", "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn", "trace_id",
            "domain_name", "chosen_cert_arn", "matched_rule_priority", "request_creation_time",
            "actions_executed", "r_url", "error_reason", "target_ip", "target_status_description",
            "code", "target_response_duration", "target_health_description"]

columns_to_drop = ["proto", "elb", "target_status", "client:port", "target:port", "request_processing_time",
                   "response_processing_time", "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn",
                   "trace_id",  "domain_name", "chosen_cert_arn",  "matched_rule_priority", "request_creation_time",
                   "actions_executed", "error_reason", "target_ip", "target_status_description",
                   "target_response_duration", "target_health_description"]

def read_dataframe(file_path=None):
    if file_path is None:
        files = glob.glob('/BigData/ELB/2018/12/*/*.log.gz')
    else:
        files = [file_path]

    df = dd.read_csv(files, sep=' ', names=columns, quotechar='"', 
                     engine='python', assume_missing=True)
    df = df.drop(columns_to_drop, axis=1)
    modified_columns = df.columns.tolist()
    
    return df, modified_columns

if __name__ == "__main__":
    import sys
    file_path = sys.argv[1] if len(sys.argv) > 1 else None
    df, modified_columns = read_dataframe(file_path)
    df = df.compute()  # this loads the data into memory, make sure you have enough RAM or comment out this line
    print(df.head())
    print(modified_columns)
