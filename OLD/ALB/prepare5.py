#!/usr/bin/env python3

import os
import pandas as pd
import gzip
import glob
from concurrent.futures import ProcessPoolExecutor

# define the columns of your DataFrame
columns = [ "proto", "timestamp", "elb", "client:port", "target:port", "request_processing_time",
            "latency", "response_processing_time", "status", "target_status", "r_bytes", "s_bytes",
            "request", "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn", "trace_id",
            "domain_name", "chosen_cert_arn", "matched_rule_priority", "request_creation_time",
            "actions_executed", "r_url", "error_reason", "target_ip", "target_status_description",
            "code", "target_response_duration", "target_health_description"]

def process_file(file_path):
    # specify the indices of the columns you want to keep
    keep_cols = [1, 7, 8, 9, 10, 11, 13, 20, 21, 22]
    with gzip.open(file_path, 'rt') as file:
        df = pd.read_csv(file,  sep=' ', usecols=keep_cols, quotechar='"', engine='python')
    df.columns = [columns[i] for i in keep_cols]
    return df, df.columns.tolist()

def read_parallel(file_path=None):
    if file_path is None:
        files = glob.glob('/BigData/ELB/2018/11/*/*.log.gz')
    else:
        files = [file_path]

    total_files = len(files)
    print(f"Processing {total_files} files")

    dfs = []
    modified_columns = None
    with ProcessPoolExecutor() as executor:
        for i, result in enumerate(executor.map(process_file, files), 1):
            df, cols = result
            dfs.append(df)
            if modified_columns is None:
                modified_columns = cols
            if i % 1000 == 0 or i == total_files:
                print(f'{i} files of {total_files} processed ({i / total_files * 100:.2f} %)')

    final_df = pd.concat(dfs, ignore_index=True)
    return final_df, modified_columns

def get_dataframe_and_columns(file_path=None):
    return read_parallel(file_path)

if __name__ == "__main__":
    import sys
    file_path = sys.argv[1] if len(sys.argv) > 1 else None
    df, modified_columns = get_dataframe_and_columns(file_path)
    print(df)
    print(modified_columns)
