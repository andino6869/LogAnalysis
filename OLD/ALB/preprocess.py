#!/usr/bin/env python3

import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import gzip

def process_file(file):
    columns = ["proto", "timestamp", "elb", "client:port", "target:port",
               "request_processing_time", "latency", "response_processing_time",
               "status", "target_status", "r_bytes", "s_bytes", "request",
               "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn",
               "trace_id", "domain_name", "chosen_cert_arn",
               "matched_rule_priority", "request_creation_time", "actions_executed",
               "r_url", "error_reason", "target_ip", "target_status_description",
               "code", "target_response_duration", "target_health_description"]
    df = pd.read_csv(file, names=columns, compression='gzip', delimiter=' ', quotechar='"', escapechar='\\',
                     on_bad_lines='warn')
    df[['method', 'url', 'protocol']] = df['request'].str.split(' ', n=2, expand=True)
    df['subpath'] = df['url'].apply(lambda x: '/'.join(x.split('/')[3:]))
    keep_cols = ['timestamp', 'latency', 'status', 'subpath', 'method']
    df = df[keep_cols]
    return df

def process_files(files):
    with ProcessPoolExecutor() as executor:
        dfs = list(executor.map(process_file, files))
    final_df = pd.concat(dfs, ignore_index=True)
    return final_df

def main():
    directory = "/BigData/ELB/2018/11/11/"
    files = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.log.gz')]
    result = process_files(files)
    result.to_csv('/BigData/ELB/2018/11/Daily/processed_data.csv.gz', index=False, compression='gzip')

if __name__ == "__main__":
    main()
