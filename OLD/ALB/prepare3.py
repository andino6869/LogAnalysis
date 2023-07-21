#!/usr/bin/env python3

import pandas as pd
import gzip
import glob 
import sys
import argparse

def get_dataframe_and_columns(filepath=None):

    columns = [
        "proto", "timestamp", "elb", "client:port", "target:port", 
        "request_processing_time", "latency", "response_processing_time", 
        "status", "target_status", "r_bytes", "s_bytes", "request",
        "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn",
        "trace_id", "domain_name", "chosen_cert_arn",
        "matched_rule_priority", "request_creation_time",
        "actions_executed", "r_url", "error_reason",
        "target_ip", "target_status_description", "code",
        "target_response_duration", "target_health_description"
    ]

    if filepath is None:  # no filepath was provided as a function argument
        files = glob.glob('/BigData/ELB/2018/12/*/*.log.gz')
        df = pd.DataFrame()
        for file in files:
            with gzip.open(file, 'rt') as f:
                temp_df = pd.read_csv(f, sep=' ', names=columns, quotechar='"', engine='python')
                df = pd.concat([df, temp_df])
    else:  # a filepath was provided as a function argument
        with gzip.open(filepath, 'rt') as file:
            df = pd.read_csv(file,  sep=' ', names=columns, quotechar='"', engine='python')
    
    df.drop(["proto", "elb", "target_status", "client:port", "target:port", 
             "request_processing_time", "response_processing_time", "user_agent", 
             "ssl_cipher", "ssl_protocol", "target_group_arn", "trace_id", 
             "domain_name", "chosen_cert_arn", "matched_rule_priority", 
             "request_creation_time", "actions_executed", "error_reason", 
             "target_ip", "target_status_description",
             "target_response_duration", "target_health_description"], axis=1, inplace=True)

    modified_columns = df.columns.tolist()
    return df, modified_columns

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath", nargs='?', default=None, help="Optional file path argument")
    args = parser.parse_args()
    df, modified_columns = get_dataframe_and_columns(args.filepath)
    print(df)
    print(modified_columns)

'''
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath", nargs='?', default=None, help="Optional file path argument")
    args = parser.parse_args()
    df, modified_columns = get_dataframe_and_columns(args.filepath)
'''
