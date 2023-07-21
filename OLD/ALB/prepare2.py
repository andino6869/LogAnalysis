#!/usr/bin/env python3

import pandas as pd
import gzip
import glob
import argparse

columns = [
    "proto",                                              # skip
    "timestamp",                    #          2023-03-04T00:40:00.673223Z
    "elb",                                                # skip
    "client:port",                                        # 18.197.198.192:5310
    "target:port",                                        # skip
    "request_processing_time",                            # skip
    "latency",                      # float32 (?)         0.110
    "response_processing_time",                           # skip
    "status",                       # string(int16)         200
    "target_status",                                      # 200
    "r_bytes",                      # int16                  702
    "s_bytes",                      # int16                  537
    "request",                      # string (?)         POST https://pc-energy.encore-prod.enbw.cloud:443/service/Electricity HTTP/1.1
    "user_agent",                                         # skip
    "ssl_cipher",                                         # skip
    "ssl_protocol",                                       # skip
    "target_group_arn",                                   # skip
    "trace_id",                                           # skip
    "domain_name",                                        # skip
    "chosen_cert_arn",                                    # skip
    "matched_rule_priority",                              # skip
    "request_creation_time",
    "actions_executed",                                   # skip
    "r_url",                        # string                 -
    "error_reason",                                       # skip
    "target_ip",                                          # skip
    "target_status_description",                          # skip
    "code",                         # float64 (?)         NaN   jedenfalls am Anfang
    "target_response_duration",                           # skip
    "target_health_description"                           # skip
]

def get_dataframe_and_columns(file_path=None):
    if file_path is None:
        files = glob.glob('/BigData/ELB/2018/12/*/*.log.gz')
    else:
        files = [file_path]

    df = pd.concat([
        pd.read_csv(file, sep=' ', names=columns, quotechar='"', engine='python')
        for file in files
    ])

    df.drop([
        "proto", "elb", "target_status", "client:port", "target:port",
        "request_processing_time", "response_processing_time",
        "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn",
        "trace_id", "domain_name", "chosen_cert_arn",
        "matched_rule_priority", "request_creation_time", "actions_executed",
        "error_reason", "target_ip", "target_status_description",
        "target_response_duration", "target_health_description"
    ], axis=1, inplace=True)

    modified_columns = df.columns.tolist()
    return df, modified_columns

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process a specific log file or all log files in the given directory.')
    parser.add_argument('file_path', nargs='?', default=None, help='The path of the log file to process.')
    args = parser.parse_args()

    df, modified_columns = get_dataframe_and_columns(args.file_path)
    print(f"Dataframe shape: {df.shape}")
    print(f"Columns: {modified_columns}")

