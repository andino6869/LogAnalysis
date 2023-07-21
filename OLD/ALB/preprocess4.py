#!/usr/bin/env python3

import os
import sys
import pandas as pd
import glob
import gzip

columns = ["proto", "timestamp", "elb", "client:port", "target:port", "request_processing_time",
           "latency", "response_processing_time", "status", "target_status", "r_bytes", "s_bytes",
           "request", "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn", "trace_id",
           "domain_name", "chosen_cert_arn", "matched_rule_priority", "request_creation_time",
           "actions_executed", "r_url", "error_reason", "target_ip", "target_status_description",
           "code", "target_response_duration", "target_health_description"]

columns_to_drop = ["proto", "elb", "target_status", "client:port", "target:port", "request_processing_time",
                   "response_processing_time", "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn",
                   "trace_id", "domain_name", "chosen_cert_arn", "matched_rule_priority", "request_creation_time",
                   "actions_executed", "error_reason", "target_ip", "target_status_description",
                   "target_response_duration", "target_health_description"]

def extract_subpath(url):
    return url.split(" ")[1]

def process_file(file):
    try:
        with gzip.open(file, "rt", encoding="utf-8") as f:
            df = pd.read_csv(f, delimiter=" ", header=None, names=columns)
            df['subpath'] = df['request'].apply(extract_subpath)
            df['method'] = df['request'].apply(lambda x: x.split(" ")[0])
            df['url'] = df['request'].apply(lambda x: x.split(" ")[1])
            df['protocol'] = df['request'].apply(lambda x: x.split(" ")[2])
            df.drop(columns=columns_to_drop + ['request'], inplace=True)
            return df
    except Exception as e:
        print(f"Error processing file {file}: {e}")
        return None

def process_files(files):
    dfs = []
    for file in files:
        df = process_file(file)
        if df is not None:
            dfs.append(df)
    return dfs

def process_directory(base_dir, output_file):
    files = glob.glob(os.path.join(base_dir, "*.log.gz"))
    if not files:
        print("No valid data found. Nothing to process")
        return

    dfs = process_files(files)
    if not dfs:
        print("No valid data found. Nothing to process")
        return

    result = pd.concat(dfs)
    result.to_csv(output_file, index=False)

def main():
    if len(sys.argv) != 4:
        print("Usage: python preprocess4.py <year> <month> <day>")
        return

    base_dir = f"/BigData/ELB/{sys.argv[1]}/{sys.argv[2]}/{sys.argv[3]}"
    output_file = f"{sys.argv[1]}-{sys.argv[2]}-{sys.argv[3]}-output.csv"
    process_directory(base_dir, output_file)

if __name__ == "__main__":
    main()

