#!/usr/bin/env python3

import os
import pandas as pd
from urllib.parse import urlparse
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

def extract_subpath(request):
    # Placeholder for your logic
    # This function should return the subpath from the request string
    pass

def process_file(file):
    print(f"Processing file: {file}")
    columns = ['type', 'timestamp', 'elb', 'client:port', 'backend:port',
               'request_processing_time', 'backend_processing_time', 'response_processing_time',
               'elb_status_code', 'backend_status_code', 'received_bytes', 'sent_bytes',
               'request', 'user_agent', 'ssl_cipher', 'ssl_protocol']
    df = pd.read_csv(file, names=columns, compression='gzip', delimiter=' ', quotechar='"', escapechar='\\', on_bad_lines='skip')
    #df['method'], df['url'], df['protocol'] = df['request'].str.split(' ', n=2).str
    df['method'], df['url'], df['protocol'] = zip(*df['request'].apply(lambda x: (x.split(' ', 2) + [None, None, None])[:3]))
    df['subpath'] = df['request'].apply(extract_subpath)
    df.drop(columns=['request'], inplace=True)
    return df

def process_files(files, output_file):
    with ProcessPoolExecutor() as executor:
        dfs = list(executor.map(process_file, files))
    result = pd.concat(dfs)
    result.to_csv(output_file, index=False)

def process_directory(directory):
    p = Path(directory)
    files = list(p.glob('**/*.gz'))
    output_file = f"{p.name}.csv"
    process_files(files, output_file)

def main():
    base_dir = '/BigData/ELB'
    for year_dir in os.scandir(base_dir):
        for month_dir in os.scandir(year_dir.path):
            for day_dir in os.scandir(month_dir.path):
                process_directory(day_dir.path)

if __name__ == "__main__":
    main()
