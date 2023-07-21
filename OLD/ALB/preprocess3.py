#!/usr/bin/env python3

import os
import pandas as pd
import concurrent.futures

def extract_subpath(url):
    if isinstance(url, str) and len(url) > 0:
        return '/'.join(url.split('/')[3:])
    return None

def safe_request_split(request):
    parts = request.split(' ', 2)
    if len(parts) == 3:
        return pd.Series(parts)
    return pd.Series([None, None, None])

def process_file(file):
    columns = ['type', 'timestamp', 'elb', 'client:port', 'backend:port', 
               'request_processing_time', 'backend_processing_time', 'response_processing_time',
               'elb_status_code', 'backend_status_code', 'received_bytes', 'sent_bytes',
               'request', 'user_agent', 'ssl_cipher', 'ssl_protocol']

    try:
        df = pd.read_csv(file, names=columns, compression='gzip', delimiter=' ', quotechar='"', escapechar='\\', on_bad_lines='skip')
    except pd.errors.ParserError:
        print(f"Could not process file {file} due to malformed 'request' field.")
        return pd.DataFrame()

    # Check if the DataFrame is empty or contains only headers
    if df.empty or df.iloc[0]['type'] == 'type':
        print(f"File {file} contains only headers or is empty. Skipping.")
        return pd.DataFrame()

    # Split the 'request' column into 'method', 'url', 'protocol' safely
    df[['method', 'url', 'protocol']] = df['request'].apply(safe_request_split)

    df['subpath'] = df['url'].apply(extract_subpath)

    # Drop unwanted columns
    drop_cols = ['elb', 'client:port', 'backend:port', 'ssl_cipher', 'ssl_protocol', 'request']
    df = df.drop(columns=drop_cols, errors='ignore')

    return df

def process_files(files):
    output_dfs = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for df in executor.map(process_file, files):
            if not df.empty:
                output_dfs.append(df)
    return output_dfs

def process_directory(dir_path, output_file):
    print(f"Processing directory: {dir_path}")
    files = [os.path.join(dir_path, file) for file in os.listdir(dir_path) if file.endswith('.log.gz')]
    dfs = process_files(files)
    if not dfs:
        print("No valid data found. Exiting.")
        return

    result = pd.concat(dfs)
    result.to_csv(output_file, index=False, compression='gzip')
    print(f"Processed data written to {output_file}")

def main():
    import sys
    if len(sys.argv) < 4:
        print("Usage: ./preprocess3.py YYYY MM [DD]")
        sys.exit(1)

    year = int(sys.argv[1])
    month = int(sys.argv[2])
    day = int(sys.argv[3]) if len(sys.argv) == 4 else None

    if day is None:
        base_dir = f'/BigData/ELB/{year}/{month:02d}'
    else:
        base_dir = f'/BigData/ELB/{year}/{month:02d}/{day:02d}'

    output_file = f'{year:04d}-{month:02d}-{day:02d}.csv.gz' if day else f'{year:04d}-{month:02d}.csv.gz'
    output_file = os.path.join('Processed', output_file)

    process_directory(base_dir, output_file)

if __name__ == "__main__":
    main()

