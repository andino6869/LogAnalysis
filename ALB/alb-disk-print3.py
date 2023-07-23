#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import argparse
import gzip

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument("year_month", help="Year and month in YYYY-MM format")
args = parser.parse_args()

year_month = args.year_month
path_pattern = f'/BigData/ELB/output/{year_month}*.csv.gz'

files = glob.glob(path_pattern)

df_from_each_file = []
bad_files = []  # Store messages about bad files

for f in files:
    try:
        df = pd.read_csv(f, compression='gzip', usecols=["timestamp","status"], parse_dates=['timestamp'])
        df_from_each_file.append(df)
    except gzip.BadGzipFile:
        bad_files.append(f)  # Append the bad file to the list

# Print all the messages about bad files after processing all files
for bad_file in bad_files:
    print(f"Skipping bad file: {bad_file}")

df = pd.concat(df_from_each_file, ignore_index=True)

df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.set_index('timestamp')

all_requests_counts = df.resample('H').size()

status_500_counts = df[df['status'] == 500].resample('H').size()
status_500_percentages = (status_500_counts / all_requests_counts) * 100

status_codes = df['status'].unique()
error_status_codes = status_codes[(status_codes >= 460) & (status_codes != 500)]

other_status_percentages_list = []
for status in error_status_codes:
    status_counts = df[df['status'] == status].resample('H').size()
    status_percentages = (status_counts / all_requests_counts) * 100
    other_status_percentages_list.append(status_percentages)

other_status_percentages = pd.concat(other_status_percentages_list, axis=1)
other_status_percentages.columns = error_status_codes

# Save the metrics to disk
os.makedirs('metrics', exist_ok=True)
all_requests_counts.to_csv(f'metrics/all_requests_counts_{year_month}.csv')
status_500_percentages.to_csv(f'metrics/status_500_percentages_{year_month}.csv')
other_status_percentages.to_csv(f'metrics/other_status_percentages_{year_month}.csv')