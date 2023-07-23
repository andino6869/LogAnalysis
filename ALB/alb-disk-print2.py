#!/usr/bin/env python3

import pandas as pd
import glob
import matplotlib.pyplot as plt
import os

# Ensure that the metrics directory exists
os.makedirs('metrics', exist_ok=True)

path_pattern = '/BigData/ELB/output/2018-12*.csv.gz'
files = glob.glob(path_pattern)

df_from_each_file = []
for f in files:
    df = pd.read_csv(f, compression='gzip', usecols=["timestamp","status"], parse_dates=['timestamp'])
    df_from_each_file.append(df)

df = pd.concat(df_from_each_file, ignore_index=True)

df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.set_index('timestamp')

all_requests_counts = df.resample('H').size()

# Extract year and month from path_pattern
year_month = path_pattern.split('/')[-1][:7]  # Assuming path_pattern is always in the form '/path/to/your/file/YYYY-MM*.csv.gz'

all_requests_counts.to_csv(f'metrics/all_requests_counts_{year_month}.csv')

status_500_counts = df[df['status'] == 500].resample('H').size()
status_500_percentages = (status_500_counts / all_requests_counts) * 100

status_500_percentages.to_csv(f'metrics/status_500_percentages_{year_month}.csv')

status_codes = df['status'].unique()
error_status_codes = status_codes[(status_codes >= 460) & (status_codes != 500)]

other_status_percentages = pd.DataFrame()

for status in error_status_codes:
    status_counts = df[df['status'] == status].resample('H').size()
    status_percentages = (status_counts / all_requests_counts) * 100
    other_status_percentages[status] = status_percentages

other_status_percentages.to_csv(f'metrics/other_status_percentages_{year_month}.csv')
