#!/usr/bin/env python3

import os
import pandas as pd
import glob
import gzip
import matplotlib.pyplot as plt
import numpy as np

def save_to_csv(df, filename):
    df.to_csv(filename, header=True)

# Specify columns and file path
columns = ["timestamp", "status", "latency", "r_bytes", "method", "uri"]
path_pattern = '/BigData/ELB/output/2018-11*.csv.gz'

# Grab files and initialize empty dataframe list
files = glob.glob(path_pattern)
dfs = []

columns_to_use = ["timestamp", "status"]

# Load each file into a dataframe and append to list
for file in files:
    df = pd.read_csv(file, compression='gzip', header=None, names=columns, skiprows=1, sep=',', quotechar='"', engine='python', usecols=columns_to_use, parse_dates=["timestamp"],
         dtype={'status': 'int16', 'latency': 'float32', 'r_bytes': 'int16'})

    df.set_index('timestamp', inplace=True)
    dfs.append(df)

print("gzip load done ...")

# Concatenate all dataframes
df = pd.concat(dfs)

print("came together")

# Resample and count requests per hour
all_requests_counts = df.resample('H').size()
error_500_counts = df[df['status'] == 500].resample('H').size()

# Calculate percentage of 500 error status codes per hour
status_500_percentages = (error_500_counts / all_requests_counts) * 100

# Define error codes
error_status_codes = df[df['status'] >= 460]['status'].unique()
error_status_codes = error_status_codes[error_status_codes != 500]

# Create a directory to store the metrics if it doesn't exist
os.makedirs('metrics', exist_ok=True)

# Save all requests counts and status 500 percentages to CSV
save_to_csv(all_requests_counts, 'metrics/all_requests_counts.csv')
save_to_csv(status_500_percentages, 'metrics/status_500_percentages.csv')

for status in error_status_codes:
    error_status_counts = df[df['status'] == status].resample('H').size()
    error_status_percentages = (error_status_counts / all_requests_counts) * 100
    save_to_csv(error_status_counts, f'metrics/error_{status}_counts.csv')
    save_to_csv(error_status_percentages, f'metrics/error_{status}_percentages.csv')
