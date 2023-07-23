#!/usr/bin/env python3

import pandas as pd
import glob
import matplotlib
matplotlib.use('Agg')  # Use the 'Agg' backend for non-graphical environments
import matplotlib.pyplot as plt

columns = ["timestamp", "status", "latency", "r_bytes", "method", "uri"]

path_pattern = '/BigData/ELB/output/2018-11*.csv.gz'
files = glob.glob(path_pattern)
dfs = []

columns_to_use = ["timestamp","status"]
for file in files:
    df = pd.read_csv(file, compression='gzip', header=None, names=columns, skiprows=1, sep=',', quotechar='"', engine='python', usecols=columns_to_use,
         dtype={'status': 'int16', 'latency': 'float32', 'r_bytes': 'int16'})
    dfs.append(df)
print("gzip load done ...")
df = pd.concat(dfs)
print("came together")

# Convert timestamp column to datetime index
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)

# Resample and count all requests and status 500 requests per hour
all_requests_counts = df.resample('H').size()
status_500_counts = df[df['status'] == 500].resample('H').size()

# Calculate the percentage of status 500 requests from all requests
status_500_percentage = (status_500_counts / all_requests_counts * 100).fillna(0)

plt.yscale('log')

# Check if series are not empty before plotting
if not all_requests_counts.empty:
    all_requests_counts.plot(label='All Requests', legend=True)
if not status_500_percentage.empty:
    status_500_percentage.plot(label='500 Errors (%)', legend=True)

plt.title('Time Series of Hourly Requests and Error 500 Percentage')
plt.xlabel('Date and Hour')
plt.ylabel('Count / Percentage')
#plt.savefig('requests_and_error_500_percentage.png')  # Save the figure to a file


