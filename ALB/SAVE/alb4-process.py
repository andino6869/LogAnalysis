#!/usr/bin/env python3

import pandas as pd
import glob
import matplotlib.pyplot as plt

columns = ["timestamp", "status", "latency", "r_bytes", "method", "uri"]

path_pattern = '/BigData/ELB/output/2018-11*.csv.gz'
files = glob.glob(path_pattern)
dfs = []

columns_to_use = ["timestamp","status"]
for file in files:
    df = pd.read_csv(file, compression='gzip', header=None, names=columns, skiprows=1, sep=',', quotechar='"', engine='python', usecols=columns_to_use,
         dtype={'status': 'int16', 'latency': 'float32', 'r_bytes': 'int16'})

    # Convert 'timestamp' to datetime and set it as the index
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp')

    dfs.append(df)
print("gzip load done ...")
df = pd.concat(dfs)
print("came together")

error_502_counts = df[df['status'] == 502].resample('H').size()
error_500_counts = df[df['status'] == 500].resample('H').size()
error_499_counts = df[df['status'] == 499].resample('H').size()

error_502_counts = error_502_counts.fillna(0)
error_500_counts = error_500_counts.fillna(0)
error_499_counts = error_499_counts.fillna(0)

# Check if series are not empty before plotting
if not error_502_counts.empty:
    error_502_counts.plot(label='502', legend=True)
if not error_500_counts.empty:
    error_500_counts.plot(label='500', legend=True)
if not error_499_counts.empty:
    error_499_counts.plot(label='499', legend=True)

plt.title('Time Series of Hourly Error Counts')
plt.xlabel('Date and Hour')
plt.ylabel('Count')
plt.yscale('log')
plt.show()

