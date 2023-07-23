#!/usr/bin/env python3

import pandas as pd
import glob
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

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

df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.set_index('timestamp')

# Status 500
status_500_counts = df[df['status'] == 500].resample('H').size()
status_500_counts.to_csv('status_500_counts.csv')

# All requests
all_requests_counts = df.resample('H').size()
all_requests_counts.to_csv('all_requests_counts.csv')

# Other status codes >= 460 (excluding 500)
other_status_counts = df[(df['status'] >= 460) & (df['status'] != 500)].resample('H').size()
other_status_counts.to_csv('other_status_counts.csv')

# Plotting - Load counts from CSV
all_requests_counts = pd.read_csv('all_requests_counts.csv', index_col=0, parse_dates=True)
status_500_counts = pd.read_csv('status_500_counts.csv', index_col=0, parse_dates=True)
other_status_counts = pd.read_csv('other_status_counts.csv', index_col=0, parse_dates=True)

# Figure 1: All Requests
fig, ax = plt.subplots()
all_requests_counts.plot(label='All Requests', legend=True)
plt.title('Time Series of Hourly All Requests')
plt.xlabel('Date and Hour')
plt.ylabel('Count')
plt.savefig('all_requests.png')  # Save the figure to a file
plt.show()

# Figure 2: Status 500 Percentage
status_500_percentages = (status_500_counts / all_requests_counts) * 100
fig, ax = plt.subplots()
status_500_percentages.plot(label='Percentage of Status 500', legend=True)
plt.title('Time Series of Hourly Percentage of Status 500')
plt.xlabel('Date and Hour')
plt.ylabel('Percentage')
plt.savefig('percentage_status_500.png')  # Save the figure to a file
plt.show()

# Other status codes >= 460 (excluding 500)
# Other status codes >= 460 (excluding 500)
unique_status_codes = df[(df['status'] >= 460) & (df['status'] != 500)]['status'].unique()

# Create figure and axes
fig, ax = plt.subplots()

for status_code in unique_status_codes:
    other_status_counts = df[df['status'] == status_code].resample('H').size()
    other_status_counts.to_csv(f'status_{status_code}_counts.csv')

    # Load counts from CSV
    other_status_counts = pd.read_csv(f'status_{status_code}_counts.csv', index_col=0, parse_dates=True)

    # Figure 3: Other Status Percentage
    other_status_percentages = (other_status_counts / all_requests_counts) * 100
    if not other_status_percentages.empty and other_status_percentages.values.any():
        other_status_percentages.plot(label=f'Status {status_code}', ax=ax)

if len(ax.lines) > 0:  # If there are any lines on the plot
    plt.title('Time Series of Hourly Percentage of Statuses')
    plt.xlabel('Date and Hour')
    plt.ylabel('Percentage')
    plt.legend(loc='best')
    plt.show()
else:
    print("No data to plot.")





