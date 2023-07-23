#!/usr/bin/env python3

import pandas as pd
import glob
import matplotlib.pyplot as plt

path_pattern = '/BigData/ELB/output/2018-11*.csv.gz'
files = glob.glob(path_pattern)

df_from_each_file = []
for f in files:
    df = pd.read_csv(f, compression='gzip', usecols=["timestamp","status"], parse_dates=['timestamp'])
    df_from_each_file.append(df)

df = pd.concat(df_from_each_file, ignore_index=True)

df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.set_index('timestamp')

all_requests_counts = df.resample('H').size()

# 1st figure: all requests
all_requests_counts.plot()
plt.yscale('log')
plt.title('Time Series of Hourly Request Counts (log scale)')
plt.xlabel('Date and Hour')
plt.ylabel('Count')
plt.grid()
plt.show()

# 2nd figure: percentage of 500 status code
status_500_counts = df[df['status'] == 500].resample('H').size()
status_500_percentages = (status_500_counts / all_requests_counts) * 100
status_500_percentages.plot(label='500', legend=True)
plt.title('Time Series of Hourly Error 500 as Percentage of All Requests')
plt.xlabel('Date and Hour')
plt.ylabel('Percentage (%)')
plt.grid()
plt.show()

# 3rd figure: percentage of other error status codes (>=460 but not 500)
status_codes = df['status'].unique()
error_status_codes = status_codes[(status_codes >= 460) & (status_codes != 500)]

fig, ax = plt.subplots()

for status in error_status_codes:
    status_counts = df[df['status'] == status].resample('H').size()
    status_percentages = (status_counts / all_requests_counts) * 100
    status_percentages.plot(ax=ax, label='Status {}'.format(status))

plt.legend(loc='upper left')
plt.title('Time Series of Hourly Other Error Statuses as Percentage of All Requests')
plt.xlabel('Date and Hour')
plt.ylabel('Percentage (%)')
plt.grid()
plt.show()
