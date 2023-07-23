#!/usr/bin/env python3

import pandas as pd
import os
import glob
import matplotlib.pyplot as plt

# gather all files
all_requests_files = sorted(glob.glob('metrics/all_requests_counts_*.csv'))
status_500_files = sorted(glob.glob('metrics/status_500_percentages_*.csv'))
other_status_files = sorted(glob.glob('metrics/other_status_percentages_*.csv'))

# initialize empty dataframes
all_requests_df = pd.DataFrame()
status_500_df = pd.DataFrame()
other_status_df = pd.DataFrame()

def read_and_concatenate(files, df):
    for filename in files:
        try:
            temp_df = pd.read_csv(filename)
            df = pd.concat([df, temp_df])
        except Exception as e:
            print(f"Error reading file {filename}: {e}")
    return df

# read and concatenate all_requests_counts files
all_requests_df = read_and_concatenate(all_requests_files, all_requests_df)

# read and concatenate status_500_percentages files
status_500_df = read_and_concatenate(status_500_files, status_500_df)

# read and concatenate other_status_percentages files
other_status_df = read_and_concatenate(other_status_files, other_status_df)

# Convert timestamp columns to datetime and set as index
all_requests_df['timestamp'] = pd.to_datetime(all_requests_df['timestamp'])
all_requests_df.set_index('timestamp', inplace=True)

status_500_df['timestamp'] = pd.to_datetime(status_500_df['timestamp'])
status_500_df.set_index('timestamp', inplace=True)

other_status_df['timestamp'] = pd.to_datetime(other_status_df['timestamp'])
other_status_df.set_index('timestamp', inplace=True)

# Sorting dataframes by index (timestamp)
all_requests_df.sort_index(inplace=True)
status_500_df.sort_index(inplace=True)
other_status_df.sort_index(inplace=True)

# Plotting
# 1st figure: all requests
all_requests_df.plot()
plt.yscale('log')
plt.title('Time Series of Hourly Request Counts (log scale)')
plt.xlabel('Date and Hour')
plt.ylabel('Count')
plt.grid()
plt.show()

# 2nd figure: percentage of 500 status code
status_500_df.plot()
plt.title('Time Series of Hourly Error 500 as Percentage of All Requests')
plt.xlabel('Date and Hour')
plt.ylabel('Percentage (%)')
plt.grid()
plt.show()

# 3rd figure: percentage of other error status codes
other_status_df.plot()
plt.title('Time Series of Hourly Other Error Statuses as Percentage of All Requests')
plt.xlabel('Date and Hour')
plt.ylabel('Percentage (%)')
plt.grid()
plt.show()
