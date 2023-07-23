#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import glob

# Define the months you want to load, as a list of year-month strings
months = ['2023-01', '2023-02']

all_requests_counts = pd.concat([pd.read_csv(f'metrics/all_requests_counts_{month}.csv', index_col=0, parse_dates=True) for month in months])

all_requests_counts.plot()
plt.yscale('log')
plt.title('Time Series of Hourly Request Counts (log scale)')
plt.xlabel('Date and Hour')
plt.ylabel('Count')
plt.grid()
plt.show()

status_500_percentages = pd.concat([pd.read_csv(f'metrics/status_500_percentages_{month}.csv', index_col=0, parse_dates=True) for month in months])

status_500_percentages.plot(label='500', legend=True)
plt.title('Time Series of Hourly Error 500 as Percentage of All Requests')
plt.xlabel('Date and Hour')
plt.ylabel('Percentage (%)')
plt.grid()
plt.show()

other_status_percentages_files = glob.glob('metrics/other_status_percentages_*.csv')

# Read the CSVs and store them in a list
dfs = [pd.read_csv(f, index_col=0, parse_dates=True) for f in other_status_percentages_files]

# Concatenate all dataframes in the list
other_status_percentages = pd.concat(dfs)

fig, ax = plt.subplots()

for column in other_status_percentages.columns:
    other_status_percentages[column].plot(ax=ax, label='Status {}'.format(column))

plt.legend(loc='upper left')
plt.title('Time Series of Hourly Other Error Statuses as Percentage of All Requests')
plt.xlabel('Date and Hour')
plt.ylabel('Percentage (%)')
plt.grid()
plt.show()
