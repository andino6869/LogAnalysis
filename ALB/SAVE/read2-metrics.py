#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt

# Load metrics from CSV files
all_requests_counts = pd.read_csv('metrics/all_requests_counts.csv', index_col=0, parse_dates=True)
status_500_percentages = pd.read_csv('metrics/status_500_percentages.csv', index_col=0, parse_dates=True)
other_status_percentages = pd.read_csv('metrics/other_status_percentages.csv', index_col=0, parse_dates=True)

# 1st figure: all requests
all_requests_counts.plot()
plt.yscale('log')
plt.title('Time Series of Hourly Request Counts (log scale)')
plt.xlabel('Date and Hour')
plt.ylabel('Count')
plt.grid()
plt.show()

# 2nd figure: percentage of 500 status code
status_500_percentages.plot(label='500', legend=True)
plt.title('Time Series of Hourly Error 500 as Percentage of All Requests')
plt.xlabel('Date and Hour')
plt.ylabel('Percentage (%)')
plt.grid()
plt.show()

# 3rd figure: percentage of other error status codes (>=460 but not 500)
fig, ax = plt.subplots()

# Unstack the DataFrame to get a Series, where each column represents a status code
other_status_percentages_unstacked = other_status_percentages.unstack()

for status in other_status_percentages_unstacked.columns:
    status_percentages = other_status_percentages_unstacked[status].dropna()
    status_percentages.plot(ax=ax, label='Status {}'.format(status))

plt.legend(loc='upper left')
plt.title('Time Series of Hourly Other Error Statuses as Percentage of All Requests')
plt.xlabel('Date and Hour')
plt.ylabel('Percentage (%)')
plt.grid()
plt.show()
