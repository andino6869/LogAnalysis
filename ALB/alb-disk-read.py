#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt

all_requests_counts = pd.read_csv('metrics/all_requests_counts.csv', index_col=0, parse_dates=True)

all_requests_counts.plot()
plt.yscale('log')
plt.title('Time Series of Hourly Request Counts (log scale)')
plt.xlabel('Date and Hour')
plt.ylabel('Count')
plt.grid()
plt.show()

status_500_percentages = pd.read_csv('metrics/status_500_percentages.csv', index_col=0, parse_dates=True)

status_500_percentages.plot(label='500', legend=True)
plt.title('Time Series of Hourly Error 500 as Percentage of All Requests')
plt.xlabel('Date and Hour')
plt.ylabel('Percentage (%)')
plt.grid()
plt.show()

other_status_percentages = pd.read_csv('metrics/other_status_percentages.csv', index_col=0, parse_dates=True)

fig, ax = plt.subplots()

for column in other_status_percentages.columns:
    other_status_percentages[column].plot(ax=ax, label='Status {}'.format(column))

plt.legend(loc='upper left')
plt.title('Time Series of Hourly Other Error Statuses as Percentage of All Requests')
plt.xlabel('Date and Hour')
plt.ylabel('Percentage (%)')
plt.grid()
plt.show()
