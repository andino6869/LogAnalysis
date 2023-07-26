#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import glob

# Define the months you want to load, as a list of year-month strings
months = ['2018-11', '2018-12', '2019-01', '2019-02', '2019-03', '2019-04', '2019-05', '2019-06', '2019-07', '2019-08', '2019-09', '2019-10', '2019-11', '2019-12',
                                '2020-01', '2020-02', '2020-03', '2020-04', '2020-05', '2020-06', '2020-07', '2020-08', '2020-09', '2020-10', '2020-11', '2020-12',
                                '2021-01', '2021-02', '2021-03', '2021-04', '2021-05', '2021-06', '2021-07', '2021-08', '2021-09', '2021-10', '2021-11', '2021-12',
                                '2022-01', '2022-02', '2022-03', '2022-04', '2022-05', '2022-06', '2022-07', '2022-08', '2022-09', '2022-10', '2022-11', '2022-12',
                                '2023-01', '2023-02', '2023-03', '2023-04', '2023-05', '2023-06']

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
