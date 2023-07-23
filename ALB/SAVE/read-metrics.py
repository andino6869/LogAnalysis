#!/usr/bin/env python3

import os
import pandas as pd
import glob
import matplotlib.pyplot as plt

def load_from_csv(filename):
    return pd.read_csv(filename, index_col=0)

# Load all requests counts and status 500 percentages
all_requests_counts = load_from_csv('metrics/all_requests_counts.csv')
status_500_percentages = load_from_csv('metrics/status_500_percentages.csv')

# Plot all requests
all_requests_counts.plot(label='Requests')
plt.title('All Requests')
plt.ylabel('Number of requests')
plt.legend(loc="upper left")
plt.show()

# Plot percentage of 500 status codes
status_500_percentages.plot(label='Status 500')
plt.title('Percentage of 500 Status Codes')
plt.ylabel('Percentage (%)')
plt.legend(loc="upper left")
plt.show()

# Get a list of all error status code metrics files
error_metrics_files = glob.glob('metrics/error_*_percentages.csv')

# Loop over metrics files and plot each one
for file in error_metrics_files:
    # Get status code from filename
    status_code = file.split('_')[1]

    # Load counts and percentages
    error_counts = load_from_csv(f'metrics/error_{status_code}_counts.csv')
    error_percentages = load_from_csv(f'metrics/error_{status_code}_percentages.csv')

    # Plot counts
    error_counts.plot(label=f'Status {status_code} counts')
    plt.title(f'Error {status_code} Counts')
    plt.ylabel('Number of errors')
    plt.legend(loc="upper left")
    plt.show()

    # Plot percentages
    error_percentages.plot(label=f'Status {status_code} percentages')
    plt.title(f'Percentage of {status_code} Status Codes')
    plt.ylabel('Percentage (%)')
    plt.legend(loc="upper left")
    plt.show()
