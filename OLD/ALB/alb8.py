#!/usr/bin/env python3

import pandas as pd
import IPython.display
from IPython.display import display, HTML
from prepare8 import get_dataframe_and_columns

df, modified_columns = get_dataframe_and_columns()

df = df.compute()  # compute the Dask DataFrame to get a pandas DataFrame

print(modified_columns)

df['latency'] = df['latency'].clip(upper=255)
df['latency'] = df['latency'].astype('float16')
df['status'] = df['status'].astype('uint16')
df['r_bytes'] = df['r_bytes'].astype('uint32')
df['s_bytes'] = df['s_bytes'].astype('uint32')

split_fields = df['request'].str.split(' ', expand=True)
df['method'] = split_fields[0]
df['url'] = split_fields[1]
df['protocol'] = split_fields[2]

df.drop('request', axis=1, inplace=True)

df['url'] = df['url'].astype(str)
df['subpath'] = df['url'].str.split('443/').str[1].str.split('/').str[:2].str.join('/')

df = df.reindex(columns=['timestamp', 'latency', 'status', 'r_bytes', 's_bytes', 'subpath', 'method', 'url', 'protocol', 'r_url', 'code'])

header_style = [{'selector': 'th', 'props': [('text-align', 'center')]}]

max_r_bytes = df['r_bytes'].max()
max_s_bytes = df['s_bytes'].max()
latency_median = df['latency'].median()
latency_95th_percentile = df['latency'].quantile(0.95)

subpath_avg_latency = df.groupby('subpath')['latency'].mean()
sorted_subpaths = subpath_avg_latency.sort_values(ascending=False)
top_10_subpaths = sorted_subpaths.head(10)

# Define the list of columns you want to display
display_columns = ['timestamp', 'latency', 'status', 'subpath', 'method']

# Index your DataFrame with this list to display only these columns
display_df = df[display_columns]
print(display_df)
