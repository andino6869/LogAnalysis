#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 10:15:26 2023

@author: andy
"""

import pandas as pd
import IPython.display

#from tabulate import tabulate
from IPython.display import display, HTML

# display(HTML('<style>.container {width: 100%} !i'))
# Define the column headers for the ALB access logs DataFrame

from prepare6 import read_dataframe,
df, modified_columns = read_dataframe()
# or
#df, modified_columns = get_dataframe_and_columns("/path/to/your/file.log.gz")

print(modified_columns)
# print(df)

'''
pd.set_option('display.width', 5000)
pd.set_option('max_colwidth', 200)
pd.set_option('max_colwidth', None)
pd.set_option('display.max_rows', 10)

pd.get_option("display.max_columns")
pd.get_option("display.max_rows")

with pd.option_context('display.max_colwidth', None):
    display(df)


# df.info()
# print(df)
'''

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
header_style = [

    {'selector': 'th', 'props': [('text-align', 'center')]}

]

'''
styled_df = df.head(10).style.set_properties(subset=['url', 'subpath'], **{'text-align': 'left'})
styled_df = styled_df.set_table_styles(header_style)
styled_df
#pd.set_option('display.width', 200)  # or any other large number; does not 
'''


# latency_stats = df['latency'].describe()
max_r_bytes = df['r_bytes'].max()
max_s_bytes = df['s_bytes'].max()
latency_median = df['latency'].median()
latency_95th_percentile = df['latency'].quantile(0.95)
#print("Max r_bytes:", max_r_bytes)
#print("Max s_bytes:", max_s_bytes)
# print(latency_stats)
# print(latency_95th_percentile)

subpath_avg_latency = df.groupby('subpath')['latency'].mean()
sorted_subpaths = subpath_avg_latency.sort_values(ascending=False)
top_10_subpaths = sorted_subpaths.head(10)

# Define the list of columns you want to display
display_columns = ['timestamp', 'latency', 'status', 'subpath', 'method']

# Index your DataFrame with this list to display only these columns
display_df = df[display_columns]
print(display_df)


print(top_10_subpaths)
'''

highest_latencies = df.nlargest(5, 'latency')

print(highest_latencies)
'''
