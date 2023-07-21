#!/usr/bin/env python3

import pandas as pd
import IPython.display
from IPython.display import display, HTML
from prepare9 import get_dataframe_and_columns

df, modified_columns = get_dataframe_and_columns()
print(modified_columns)

df['latency'] = df['request_processing_time'] + df['backend_processing_time'] + df['response_processing_time']
df['latency'] = df['latency'].clip(upper=255)
df['latency'] = df['latency'].astype('float16')
df['elb_status_code'] = df['elb_status_code'].astype('uint16')
df['received_bytes'] = df['received_bytes'].astype('uint32')
df['sent_bytes'] = df['sent_bytes'].astype('uint32')

split_fields = df['request'].str.split(' ', expand=True)

df['method'] = split_fields[0]
df['url'] = split_fields[1]
df['protocol'] = split_fields[2]
df.drop('request', axis=1, inplace=True)

df['url'] = df['url'].astype(str)
df['subpath'] = df['url'].str.split('443/').str[1].str.split('/').str[:2].str.join('/')

df = df.reindex(columns=['timestamp', 'latency', 'elb_status_code', 'received_bytes', 'sent_bytes', 'subpath', 'method', 'url', 'protocol'])
header_style = [{'selector': 'th', 'props': [('text-align', 'center')]}]

display_columns = ['timestamp', 'latency', 'elb_status_code', 'subpath', 'method']
display_df = df[display_columns]
print(display_df)

