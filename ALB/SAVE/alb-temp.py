#!/home/andy/anaconda3/bin/python3

"""
Irgendein Kommentar
"""

import pandas as pd
#import gzip 
#import IPython.display

from IPython.display import display, HTML
display(HTML('<style>.container {width: 100%} !i'))

'''
columns = [
    "proto",
    "timestamp",                   
    "elb",                           
    "client:port",
    "target:port",
    "request_processing_time",
    "latency",
    "response_processing_time",                           
    "status",
    "target_status",
    "r_bytes",
    "s_bytes",    
    "request",    
    "user_agent",                           
    "ssl_cipher",                           
    "ssl_protocol",                           
    "target_group_arn",                           
    "trace_id",                           
    "domain_name",                           
    "chosen_cert_arn",                           
    "matched_rule_priority",                           
    "request_creation_time",     
    "actions_executed",                           
    "r_url",      
    "error_reason",      
    "target_ip",                    
    "target_status_description",
    "code",    
    "target_response_duration",    
    "target_health_description"    
]

FILE = '/BigData/ELB/2018/12/11/647933830095_elasticloadbalancing_eu-central-1_app.awseb-AWSEB-18IHG6R3W1A7K.ac573d33cdf8d1a2_20181211T2355Z_35.157.194.235_tbc04iw9.log.gz'
with gzip.open(FILE, 'rt') as file:
    df = pd.read_csv(FILE,  sep=' ', names=columns, quotechar='"', engine='python')    
  
df.drop(["proto", "elb", "target_status", "client:port", "target:port", "request_processing_time", "response_processing_time", "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn"], axis=1, inplace=True)  
df.drop(["trace_id",  "domain_name", "chosen_cert_arn",  "matched_rule_priority", "request_creation_time", "actions_executed"], axis=1, inplace=True)
df.drop(["error_reason", "target_ip", "target_status_description","target_response_duration", "target_health_description"], axis=1, inplace=True)
'''

from logfile_definition import columns 

pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', 100)

pd.set_option('display.max_rows', 10)

#df.head(10)
print(df)

pd.get_option("display.max_columns")
pd.get_option("display.max_rows")

df.info()

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
styled_df = df.head(10).style.set_properties(subset=['url', 'subpath'], **{'text-align': 'left'})
styled_df = styled_df.set_table_styles(header_style)
styled_df

max_r_bytes = df['r_bytes'].max()
max_s_bytes = df['s_bytes'].max()

print("Max r_bytes:", max_r_bytes)
print("Max s_bytes:", max_s_bytes)

latency_stats = df['latency'].describe()
print(latency_stats)

latency_median = df['latency'].median()
latency_95th_percentile = df['latency'].quantile(0.95)
print(latency_95th_percentile)

subpath_avg_latency = df.groupby('subpath')['latency'].mean()
sorted_subpaths = subpath_avg_latency.sort_values(ascending=False)
top_5_subpaths = sorted_subpaths.head(5)
print(top_5_subpaths)

highest_latencies = df.nlargest(5, 'latency')
print(highest_latencies)





