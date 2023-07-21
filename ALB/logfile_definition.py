#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 10:19:53 2023

@author: andy
"""

import pandas as pd
import gzip
import glob 

columns = []
def get_dataframe_and_columns():

    columns = [
        "proto",                                              # skip
        "timestamp",                    #          2023-03-04T00:40:00.673223Z               
        "elb",                                                # skip
        "client:port",                                        # 18.197.198.192:5310
        "target:port",                                        # skip
        "request_processing_time",                            # skip
        "latency",                      # float32 (?)         0.110
        "response_processing_time",                           # skip
        "status",                       # string(int16)         200
        "target_status",                                      # 200
        "r_bytes",                      # int16                  702
        "s_bytes",                      # int16                  537
        "request",                      # string (?)         POST https://pc-energy.encore-prod.enbw.cloud:443/service/Electricity HTTP/1.1
        "user_agent",                                         # skip
        "ssl_cipher",                                         # skip
        "ssl_protocol",                                       # skip
        "target_group_arn",                                   # skip
        "trace_id",                                           # skip
        "domain_name",                                        # skip
        "chosen_cert_arn",                                    # skip
        "matched_rule_priority",                              # skip
        "request_creation_time",        
        "actions_executed",                                   # skip
        "r_url",                        # string                 - 
        "error_reason",                                       # skip
        "target_ip",                                          # skip
        "target_status_description",                          # skip
        "code",                         # float64 (?)         NaN   jedenfalls am Anfang  
        "target_response_duration",                           # skip
        "target_health_description"                           # skip
    ]
    '''
    FILE = '/BigData/ELB/2018/12/11/647933830095_elasticloadbalancing_eu-central-1_app.awseb-AWSEB-18IHG6R3W1A7K.ac573d33cdf8d1a2_20181211T2355Z_35.157.194.235_tbc04iw9.log.gz'
    with gzip.open(FILE, 'rt') as file:
        df = pd.read_csv(file,  sep=' ', names=columns, quotechar='"', engine='python')
    '''
    '''
    files = glob.glob('/BigData/ELB/2018/12/2*/*.log.gz')
    dfs = []
    for file in files:
        with gzip.open(file, 'rt') as f:
            df = pd.read_csv(f, sep=' ', names=columns, quotechar='"', engine='python')
            dfs.append(df)
    final_df = pd.concat(dfs, ignore_index=True) 

    '''
    files = glob.glob('/BigData/ELB/2018/12/2*/*.log.gz')
    final_df = pd.DataFrame()
    for file in files:
        chunk_iterator = pd.read_csv(file, sep=' ', names=columns, quotechar='"', engine='python', chunksize=10000, compression='gzip')

        for chunk in chunk_iterator:
            final_df = pd.concat([final_df, chunk], ignore_index=True)
    
    final_df.drop(["proto", "elb", "target_status", "client:port", "target:port", "request_processing_time", "response_processing_time", "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn"], axis=1, inplace=True)
    final_df.drop(["trace_id",  "domain_name", "chosen_cert_arn",  "matched_rule_priority", "request_creation_time", "actions_executed"], axis=1, inplace=True)
    final_df.drop(["error_reason", "target_ip", "target_status_description","target_response_duration", "target_health_description"], axis=1, inplace=True)
    modified_columns = final_df.columns.tolist()
    return final_df, modified_columns