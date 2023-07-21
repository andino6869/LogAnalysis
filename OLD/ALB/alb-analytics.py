#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 26 16:07:16 2023

@author: andy
"""

import pandas as pd

# Define the column headers for the ALB access logs DataFrame
columns = [
    "proto",                        # https
    "timestamp",                    # 2023-03-04T00:40:00.673223Z               
    "elb",                          # skip
    "client:port",                  # 18.197.198.192:5310
    "target:port",                  # skip
    "request_processing_time",      # skip
    "target_processing_time",       # 0.110
    "response_processing_time",     # skip
    "elb_status_code",              # 200
    "target_status_code",           # 200
    "received_bytes",               # 702
    "sent_bytes",                   # 537
zdf.de    "request",                      # POST https://pc-energy.encore-prod.enbw.cloud:443/service/Electricity HTTP/1.1
    "user_agent",                   # skip
    "ssl_cipher",                   # skip
    "ssl_protocol",                 # skip
    "target_group_arn",             # skip
    "trace_id",                     # skip
    "domain_name",                  # skip
    "chosen_cert_arn",              # skip
    "matched_rule_priority",        # skip
    "request_creation_time",        
    "actions_executed",             # skip
    "redirect_url", 
    "error_reason",                 # skip
    "target_ip",                    # skip
    "target_status_description",    # skip
    "target_response_code",
    "target_response_duration",     # skip
    "target_health_description"     # skip
]

# Create an empty DataFrame with the defined column headers
# df = pd.DataFrame(columns=columns
df = pd.read_csv('/home/andy/Python/Specials/ALB/alb', sep=' ', names=columns, quotechar='"', engine='python')
print(df)