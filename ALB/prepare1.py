import pandas as pd
import gzip

def get_dataframe_and_columns():
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
        df = pd.read_csv(file,  sep=' ', names=columns, quotechar='"', engine='python')

    drop_columns = [
        "proto", "elb", "target_status", "client:port", "target:port", "request_processing_time", 
        "response_processing_time", "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn", 
        "trace_id",  "domain_name", "chosen_cert_arn",  "matched_rule_priority", "request_creation_time", 
        "actions_executed", "error_reason", "target_ip", "target_status_description","target_response_duration", 
        "target_health_description"
    ]

    df.drop(drop_columns, axis=1, inplace=True)

    modified_columns = df.columns.tolist()

    return df, modified_columns


if __name__ == '__main__':
    df, modified_columns = get_dataframe_and_columns()
    print(f"Dataframe shape: {df.shape}")
    print(f"Columns: {modified_columns}")
