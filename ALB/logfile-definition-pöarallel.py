import pandas as pd
import glob
import gzip
from multiprocessing import Pool

# Function to process a single file
def process_file(file):
    df_list = []
    
    # Read and process the file in chunks of size 10,000
    chunk_iterator = pd.read_csv(file, sep=' ', names=columns, quotechar='"', engine='python', chunksize=10000, compression='gzip')

    for chunk in chunk_iterator:
        # You can do your processing here
        df_list.append(chunk)
    
    # Return the concatenated DataFrame
    return pd.concat(df_list, ignore_index=True)

# Use glob to get all the files for December 2018
files = glob.glob('/BigData/ELB/2018/12/*/*.log.gz')

# Create a Pool of worker processes
with Pool() as pool:
    # Use the pool to process the files
    result_list = pool.map(process_file, files)

# Concatenate the results into a single DataFrame
final_df = pd.concat(result_list, ignore_index=True)

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
