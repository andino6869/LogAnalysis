'''
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

print(final_df)import pandas as pd
'''

import gzip
import glob 
from multiprocessing import Pool

def load_single_file(args):
    file, columns, idx = args
    if idx % 10 == 0:  # if the index is a multiple of 10
        print(f"Processing file number {idx + 1}: {file}")
    
    df = pd.DataFrame()
    chunk_iterator = pd.read_csv(file, sep=' ', names=columns, quotechar='"', engine='python', chunksize=10000, compression='gzip')
    
    for chunk in chunk_iterator:
        df = pd.concat([df, chunk], ignore_index=True)
    
    final_df = pd.concat(result_list, ignore_index=True)
    return final_df
    
def load_data_parallel(columns):
    print("Loading data...")
    files = glob.glob('/BigData/ELB/2018/12/2*/*.log.gz')
    
    with Pool() as p:
        dfs = p.starmap(load_single_file, [((file, columns, idx),) for idx, file in enumerate(files)])
    
    df = pd.concat(dfs, ignore_index=True)
    
    df.drop(["proto", "elb", "target_status", "client:port", "target:port", "request_processing_time", "response_processing_time", "user_agent", "ssl_cipher", "ssl_protocol", "target_group_arn"], axis=1, inplace=True)
    df.drop(["trace_id",  "domain_name", "chosen_cert_arn",  "matched_rule_priority", "request_creation_time", "actions_executed"], axis=1, inplace=True)
    df.drop(["error_reason", "target_ip", "target_status_description","target_response_duration", "target_health_description"], axis=1, inplace=True)
    
    print("Data processing complete!")
    modified_columns = final_df.columns.tolist()
    return modified_columns
    # return df, df.columns.tolist()
