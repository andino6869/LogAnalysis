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
print(final_df)