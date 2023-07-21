import glob
import gzip
from multiprocessing import Pool

# Function to process a single file
def process_file(file):
    with gzip.open(file, 'rt') as f:
        return sum(1 for line in f)

# Use glob to get all the files for December 2018
files = glob.glob('/BigData/ELB/2018/*/*/*.log.gz')

# Create a Pool of worker processes
with Pool() as pool:
    # Use the pool to process the files
    line_counts = pool.map(process_file, files)

# Print the total number of lines
print(sum(line_counts))
