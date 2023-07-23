import pandas as pd
import glob
import gzip 
import IPython.display
import matplotlib.pyplot as plt

# from IPython.display import display, HTML
# display(HTML('<style>.container {width: 100%} !i'))

columns = ["date", "time", "status", "latency", "r_bytes", "method", "uri"]

path_pattern = '/BigData/ELB/output/2019-12*.csv.gz'
files = glob.glob(path_pattern)
dfs = []

for file in files:
    df = pd.read_csv(file, compression='gzip', header=None, names=columns, skiprows=1, sep=',', quotechar='"', engine='python',
         parse_dates={'datetime': ['date', 'time']}, dtype={'status': 'int16', 'latency': 'float32', 'r_bytes': 'int16'})
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.set_index('datetime')
    dfs.append(df)
print("gzip load done ...")
df = pd.concat(dfs)
print("came together")

#mask = df['uri'].str.contains('login')
#indices = df[mask].index
#df.drop(indices, inplace=True)

#subpath = df['uri'].str.split('/').str[:2].str.join('/')
#print(subpath)
#df['subpath'] = subpath
print("Ready")

