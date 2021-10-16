# Schedule cron job to run this script on a daily basis to compress and archive
# the csv trade files to gzip and parquet formats

import gzip
import os
# import pyarrow.csv as pv
# import pyarrow.parquet as pq

path_exchanges = "../ed"
exchanges = os.listdir(path_exchanges)
for exchange in exchanges:
    path_csv = path_exchanges + "/" + exchange
    if os.isdir(path_csv):
        csvs = os.listdir(pa)
    path_gzip = 