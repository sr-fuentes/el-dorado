# Schedule cron job to run this script on a daily basis to compress and archive
# the csv trade files to gzip and parquet formats

import gzip
import os
import yaml
# import pyarrow.csv as pv
# import pyarrow.parquet as pq

# Read config settings
with open("../configuration/local.yaml") as file:
    try:
        config = yaml.safe_load(file)
    except yaml.YAMLError as exc:
        print(exc)
path_exchanges = "../ed"
exchanges = os.listdir(path_exchanges)
for exchange in exchanges:
    path_csv = path_exchanges + "/" + exchange
    if os.isdir(path_csv):
        csvs = os.listdir(path_csv)
    path_gzip = "todo"