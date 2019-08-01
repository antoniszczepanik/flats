""""
Pipeline to concatinate and dedup all scraping outputs
It concats data from each folder - sale and rent
and saves them on s3 in 'morizon-data/spider-name/concated' folder.

TODO: iterate over dictionaries, replace listing function.

https://github.com/s3fs-fuse/s3fs-fuse
"""
from datetime import datetime
import os
import logging as log

import pandas as pd

import utils

PATHS = {
        'rent': ("/morizon-data/morizon_sale/raw", "/morizon-data/morizon_sale/concated"),
        'sale': ("/morizon-data/morizon_rent/raw", "/morizon-data/morizon_rent/concated"),
}
HOME_PATH = '/home/ubuntu'

log.basicConfig(
    level=log.INFO, format="%(asctime)s %(message)s", datefmt="%m-%d-%Y %I:%M:%S"
)


def concat_csvs_to_parquet(in_path, out_path, spider_name):
    """
    Concat all files stored in 'in_path' directory and most current parquet file
    from 'out_path' directory. Save the output to 'out_path' directory.
    """
    # filter desc files
    in_path_content = os.listdir(f'{HOME_PATH}{in_path}')
    paths_to_concat = [f for f in in_path_content if "desc" not in f]
    paths_to_concat = [f'{HOME_PATH}{in_path}/{f}' for f in paths_to_concat]

    # select most up-to date previous scraping file
    out_path_content = os.listdir(f'{HOME_PATH}{out_path}')
    prev_concated = utils.select_most_up_to_date_file(out_path_content)
    if prev_concated:
        paths_to_concat.append(f'{HOME_PATH}{out_path}/{prev_concated}')

    full_df, full_desc = utils.concat_dfs(paths_to_concat)

    current_dt = datetime.now().strftime("%Y_%m_%dT%H_%M_%S")
    out_filepath = f'{HOME_PATH}{out_path}_{spider_name}_full_{current_dt}.parquet'
    out_filepath_desc = f'{HOME_PATH}{out_path}_{spider_name}_desc_{current_dt}.parquet'

    full_df.to_parquet(out_filepath, index=False)
    if not concatinated_desc.empty:
        full_desc.to_parquet(
            out_filepath_desc, index=False
        )

if __name__ == "__main__":

    log.info("Starting data concatination pipeline")
    for spider_type in PATHS:
        in_path =  PATHS[spider_type][0]
        out_path = PATHS[spider_type][1]
        concat_csvs_to_parquet(in_path, out_path, spider_type)
    log.info("Finished data_concatination pipeline.")
