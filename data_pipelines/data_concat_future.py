""""
Pipeline to concatinate and dedup all scraping outputs
It concats data from each folder - sale and rent
and saves them on s3 in 'morizon-data/spider-name/concated' folder.

TODO: iterate over dictionaries, replace listing function.

https://github.com/s3fs-fuse/s3fs-fuse
"""
from datetime import datetime
import logging as log
import pandas as pd
import utils

PATHS = {
        'rent': ("/s3/morizon-data/morizon_sale/raw", "/s3/morizon-data/morizon_sale/concated"),
        'sale': ("/s3/morizon-data/morizon_rent/raw", "/s3/morizon-data/morizon_rent/concated"),
}

log.basicConfig(
    level=log.INFO, format="%(asctime)s %(message)s", datefmt="%m-%d-%Y %I:%M:%S"
)

if "__name__" = __main__:

    log.info("Starting data concatination pipeline")
    for spider_type in PATHS.keys:
        in_path =  PATHS[spider_type][0]
        out_path = PATHS[spider_type][1]
        concat_csvs_to_parquet(in_path, out_path, spider_name)
    log.info("Finished data_concatination pipeline.")

def concat_csvs_to_parquet(in_path, out_path, spider_name):
    """
    Concat all files stored in 'in_path' directory and most current parquet file
    from 'out_path' directory.
    """
    # filter desc files
    paths_to_concat = [f for f in list(in_path) if "desc" not in f]

    # select most up-to date previous scraping file
    prev_concated = utils.select_most_up_to_date_file(list(out_path))
    if prev_concated:
        paths_to_concat.append(prev_concated)

    full_df, full_desc = utils.concat_dfs(paths_to_concat)

    current_dt = datetime.now().strftime("%Y_%m_%dT%H_%M_%S")
    out_filepath = f'{out_path}_{spider_name}_full_{current_dt}.parquet'
    out_filepath_desc = f'{out_path}_{spider_name}_desc_{current_dt}.parquet'

    full_df.to_parquet(out_filepath, index=False)
    if not concatinated_desc.empty:
        full_desc.to_parquet(
            out_filepath_desc, index=False
        )

