""""
Pipeline to concatinate and dedup all scraping outputs.
"""
import os
import logging as log

import pandas as pd

import pipelines.utils as utils
from common import (
    RAW_DATA_PATH,
    CONCATED_DATA_PATH,
    get_current_dt,
    logs_conf,
    list_s3_dir,
    read_newest_df_from_s3,
    upload_df_to_s3,
    read_df_from_s3,
    select_newest_date,
    get_date_from_filename,
)

log.basicConfig(**logs_conf)

# skip concating memory heavy columns
COLUMNS_TO_SKIP = ("desc", "image_link")


def concat_data_task(data_type):
    log.info("Starting data concatination task...")
    concat_csvs_to_parquet(data_type, columns_to_skip=COLUMNS_TO_SKIP)
    log.info(f"Finished concating files for {data_type}.")
    log.info("Finished concatination task.")


def concat_csvs_to_parquet(data_type, columns_to_skip):
    """
    Concat all raw csv files and save them as parquet skipping selected cols.
    """
    raw_paths = get_unconcated_raw_paths(data_type)
    if len(raw_paths) == 0:
        log.info("No files to concat. Skipping")
        return None
    raw_df = concat_dfs(raw_paths, columns_to_skip=columns_to_skip)

    previous_concated_df = read_newest_df_from_s3(
        CONCATED_DATA_PATH.format(data_type=data_type)
    )
    log.info(f"Previous concated df shape: {previous_concated_df.shape}")

    full_df = pd.concat([raw_df, previous_concated_df], sort=True)
    full_df = full_df.drop_duplicates("offer_id", keep="last")
    log.info(f"New concated df shape: {full_df.shape}")
    if full_df.shape == previous_concated_df.shape:
        log.info(
            "New concated file has the same number of records - not sending an update to s3"
        )
        return None
    current_dt = get_current_dt()
    target_s3_name = f"/{data_type}_concated_{current_dt}.parquet"
    target_s3_path = CONCATED_DATA_PATH.format(data_type=data_type) + target_s3_name
    upload_df_to_s3(full_df, target_s3_path)


def get_unconcated_raw_paths(data_type):
    """Returns paths of raw files newer than last concat date"""
    concated_paths = list_s3_dir(CONCATED_DATA_PATH.format(data_type=data_type))
    raw_paths = list_s3_dir(RAW_DATA_PATH.format(data_type=data_type))

    last_concat_date = select_newest_date(concated_paths)
    # skip datetimes with invalid format
    raw_paths = [r for r in raw_paths if get_date_from_filename(r) is not None]
    # skip raw files covered in previous concatination step
    raw_paths = [r for r in raw_paths if get_date_from_filename(r) > last_concat_date]
    return raw_paths


def concat_dfs(paths, columns_to_skip):
    """
    Concat all files and drop all duplicates.
    """
    dfs = []
    for s3_path in paths:
        df = read_df_from_s3(s3_path, columns_to_skip=columns_to_skip)
        dfs.append(df)
    log.info("Concatinating raw dfs ...")
    concatinated_df = pd.concat(dfs, sort=True).drop_duplicates(keep="last")
    return concatinated_df
