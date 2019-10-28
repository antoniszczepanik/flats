"""
Load concated data and output clean parquets (categorical variables mapped to
numerical format). Does not drop any rows.
"""
import os
import logging as log

import pandas as pd

from common import (
    CONCATED_DATA_PATH,
    CLEAN_DATA_PATH,
    get_current_dt,
    logs_conf,
    read_newest_df_from_s3,
    upload_df_to_s3,
)
from pipelines.cleaning import MorizonCleaner

log.basicConfig(**logs_conf)


def cleaning_task(data_type):
    log.info("Starting data cleaning pipeline...")
    clean_morizon_data(data_type)
    log.info(f"Successfully cleaned data for {data_type}.")
    log.info("Finished data cleaning pipeline.")


def clean_morizon_data(data_type):
    """
    Clean most current file from in_path directory
    for each spider. Save the output to out_path directory.
    """
    df = read_newest_df_from_s3(CONCATED_DATA_PATH.format(data_type=data_type))
    clean_df = MorizonCleaner(df).clean()

    current_dt = get_current_dt()
    target_s3_name = f"/{data_type}_clean_{current_dt}.parquet"
    target_s3_path = CLEAN_DATA_PATH.format(data_type=data_type) + target_s3_name
    upload_df_to_s3(clean_df, target_s3_path)
