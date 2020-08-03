"""
Load concated data and output clean parquets (categorical variables mapped to
numerical format). Does not drop any rows.
"""
import os
import logging

import pandas as pd

from common import (
    CONCATED_DATA_PATH,
    CLEAN_DATA_PATH,
)
from pipelines.cleaning import MorizonCleaner
from s3_client import s3_client

log = logging.getLogger(__name__)

# number of rows to process at once
CHUNK_SIZE = 5000

s3_client = s3_client()

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
    df = s3_client.read_newest_df_from_s3(CONCATED_DATA_PATH, dtype=data_type)

    batches = [df[i:i+CHUNK_SIZE] for i in range(0, df.shape[0],CHUNK_SIZE)]
    cleaned_dfs = []
    for batch_n, batch in enumerate(batches):
        log.info(f"Processing batch number {batch_n} ...")
        clean_batch = MorizonCleaner(batch).clean()
        cleaned_dfs.append(clean_batch)
    cleaned_df = pd.concat(cleaned_dfs, sort=True)

    log.info(f'Before cleaning dataframe shape: {df.shape}')
    log.info(f'Cleaned dataframe shape: {cleaned_df.shape}')

    s3_client.upload_df_to_s3_with_timestamp(cleaned_df,
                                             s3_path=CLEAN_DATA_PATH,
                                             keyword='clean',
                                             dtype=data_type,
                                             )


