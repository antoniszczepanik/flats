"""
Load concated data and output clean data (categorical variables mapped to
numerical format). Does not drop any rows.
"""
import datetime
import logging

import pandas as pd

import columns
from common import (
    RAW_DATA_PATH,
    LOCAL_ROOT,
    select_newest_date,
    get_process_from_date,
    fs,
)
from pipelines.process.cleaning_utils import MorizonCleaner
from pipelines.utils import save_df

log = logging.getLogger(__name__)

# number of rows to process at once
CHUNK_SIZE = 1000
# skip concating memory heavy columns
COLUMNS_TO_SKIP = (columns.DESC, columns.IMAGE_LINK)

def clean(data_type):
    log.info("Starting data cleaning pipeline...")
    clean_morizon_data(data_type)
    log.info(f"Successfully cleaned data for {data_type}.")
    log.info("Finished data cleaning pipeline.")


def clean_morizon_data(data_type):
    """
    Clean most current file from in_path directory
    for each spider. Save the output to out_path directory.
    """
    from_date = get_process_from_date(data_type, last_date_of="final")
    log.info(f'Will concat raw files newer than {from_date}')
    df = get_df_to_process(data_type, from_date)
    batches = [df[i:i+CHUNK_SIZE] for i in range(0, df.shape[0],CHUNK_SIZE)]
    cleaned_dfs = []
    for batch_n, batch in enumerate(batches):
        log.info(f"Processing batch number {batch_n} ...")
        clean_batch = MorizonCleaner(batch).clean()
        cleaned_dfs.append(clean_batch)
    cleaned_df = pd.concat(cleaned_dfs, sort=True)

    log.info(f'Before cleaning dataframe shape: {df.shape}')
    log.info(f'Cleaned dataframe shape: {cleaned_df.shape}')
    save_df(cleaned_df, LOCAL_ROOT, keyword='clean', dtype=data_type)




def get_df_to_process(data_type, from_date):
    raw_paths = fs.list_dir(RAW_DATA_PATH.format(data_type=data_type))
    raw_paths = [r for r in raw_paths if fs.get_date_from_filename(r) is not None]
    raw_paths = [r for r in raw_paths if fs.get_date_from_filename(r) > from_date]
    log.info(f'Found {len(raw_paths)} raw files newer than {from_date}')
    return concat_dfs(raw_paths)


def concat_dfs(paths):
    """
    Concat all files and drop all duplicates.
    """
    dfs = []
    for path in paths:
        df = fs.read_df(path, columns_to_skip=COLUMNS_TO_SKIP)
        dfs.append(df)
    concatinated_df = pd.concat(dfs, sort=True).drop_duplicates(keep="last")
    log.info("Successfully concatinated raw dfs.")
    log.info(f"shape: {concatinated_df.shape}")
    return concatinated_df
