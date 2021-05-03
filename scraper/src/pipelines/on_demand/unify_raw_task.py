import logging

import pandas as pd

import columns
from common import RAW_DATA_PATH, fs

log = logging.getLogger(__name__)


# Columns that are required in raw files (in this order as well)
RAW_COLUMNS = [
    columns.TITLE,
    columns.FLOOR,
    columns.BUILDING_HEIGHT,
    columns.OFFER_ID,
    columns.BUILDING_YEAR,
    columns.DATE_ADDED,
    columns.DATE_REFRESHED,
    columns.BUILDING_TYPE,
    columns.BUILDING_MATERIAL,
    columns.MARKET_TYPE,
    columns.FLAT_STATE,
    columns.BALCONY,
    columns.TARAS,
    columns.PRICE,
    columns.PRICE_M2,
    columns.SIZE,
    columns.ROOM_N,
    columns.LAT,
    columns.LON,
    columns.URL,
    columns.DIRECT,
    columns.DESC_LEN,
    columns.VIEW_COUNT,
    columns.PROMOTION_COUNTER,
    columns.DESC,
    columns.IMAGE_LINK,
    columns.HEATING,
    columns.CONVINIENCES,
    columns.MEDIA,
    columns.EQUIPMENT,
    columns.WATER,
]

def unify_raw_data_task(data_type):
    log.info(f"Starting raw files unification for {data_type} data.")
    raw_paths = fs.list_dir(RAW_DATA_PATH.format(data_type=data_type))
    if len(raw_paths) == 0:
        log.info("No files to unify. Skipping")
        return None

    for i, f in enumerate(raw_paths):
        log.info(f"Processing file {i+1}/{len(raw_paths)}")
        unify_and_reupload(f)

    log.info(f"Finished unifying raw files for {data_type} data.")

def unify_and_reupload(raw_file_path):
    """
    Check if file structure is as expected. If not convert it to
    consistent format (eg. all necessary columns) and reupload.
    """
    raw_df = fs.read_df(raw_file_path)
    if not is_structure_correct(raw_df):
        log.info(f"Fixing file with invalid stucture: {raw_file_path} ...")
        fixed_raw_df = fix_raw_df(raw_df)
        print("Fixed file stats:")
        print(f"Shape: {fixed_raw_df.shape}")
        print("Nans %")
        print(fixed_raw_df.isna().sum()/len(fixed_raw_df))
        fs.save_df(fixed_raw_df, raw_file_path)

def is_structure_correct(raw_df):
    if len(raw_df.columns) != len(RAW_COLUMNS):
        log.info("Did not find all required columns ...")
        return False
    for c_raw, c_required in zip(raw_df.columns, RAW_COLUMNS):
        if c_raw != c_required:
            log.info("Columnn order does not match ...")
            return False
    log.info("Structure is correct.")
    return True

def fix_raw_df(raw_df):
    # if columns does not exist create it with empty values
    get_column = lambda df, col: df.get(col, pd.Series(index=df.index, name=col))
    for col in RAW_COLUMNS:
        raw_df[col] = get_column(raw_df, col)
    for col in raw_df.columns:
        if col not in RAW_COLUMNS:
            log.info(f"Dropping not required column: {col}")
            raw_df = raw_df.drop(col, axis=1)

    # sort columns
    raw_df = raw_df[RAW_COLUMNS]
    return raw_df
