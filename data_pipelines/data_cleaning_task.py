#!/usr/bin/env python3 
""""
Pipeline to clean concated data in parquet format.
It loads concated data and outputs clean parquets to clean/
directory. It converts each column to numerical, without
dropping any rows.
"""
from datetime import datetime
import os
import logging as log

import pandas as pd

from common import select_most_up_to_date_file, HOME_PATH, PATHS
from data_cleaning import MorizonCleaner

log.basicConfig(
    level=log.INFO, format="%(asctime)s %(message)s", datefmt="%m-%d-%Y %I:%M:%S"
)

def clean_morizon_data(in_path, out_path, spider_name):
    """
    Clean most current file from /concated directory
    for each spider. Save the output to /clean directory.
    """
    in_path_content = os.listdir(f'{HOME_PATH}{in_path}')
    paths_to_clean = [f'{HOME_PATH}{in_path}/{f}' for f in in_path_content]

    most_current_file = select_most_up_to_date_file(paths_to_clean)
    log.info(f"Found most up-to-date file: {most_current_file}")
    most_current_df = pd.read_parquet(most_current_file)
    clean_df = MorizonCleaner(most_current_df).clean()
    current_dt = datetime.now().strftime("%Y_%m_%dT%H_%M_%S")
    out_filepath = f'{HOME_PATH}{out_path}/{spider_name}_clean_{current_dt}.parquet'
    log.info(f"Saving cleaned dataframe to {out_filepath}")
    clean_df.to_parquet(out_filepath, index=False)


log.info("Starting data cleaning pipeline...")
for spider_type in PATHS:
    in_path =  PATHS[spider_type]['concated']
    out_path = PATHS[spider_type]['clean']
    clean_morizon_data(in_path, out_path, spider_type)
    log.info(f"Successfully cleaned data for {spider_type}.")
log.info("Finished data cleaning pipeline.")
