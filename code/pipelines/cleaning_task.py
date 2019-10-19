#!/usr/bin/env python3

"""
Load concated data and output clean parquets (categorical variables mapped to
numerical format). Does not drop any rows.
"""
import os
import logging as log

import pandas as pd

from common import select_most_up_to_date_file, PATHS, get_current_dt, logs_conf
from cleaning import MorizonCleaner

log.basicConfig(**logs_conf)


def clean_morizon_data(in_path, out_path, spider_name):
    """
    Clean most current file from in_path directory
    for each spider. Save the output to out_path directory.
    """
    in_path_content = os.listdir(f"{in_path}")
    paths_to_clean = [f"{in_path}/{f}" for f in in_path_content]
    most_current_file = select_most_up_to_date_file(paths_to_clean)
    log.info(f"Found most up-to-date file: {most_current_file}")

    most_current_df = pd.read_parquet(most_current_file)
    log.info(f"Finished reading {most_current_file}")

    clean_df = MorizonCleaner(most_current_df).clean()
    current_dt = get_current_dt()

    out_filepath = f"{out_path}/{spider_name}_clean_{current_dt}.parquet"
    log.info(f"Writing cleaned dataframe to {out_filepath}")
    clean_df.to_parquet(out_filepath, index=False)
    log.info(f"Files writing finished succesfully.")


log.info("Starting data cleaning pipeline...")
for spider_type in PATHS:
    in_path = PATHS[spider_type]["concated"]
    out_path = PATHS[spider_type]["clean"]
    clean_morizon_data(in_path, out_path, spider_type)
    log.info(f"Successfully cleaned data for {spider_type}.")
log.info("Finished data cleaning pipeline.")
