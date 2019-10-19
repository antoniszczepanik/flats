#!/usr/bin/env python3

"""
Based on clean data and coords encoding map add new coords features.
"""
import logging as log

import pandas as pd
from geopy.distance import great_circle

from common import (
    select_most_up_to_date_file,
    logs_conf,
    PATHS,
    S3_MODELS_BUCKET,
    S3_MODELS_CLEANING_MAP_DIR,
)

log.basicConfig(**logs_conf)


def read_newest_cleaning_map(spider_type=None):
    with tempfile.TemporaryDirectory() as tmpdir:
        current_dt = get_current_dt()
        filepath = f"{tmpdir}/{current_dt}.parquet"
        coords_encoding_map.to_parquet(filepath)
        response = download_file_from_to_s3(
            filepath,
            S3_MODELS_BUCKET,
            f"{offer_type}/{S3_MODELS_CLEANING_MAP_DIR}/{current_dt}.parquet",
        )

    pass


def add_coords_features():
    pass


def get_distance(coords_df):
    """
    Calculate distace between two coords tuples.
    Takes dataframe with coords tuples as an argument.
    """
    distances = []
    for _, row in coords_df.iterrows():
        distances.append(
            great_circle(row[coords_df.columns[0]], row[coords_df.columns[1]]).km
        )
    return [round(dist, 3) for dist in distances]


def engineer_new_features(in_path, out_path, spider_name):
    """
    Add new columns to  most current file from in_path directory
    for each spider. Save output to out_path directory.
    """
    in_path_content = os.listdir(f"{in_path}")
    paths_to_add_features = [f"{in_path}/{f}" for f in in_path_content]
    most_current_file = select_most_up_to_date_file(paths_to_clean)
    log.info(f"Found most up-to-date file: {most_current_file}")

    most_current_df = pd.read_parquet(most_current_file)
    log.info(f"Finished reading {most_current_file}")

    coords_map = read_newest_cleaning_map(spider_type=spider_name)

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
