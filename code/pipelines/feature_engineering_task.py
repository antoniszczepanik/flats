#!/usr/bin/env python3

"""
Based on clean data and coords encoding map add new coords features.
"""
import logging as log

import pandas as pd
from geopy.distance import great_circle

from common import (
    read_newest_df_from_s3,
    logs_conf,
    DATA_TYPES,
    CLEAN_DATA_PATH,
    FINAL_DATA_PATH,
    COORDS_MAP_MODELS_PATH,
    upload_df_to_s3,
    get_current_dt,
)
from utils import add_zipped_coords_column, unzip_coord_series_to_lon_and_lat, closest_point

log.basicConfig(**logs_conf)

def feature_engineering_task():
    log.info("Starting feature engineering task...")
    for data_type in DATA_TYPES:
        add_features(data_type)
        log.info(f"Finished adding features to {data_type} data.")
    log.info("Finished feature engineering task.")

def add_features(data_type):
    coords_encoding_map = read_newest_df_from_s3(COORDS_MAP_MODELS_PATH.format(data_type=data_type))
    df = read_newest_df_from_s3(CLEAN_DATA_PATH.format(data_type=data_type))

    df = df.pipe(add_coords_features, coords_encoding_map=coords_encoding_map)
    # round all values in df to 2 decimal places
    df = df.round(2)

    current_dt = get_current_dt()
    target_s3_name = f"/{data_type}_final_{current_dt}.parquet"
    target_s3_path = FINAL_DATA_PATH.format(data_type=data_type) + target_s3_name
    upload_df_to_s3(df, target_s3_path)


def add_coords_features(df, coords_encoding_map):
    log.info("Adding coordinates features ...")
    zipped_coords_col = 'temp_zipped_coords'
    closest_zipped_coords_col = 'closest_zipped_coords'

    # add "zipped" coords
    coords_encoding_map = coords_encoding_map.pipe(add_zipped_coords_column,
                                                   new_col_name=zipped_coords_col)
    df = (df.pipe(add_zipped_coords_column,
                  new_col_name=zipped_coords_col)
            # add closest point from coords map
            .pipe(add_closest_zipped_coords_column,
                  coords_map=coords_encoding_map,
                  zipped_coords_col=zipped_coords_col,
                  new_col_name=closest_zipped_coords_col))

    # merge coords encoding map
    log.info("Merging coords encoding map ...")
    df = pd.merge(df,
                  coords_encoding_map,
                  left_on=closest_zipped_coords_col,
                  right_on=zipped_coords_col,
                  how='left',
                  suffixes=('', 'duplicate')
                  )
    # add distance column
    df = df.pipe(add_distance_col,
                 zipped_coords_1=zipped_coords_col,
                 zipped_coords_2=closest_zipped_coords_col,
                 distance_colname='coords_cluster_center_dist_km')

    # drop duplicate and unnecessary columns
    for col in df.columns:
        if 'duplicate' in col or col in [zipped_coords_col, closest_zipped_coords_col]:
            df = df.drop(col, axis=1)

    log.info("Successfully added coords features")
    return df

def add_closest_zipped_coords_column(df, coords_map, zipped_coords_col, new_col_name):
    log.info("Adding closest coords columns ...")
    df[new_col_name] = [
        closest_point(x, list(coords_map[zipped_coords_col])) for x in df[zipped_coords_col]]
    return df

def add_distance_col(df, zipped_coords_1, zipped_coords_2, distance_colname):
    """
    Calculate distace between two coords tuples.
    Takes dataframe with coords tuples as an argument.
    """
    log.info("Adding distance column...")
    distances = []
    for _, row in df.iterrows():
        distances.append(
            great_circle(row[zipped_coords_1], row[zipped_coords_2]).km
        )
    df[distance_colname] = [round(dist, 3) for dist in distances]
    return df


feature_engineering_task()
