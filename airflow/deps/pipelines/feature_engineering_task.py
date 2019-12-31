"""
Based on clean data and coords encoding map add new coords features.
"""
import logging as log

import pandas as pd
from geopy.distance import great_circle

import columns
from common import (
    DATA_TYPES,
    CLEAN_DATA_PATH,
    FINAL_DATA_PATH,
    COORDS_MAP_MODELS_PATH,
    logs_conf,
)
from pipelines.utils import add_zipped_coords_column, unzip_coord_series_to_lon_and_lat, closest_point
from s3_client import s3_client

log.basicConfig(**logs_conf)

s3_client = s3_client()

def feature_engineering_task(data_type):
    log.info("Starting feature engineering task...")
    add_features(data_type)
    log.info(f"Finished adding features to {data_type} data.")

def add_features(data_type):
    coords_encoding_map = s3_client.read_newest_df_from_s3(COORDS_MAP_MODELS_PATH, dtype=data_type)
    df = s3_client.read_newest_df_from_s3(CLEAN_DATA_PATH, dtype=data_type)

    df = df.pipe(add_coords_features, coords_encoding_map=coords_encoding_map)
    # round all values in df to 2 decimal places
    round_cols = [columns.CLUSTER_MEAN_PRICE_M2, columns.CLUSTER_CENTER_DIST_KM]
    df[round_cols] = df[round_cols].round(2)

    s3_client.upload_df_to_s3_with_timestamp(df,
                                             s3_path=FINAL_DATA_PATH,
                                             keyword='final',
                                             dtype=data_type,
                                             )


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
                 distance_colname=columns.CLUSTER_CENTER_DIST_KM)

    # drop duplicate and unnecessary columns
    for col in df.columns:
        if 'duplicate' in col or col in [zipped_coords_col, closest_zipped_coords_col]:
            df = df.drop(col, axis=1)

    df = df.pipe(add_coords_factor_col)

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

def add_coords_factor_col(df: pd.DataFrame) -> pd.DataFrame:
    df[columns.CLUSTER_COORDS_FACTOR] = df[columns.CLUSTER_MEAN_PRICE_M2] + (df[columns.CLUSTER_MEAN_PRICE_M2] / (df[columns.CLUSTER_CENTER_DIST_KM] + 1))
    return df
