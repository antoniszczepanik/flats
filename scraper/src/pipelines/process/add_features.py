"""
Based on clean data and coords encoding map add new coords features.
"""
import logging

import pandas as pd
import numpy as np
from geopy.distance import great_circle
from shapely.geometry import MultiPoint
from shapely.ops import nearest_points

import columns as c
from common import (
    DATA_TYPES,
    LOCAL_ROOT,
    COORDS_MAP_MODELS_PATH,
    fs,
)
from pipelines.utils import add_point_col, unzip_point_to_lon_and_lat, read_df, save_df

log = logging.getLogger(__name__)


def features(data_type):
    log.info("Starting add features task...")
    coords_map = fs.read_newest_df(COORDS_MAP_MODELS_PATH, dtype=data_type)
    df = read_df(LOCAL_ROOT, keyword='clean', dtype=data_type)
    log.info(f'Shape of input dataframe: {df.shape}')
    log.info(f'Shape of center coords map: {coords_map.shape}')
    df = df.pipe(add_coords_features, coords_map=coords_map)
    save_df(df, LOCAL_ROOT, keyword='final', dtype=data_type)
    log.info(f"Finished adding features to {data_type} data.")


def add_coords_features(df, coords_map):
    log.info("Adding coordinates features ...")

    coords_map = add_point_col(coords_map)
    coords_map_multipoint = MultiPoint(coords_map['point'])
    df = add_point_col(df)
    log.info(f'Finding closest center for each flat ... (df.shape = {df.shape})')
    df['closest_center'] = df.apply(
        lambda row: nearest_points(row['point'], coords_map_multipoint)[1], axis=1)
    df = add_distance_col(df)
    # unzip points to two columns in map and df - merge on these two columns
    df = unzip_point_to_lon_and_lat(df, 'closest_center')
    center_colnames = ['unziped_lat_closest_center', 'unziped_lon_closest_center']

    log.info(f'Shape before merging map: {df.shape}')
    df = pd.merge(df,
                  coords_map,
                  left_on=center_colnames,
                  right_on=[c.LAT, c.LON],
                  how='left',
                  suffixes=('', 'duplicate')
                  )
    log.info(f'Shape after merging map: {df.shape}')

    for col in df.columns:
        if 'duplicate' in col or col in center_colnames + ['point']:
            df = df.drop(col, axis=1)

    df = df.pipe(add_coords_factor_col)

    cols_to_round = [c.CLUSTER_MEAN_PRICE_M2, c.CLUSTER_CENTER_DIST_KM]
    df.loc[:,cols_to_round] = df.loc[:,cols_to_round].round(2)

    log.info(f'Output df.shape: {df.shape}')
    return df


def add_distance_col(df):
    log.info('Calculating distance between flats and "centers" ...')
    df['lon1'] = df['closest_center'].map(lambda point: point.x)
    df['lat1'] = df['closest_center'].map(lambda point: point.y)

    df['lon2'] = df['point'].map(lambda point: point.x)
    df['lat2'] = df['point'].map(lambda point: point.y)

    df[c.CLUSTER_CENTER_DIST_KM] = get_haversine_dist(df['lon1'], df['lat1'], df['lon2'], df['lat2'])

    return df


def get_haversine_dist(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    All args must be of equal length.

    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return np.round(km, 3)

def add_coords_factor_col(df: pd.DataFrame) -> pd.DataFrame:
    df[c.CLUSTER_COORDS_FACTOR] = df[c.CLUSTER_MEAN_PRICE_M2] + (df[c.CLUSTER_MEAN_PRICE_M2] / (df[c.CLUSTER_CENTER_DIST_KM] + 1))
    return df
