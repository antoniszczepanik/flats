"""
Add coord-related features.
"""
import pandas as pd
import numpy as np

from geopy.distance import great_circle
from shapely.geometry import MultiPoint, Point
from shapely.ops import nearest_points

from s3_client import S3Client
import columns as c

s3_client = S3Client()
coords_map = s3_client.read_newest_df_from_s3(
    "flats-models/{data_type}/coords_encoding",
    dtype="sale"
)


def get_coords_factor(lon, lat):
    df = pd.DataFrame(
        {
            c.LON: [lon],
            c.LAT: [lat],
        }
    )
    df = df.pipe(add_coords_features, coords_map=coords_map)
    return df[c.CLUSTER_COORDS_FACTOR][0]


def add_coords_features(df, coords_map):
    coords_map = add_point_col(coords_map)
    coords_map_multipoint = MultiPoint(coords_map['point'])
    df = add_point_col(df)
    df['closest_center'] = df.apply(
        lambda row: nearest_points(row['point'], coords_map_multipoint)[1], axis=1)
    df = add_distance_col(df)
    # unzip points to two columns in map and df - merge on these two columns
    df = unzip_point_to_lon_and_lat(df, 'closest_center')
    center_colnames = ['unziped_lat_closest_center', 'unziped_lon_closest_center']

    df = pd.merge(df,
                  coords_map,
                  left_on=center_colnames,
                  right_on=[c.LAT, c.LON],
                  how='left',
                  suffixes=('', 'duplicate')
                  )

    for col in df.columns:
        if 'duplicate' in col or col in center_colnames + ['point']:
            df = df.drop(col, axis=1)

    df = df.pipe(add_coords_factor_col)

    cols_to_round = [c.CLUSTER_MEAN_PRICE_M2, c.CLUSTER_CENTER_DIST_KM]
    df.loc[:,cols_to_round] = df.loc[:,cols_to_round].round(2)

    return df


def add_distance_col(df):
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


def unzip_point_to_lon_and_lat(df: pd.DataFrame, col_to_unzip: str, drop: bool = True):
    df[f'unziped_lat_{col_to_unzip}'] = df[col_to_unzip].apply(lambda point: point.y)
    df[f'unziped_lon_{col_to_unzip}'] = df[col_to_unzip].apply(lambda point: point.x)
    if drop:
        df = df.drop(col_to_unzip, axis=1)
    return df


def add_point_col(df: pd.DataFrame) -> pd.DataFrame:
    """ Zips lon and lat columns to create a series of coords "points". """
    df['point'] = [Point(x, y) for x, y in zip(df[c.LON], df[c.LAT])]
    return df
