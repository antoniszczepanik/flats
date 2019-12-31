#!/usr/bin/env python3

"""
Get most representative coordinates out of a dataframe and store them in S3.
Creates a dataframe of lat/lon values with mean prices which enables encoding
coordinates to categorical and interesting feature engineering at later steps.
"""
import pandas as pd
import numpy as np
import logging as log
import sys
import tempfile

from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint

import columns
from common import (
    DATA_TYPES,
    CLEAN_DATA_PATH,
    COORDS_MAP_MODELS_PATH,
    logs_conf,
)
from pipelines.utils import (
    closest_point,
    unzip_coord_series_to_lon_and_lat,
    add_zipped_coords_column,
)
from s3_client import s3_client

# max distance (in km) between coordinates to get "clustered"
EPSILON = 3
# min samples per cluster
MIN_SAMPLES = 1
KMS_PER_RADIAN = 6371.0088

log.basicConfig(**logs_conf)

s3_client = s3_client()

def coords_map_task(data_type):
    log.info(f"Starting coords encoding map task for {data_type} data.")
    newest_df = s3_client.read_newest_df_from_s3(CLEAN_DATA_PATH, dtype=data_type)
    cols = newest_df.columns
    if columns.LON not in cols or columns.LAT not in cols:
        log.warning("Missing coordinates. Skipping.")
        return None

    coords_map = get_coords_map(newest_df, data_type)

    s3_client.upload_df_to_s3_with_timestamp(coords_map,
                                             s3_path=COORDS_MAP_MODELS_PATH,
                                             keyword='coords_map',
                                             dtype=data_type,
                                             )
    log.info(f"Finished coords encoding map task for {data_type} data.")


def get_coords_map(df, data_type):
    # remove "artificial" duplicates
    df_unduped = df.drop_duplicates(subset=[columns.LON, columns.LAT], keep="last")
    repr_coords_df = get_repr_points(df_unduped)
    coords_tuple_colname = "coords_tuple"
    repr_coords_df = add_zipped_coords_column(repr_coords_df, coords_tuple_colname)
    df = add_zipped_coords_column(df, coords_tuple_colname)
    # assign a closest point
    df["coords_closest_tuple"] = [
        closest_point(x, list(repr_coords_df[coords_tuple_colname]))
        for x in df[coords_tuple_colname]
    ]
    coords_encoding_map = (
        df.loc[:, ["coords_closest_tuple", columns.PRICE_M2, columns.PRICE]]
        .groupby("coords_closest_tuple", as_index=False)
        .mean()
        .sort_values(by=columns.PRICE_M2)
        .reset_index(drop=True)
        .rename(columns={
            columns.PRICE_M2: columns.CLUSTER_MEAN_PRICE_M2,
            columns.PRICE: columns.CLUSTER_MEAN_PRICE,
        })
        .pipe(unzip_coord_series_to_lon_and_lat, "coords_closest_tuple")
    )
    coords_encoding_map[columns.CLUSTER_ID] = coords_encoding_map.index + 1
    return coords_encoding_map


def get_repr_points(lon_lat_df):
    """
    Get's lon's and lat's representative for a dataframe with lon and lat
    values. For details see:
    https://geoffboeing.com/2014/08/clustering-to-reduce-spatial-data-set-size/
    """
    coords = lon_lat_df[[columns.LAT, columns.LON]].to_numpy()

    epsilon = EPSILON / KMS_PER_RADIAN

    log.info("Starting DBScan alghorithm ...")
    db = DBSCAN(
        eps=epsilon, min_samples=MIN_SAMPLES, algorithm="ball_tree", metric="haversine"
    ).fit(np.radians(coords))

    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
    centermost_points = list(clusters.map(get_centermost_point))
    log.info(f"DBScan algoritm found {num_clusters} clusters.")

    return pd.DataFrame(centermost_points, columns=[columns.LAT, columns.LON])


def get_centermost_point(cluster):
    """
    Get the most "center" point for a cluster according to DBscan.
    """
    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
    centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
    return tuple(centermost_point)
