#!/usr/bin/env python3

import pandas as pd
import numpy as np
import logging as log
import sys

from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint
from scipy.spatial.distance import cdist

# max distance (in km) between coordinates to get "clustered"
EPSILON = 1
# min samples per cluster
MIN_SAMPLES = 1

def get_repr_points(lon_lat_df):
    """
    Get's lon's and lat's representative for a dataframe with lon and lat values.
    For details see: https://geoffboeing.com/2014/08/clustering-to-reduce-spatial-data-set-size/
    """
    try:
        coords = df[['lat', 'lon']].to_numpy()
    except Exception as e:
        log.error('Failed to access lat and lon columns.')
        raise e

    KMS_PER_RADIAN = 6371.0088
    epsilon = EPSILON / kms_per_radian
    db = DBSCAN(eps=epsilon,
                min_samples=MIN_SAMPLES,
                algorithm='ball_tree',
                metric='haversine').fit(np.radians(coords))
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
    centermost_points = list(clusters.map(get_centermost_point))
    log.info(f'Found {num_clusters} clusters')
    return centermost_points

def get_centermost_point(cluster):
    """
    Get the most "center" point for a cluster according to DBscan.
    """
    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
    centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
    return tuple(centermost_point)

def send_repr_coords_to_s3(repr_coords):
    #TODO: Create func to send repr_coords_to_s3
    print(repr_coords)
    return None

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print('You should specify source data_frame as an argument.')
    else:

        path = sys.argv[1]
        try:
            df = pd.read_parquet(path)
        except OSError as e:
            print(f'Did not find "{path}" parquet file. Aborting.')
            raise e

        repr_coords = get_repr_points(df)
        send_repr_coords_to_s3(repr_coords)


