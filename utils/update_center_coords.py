#!/usr/bin/env python3

"""
Get most representative coordinates out of a dataframe and store them in S3.
Creates a dataframe of lat/lon values which enables encoding coordinates
to categorical variables later.
"""

import pandas as pd
import numpy as np
import logging as log
import sys
import tempfile

from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint
from scipy.spatial.distance import cdist

from common import upload_file_to_s3, logs_conf

# bucket to store repr points
S3_BUCKET = 'flats'
# max distance (in km) between coordinates to get "clustered"
EPSILON = 1
# min samples per cluster
MIN_SAMPLES = 1
KMS_PER_RADIAN = 6371.0088

log.basicConfig(**logs_conf)

def get_repr_points(lon_lat_df):
    """
    Get's lon's and lat's representative for a dataframe with lon and lat
    values. For details see:
    https://geoffboeing.com/2014/08/clustering-to-reduce-spatial-data-set-size/
    """
    try:
        coords = df[['lat', 'lon']].to_numpy()
    except Exception as e:
        log.error('Failed to access lat and lon columns.')
        raise e

    epsilon = EPSILON / KMS_PER_RADIAN

    log.info('Startind DBScan alhorithm ...')
    db = DBSCAN(eps=epsilon,
                min_samples=MIN_SAMPLES,
                algorithm='ball_tree',
                metric='haversine').fit(np.radians(coords))

    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
    centermost_points = list(clusters.map(get_centermost_point))
    log.info(f'DBScan algoritm found {num_clusters} clusters.')

    return centermost_points

def get_centermost_point(cluster):
    """
    Get the most "center" point for a cluster according to DBscan.
    """
    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
    centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
    return tuple(centermost_point)

def send_repr_coords_to_s3(repr_coords_df, offer_type):
    with tempfile.TemporaryDirectory() as tmpdir:
        current_dt = get_current_dt()
        repr_coords.to_parquet(f'{current_dt}.parquet')
        upload_file_to_s3(f'{offer_type}/central_coords/{current_dt}',
                          BUCKET_NAME)
    return None

if __name__ == "__main__":

    if len(sys.argv) != 3:
        log.error('You should specify source data_frame path and offer_type'
                  '(sale or rent) as an argument.')
    else:

        path = sys.argv[1]
        offer_type = sys.argv[2]
        try:
            df = pd.read_parquet(path)
        except OSError as e:
            log.error(f'Did not find "{path}" parquet file. Aborting.')
            raise e

        repr_coords_df = get_repr_points(df)
        send_repr_coords_to_s3(repr_coords_df, offer_type)


