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

from common import (
    upload_file_to_s3,
    logs_conf,
    get_current_dt,
    PATHS,
    S3_MODELS_BUCKET,
    S3_MODELS_CLEANING_MAP_DIR,
)
from utils import (
    closest_point,
    unzip_coord_series_to_lon_and_lat,
    create_zipped_coords_series,
)

# max distance (in km) between coordinates to get "clustered"
EPSILON = 3
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
    coords = df[["lat", "lon"]].to_numpy()

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

    return pd.DataFrame(centermost_points, columns=["lat", "lon"])


def get_centermost_point(cluster):
    """
    Get the most "center" point for a cluster according to DBscan.
    """
    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
    centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
    return tuple(centermost_point)


def send_coords_encoding_map_to_s3(coords_encoding_map, offer_type):
    with tempfile.TemporaryDirectory() as tmpdir:
        current_dt = get_current_dt()
        filepath = f"{tmpdir}/{current_dt}.parquet"
        coords_encoding_map.to_parquet(filepath)
        s3_path = f"{offer_type}/{S3_MODELS_CLEANING_MAP_DIR}/{current_dt}.parquet"
        response = upload_file_to_s3(filepath, S3_MODELS_BUCKET, s3_path)
    if response:
        return True
    return False


if __name__ == "__main__":

    if len(sys.argv) != 3:
        log.error(
            "You should specify source dataframe path and offer type "
            "(sale or rent) as an argument."
        )
    else:
        offer_type = sys.argv[2]
        # verify if offer type is in supported types
        assert offer_type in PATHS.keys()

        path = sys.argv[1]
        try:
            df = pd.read_parquet(path)
        except OSError as e:
            log.error(f'Did not find "{path}" parquet file. Aborting.')
            raise e

        assert "lon" in df.columns
        assert "lat" in df.columns

        # remove "artificial" duplicates
        df_unduped = df.drop_duplicates(subset=['lon', 'lat'], keep='last')

        repr_coords_df = get_repr_points(df_unduped)

        df["coords_tuple"] = create_zipped_coords_series(df)
        repr_coords_df["coords_tuple"] = create_zipped_coords_series(repr_coords_df)

        # assign a closest point
        df["coords_closest_tuple"] = [
            closest_point(x, list(repr_coords_df["coords_tuple"]))
            for x in df["coords_tuple"]
        ]

        coords_encoding_map = (
            df.loc[:, ["coords_closest_tuple", "price_m2"]]
            .groupby("coords_closest_tuple", as_index=False)
            .mean()
            .sort_values(by="price_m2")
            .reset_index(drop=True)
            .rename(columns={"price_m2": "coords_mean_price_m2"})
            .pipe(unzip_coord_series_to_lon_and_lat, "coords_closest_tuple")
        )
        coords_encoding_map["coords_category"] = coords_encoding_map.index + 1

        response = send_coords_encoding_map_to_s3(coords_encoding_map, offer_type)
        if response:
            log.info("Succesfully uploaded file to S3.")
