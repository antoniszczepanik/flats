import logging as log

import pandas as pd
import numpy as np
from geopy.distance import great_circle
from sklearn.cluster import DBSCAN
from sklearn.metrics import pairwise_distances
from shapely.geometry import MultiPoint, Point
from shapely.ops import nearest_points
from scipy.spatial.distance import cdist

from common import CLEAN_DATA_PATH, logs_conf
import columns as c
from s3_client import s3_client


log.basicConfig(**logs_conf)
s3_client = s3_client()

PARAMS = {
    'sale': {
        'price_factor': 1/20_000,
        'max_dist': 10, # in km
        'min_samples': 6,
    },
    'rent': {
        'price_factor': 1/50,
        'max_dist': 10,
        'min_samples': 6,
    }
}


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


def get_coords_map(df):
    log.info(f'Shape of input df: {df.shape}')
    without_duplicates = remove_duplicates(df)
    log.info(f'Shape of df without lon/lat duplicates: {without_duplicates.shape}')

    center_coords_df = get_repr_points(without_duplicates)

    center_coords_df = add_point_col(center_coords_df)
    df = add_point_col(df)
    log.info('Converting_center_to multipoint')
    center_coords_multipoint = MultiPoint(center_coords_df['point'])

    log.info('Assigning closest values ...')
    df['coords_closest_point'] = df.apply(
        lambda row: nearest_points(row['point'], center_coords_multipoint)[1], axis=1)

    log.info('Clearing closest points map...')
    # coords encoding map is just center coords df with "mean" values assigned
    coords_encoding_map = (
        df.loc[:, ["coords_closest_point", 'price_m2__offer', 'price__offer']]
        .pipe(unzip_point_to_lon_and_lat, "coords_closest_point")
        .groupby([c.LON, c.LAT], as_index=False)
        .mean()
        .sort_values(by=c.PRICE_M2)
        .reset_index(drop=True)
        .rename(columns={
            c.PRICE_M2: 'cluster_mean_price_m2',
            c.PRICE: 'cluster_mean_price',
        })
    )
    coords_encoding_map['cluster_id'] = coords_encoding_map.index + 1
    return coords_encoding_map

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicates in lon/lat values. For each duplicate assingn a
    mean value in a given group
    """
    df = (df[['price_m2__offer', 'lon__offer', 'lat__offer']]
                          .groupby(['lon__offer', 'lat__offer'])
                          .mean()
                          .reset_index())
    return df


def add_point_col(df):
    """ Zips lon and lat columns to create a series of coords tuples. """
    df['point'] = [Point(x, y) for x, y in zip(df['lat__offer'], df['lon__offer'])]
    return df


def get_nearest(point, points: pd.Series):
    return nearest_points(point, points)


def get_repr_points(lon_lat_df):
    """
    Returns a dataframe with "center" points - only lon and lat columns. Based on:
    https://geoffboeing.com/2014/08/clustering-to-reduce-spatial-data-set-size/
    """
    KMS_PER_RADIAN = 6371.0088

    coords = lon_lat_df[['lat__offer', 'lon__offer']].to_numpy()

    #TODO: replace for rent
    epsilon = PARAMS['sale']['max_dist'] / KMS_PER_RADIAN

    # pairwise geographical distance between flat offers
    #TODO: for each cluster (groupped by title somehow) find clusters (i.e. 3 biggest cities and the rest)
    log.info('Calculating pairwise price and location distances ...')
    pairwise_dist_geo = pairwise_distances(lon_lat_df[['lat__offer', 'lon__offer']], metric='haversine')
    # pairwise differences between prices per m2
    pairwise_dist_price = pairwise_distances(lon_lat_df[['price_m2__offer']])

    # the idea is to use the price difference to modify the geographical difference
    # e.g. two flats with the same price have distance equal to their "raw" geographical distance
    # but two flats with a price difference of 10,000 have distance equal to 2 times the geographical distance
    pairwise_dist = pairwise_dist_geo * (1 + pairwise_dist_price * PARAMS['sale']['price_factor'])

    log.info("Starting DBScan alghorithm ...")
    db = DBSCAN(
        eps=epsilon, min_samples=PARAMS['sale']['min_samples'], metric="precomputed", n_jobs=-1,
    ).fit(pairwise_dist)
    log.info("Finished fitting ...")

    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
    centermost_points = []
    for cluster in clusters:
        if len(cluster) > 0:
            centermost_points.append(get_centroid(cluster) + [len(cluster)])
    #TODO: log info about clusters found, mean clusters, size of sample per CITY CLUSTER

    #TODO: concat smartly and log global info
    log.info(f"DBScan algoritm found {num_clusters} clusters.")
    repr_points_df = pd.DataFrame(centermost_points, columns=['lat__offer', 'lon__offer', 'cluster_size'])
    log.info(f"Mean cluster size: {repr_points_df['cluster_size'].mean()}")
    log.info(f"Mode cluster size: {repr_points_df['cluster_size'].mode()[0]}")
    log.info(f"Min cluster size: {repr_points_df['cluster_size'].min()}")
    log.info(f"Max cluster size: {repr_points_df['cluster_size'].max()}")
    repr_points_df['cluster_size'].hist(bins=100)
    return repr_points_df


def get_centroid(cluster):
    """
    Get the most "center" point for a cluster according to DBscan.
    """
    multi_point_centr = MultiPoint(cluster).centroid
    return [multi_point_centr.x, multi_point_centr.y]


def plot_points_on_map(df):
    """ Assumes lat__offer, lon__offer and cluster_id columns are present """
    cluster_price_max = df['cluster_mean_price_m2'].max()

    cmap = plt.cm.get_cmap('bwr')
    colors = cmap(np.arange(cmap.N))
    m = folium.Map(location=[51.5, 19.3], zoom_start=6, prefer_canvas=True, tiles='Stamen Toner')
    for index, row in df.iterrows():
        color = 'rgb({})'.format(', '.join([str(int(j*256)) for j in mcolors.to_rgb(colors[int(row['cluster_mean_price_m2']/cluster_price_max*256)-1])]))
        folium.Circle(
            location=[row['lat__offer'], row['lon__offer']],
            radius=100,
            color=color,
            fill=True,
            fill_opacity=1,
            popup=f"Mean price/m2: {row['cluster_mean_price_m2'].round(2)}",
        ).add_to(m)
    display(m)
