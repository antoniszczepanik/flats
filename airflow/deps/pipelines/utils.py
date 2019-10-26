from datetime import datetime
import logging as log

import pandas as pd
from scipy.spatial.distance import cdist

from common import logs_conf

log.basicConfig(**logs_conf)


def update_txt_list(path_list, path):
    with open(path, "a+") as f:
        for item in path_list:
            f.write("%s\n" % item)


def read_txt_list(path):
    elements = set()
    with open(path, "r") as f:
        for line in f:
            elements.add(line.replace("\n", ""))
    return list(elements)


def name_from_path(filename):
    return filename.split("/")[-1]


def closest_point(point, points):
    """ Find closest point from a of list tuples with coordinates. """
    return points[cdist([point], points).argmin()]


def unzip_coord_series_to_lon_and_lat(df, zipped_colname):
    df["lat"] = df[zipped_colname].apply(lambda x: x[0])
    df["lon"] = df[zipped_colname].apply(lambda x: x[1])
    df = df.drop(zipped_colname, axis=1)
    return df


def add_zipped_coords_column(df, new_col_name):
    """ Zips lon and lat columns to create a series of coords tuples. """
    df[new_col_name] = [(x, y) for x, y in zip(df["lat"], df["lon"])]
    return df
