from datetime import datetime
import logging

import pandas as pd
from shapely.geometry import MultiPoint, Point

import columns as c

log = logging.getLogger(__name__)


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


