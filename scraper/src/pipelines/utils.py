from datetime import datetime
import logging
import os

import pandas as pd
from shapely.geometry import MultiPoint, Point

import columns as c
from common import S3_FINAL_PATH, select_newest_date
from s3_client import s3_client

s3_client = s3_client()

log = logging.getLogger(__name__)

def get_process_from_date(data_type):
    from_date_str = os.environ.get("PROCESS_RAW_FILES_FROM")
    if not from_date_str:
        from_date = get_last_processing_date(data_type)
    else:
        from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
    return from_date

def get_last_processing_date(data_type):
    final_paths = s3_client.list_s3_dir(S3_FINAL_PATH.format(data_type=data_type))
    if not final_paths:
        return datetime(2000, 1, 1)
    else:
        return select_newest_date(final_paths)


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

def read_df(path, keyword, dtype, extension="csv"):
    path += f"/{dtype}_{keyword}.{extension}"
    if extension != 'csv':
        raise InvalidExtensionException
    log.info(f"Reading {keyword} {dtype} dataframe from {path}")
    return pd.read_csv(path)

def save_df(df, path, keyword, dtype, extension="csv"):
    path += f"/{dtype}_{keyword}.{extension}"
    extension = path.split(".")[-1]
    if extension != 'csv':
        raise InvalidExtensionException
    log.info(f"Saving {keyword} {dtype} dataframe to {path}")
    return df.to_csv(path, index=False)
