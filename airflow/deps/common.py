from datetime import datetime
import logging as log
import tempfile

import pandas as pd
from scipy.spatial.distance import cdist

logs_conf = {
    "level": log.INFO,
    "format": "%(filename)-30s %(asctime)s %(levelname)s: %(message)s",
    "datefmt": "%H:%M:%S",
}

# data pipelines paths
S3_DATA_BUCKET = "flats-data"
DATA_TYPES = ("sale", "rent")
RAW_DATA_PATH = S3_DATA_BUCKET + "/{data_type}/raw"
CONCATED_DATA_PATH = S3_DATA_BUCKET + "/{data_type}/concated"
CLEAN_DATA_PATH = S3_DATA_BUCKET + "/{data_type}/clean"
FINAL_DATA_PATH = S3_DATA_BUCKET + "/{data_type}/final"

# models paths
S3_MODELS_BUCKET = "flats-models"
COORDS_MAP_MODELS_PATH = S3_MODELS_BUCKET + "/{data_type}/coords_encoding"

CLEANING_REQUIRED_COLUMNS = [
    "balcony",
    "building_height",
    "building_material",
    "building_type",
    "building_year",
    "conviniences",
    "date_added",
    "date_refreshed",
    "desc_len",
    "direct",
    "equipment",
    "flat_state",
    "floor",
    "heating",
    "lat",
    "lon",
    "market_type",
    "media",
    "offer_id",
    "price",
    "price_m2",
    "promotion_counter",
    "room_n",
    "size",
    "taras",
    "title",
    "url",
    "view_count",
]


log.basicConfig(**logs_conf)


def select_newest_date(file_paths):
    """ Select newest date from list of strings and return datetime object."""
    if len(file_paths) == 0:
        return None
    datetimes = []
    for path in file_paths:
        date = get_date_from_filename(path)
        # filter nans
        if date:
            datetimes.append(date)
    return max(datetimes)

def get_date_from_filename(filename):
    date_numbers = "".join([x for x in filename if x.isdigit()])
    # make sure this is a valid datetime format used accross project
    if len(date_numbers) != 14:
        log.warning(f"Not getting date from invalid file name: {filename}")
        return None
    return datetime.strptime(date_numbers, "%Y%m%d%H%M%S")


def get_current_dt():
    return datetime.now().strftime("%Y_%m_%dT%H_%M_%S")


class InvalidExtensionException(Exception):
    pass
