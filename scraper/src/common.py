from datetime import datetime, timedelta
import logging
import os
import tempfile

import columns
import pandas as pd
from scipy.spatial.distance import cdist

logs_conf = {
    "level": logging.INFO,
    "format": "%(filename)s %(asctime)s %(levelname)s: %(message)s",
    "datefmt": "%H:%M:%S",
}
logging.basicConfig(**logs_conf)
logger = logging.getLogger('deps')
logger.setLevel(logging.INFO)

# data pipelines s3 paths
S3_DATA_BUCKET = "flats-data"
DATA_TYPES = ("sale", "rent")
S3_RAW_DATA_PATH = S3_DATA_BUCKET + "/{data_type}/raw"
LOCAL_ROOT = "/data"
S3_FINAL_PATH = S3_DATA_BUCKET + "/{data_type}/final"

# models s3 paths
S3_MODELS_BUCKET = "flats-models"
COORDS_MAP_MODELS_PATH = S3_MODELS_BUCKET + "/{data_type}/coords_encoding"
MODELS_PATH = S3_MODELS_BUCKET + "/{data_type}/models"

SCRAPING_TEMPDIR_PATH = "/tmp/{data_type}_dump.csv"

CLEANING_REQUIRED_COLUMNS = [
    columns.BALCONY,
    columns.BUILDING_HEIGHT,
    columns.BUILDING_MATERIAL,
    columns.BUILDING_TYPE,
    columns.BUILDING_YEAR,
    columns.CONVINIENCES,
    columns.DATE_ADDED,
    columns.DATE_REFRESHED,
    columns.DESC_LEN,
    columns.DIRECT,
    columns.EQUIPMENT,
    columns.FLAT_STATE,
    columns.FLOOR,
    columns.HEATING,
    columns.LAT,
    columns.LON,
    columns.MARKET_TYPE,
    columns.MEDIA,
    columns.OFFER_ID,
    columns.PRICE,
    columns.PRICE_M2,
    columns.PROMOTION_COUNTER,
    columns.ROOM_N,
    columns.SIZE,
    columns.TARAS,
    columns.TITLE,
    columns.URL,
    columns.VIEW_COUNT,
]

SALE_MODEL_INPUTS = [
    columns.CLUSTER_COORDS_FACTOR,
    columns.BUILDING_HEIGHT,
    columns.SIZE,
    columns.FLOOR,
    columns.BUILDING_YEAR,
    columns.VIEW_COUNT,
    columns.DESC_LEN,
    columns.FLOOR_N,
    columns.LAT,
    columns.LON,
]

RENT_MODEL_INPUTS = [
    columns.CLUSTER_COORDS_FACTOR,
    columns.SIZE,
    columns.BUILDING_HEIGHT,
    columns.BUILDING_YEAR,
    columns.LON,
    columns.LAT,
    columns.CLUSTER_MEAN_PRICE_M2,
    columns.CLUSTER_ID,
    columns.DESC_LEN,
    columns.FLOOR,
    columns.VIEW_COUNT,
    columns.CLUSTER_CENTER_DIST_KM,
]

log = logging.getLogger(__name__)


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

def get_process_from_date(data_type, last_date_of="raw"):
    if last_date_of == "final":
        from_date_str = os.environ.get("PROCESS_RAW_FILES_FROM")
    elif last_date_of == "raw":
        from_date_str = os.environ.get("SCRAPE_OFFERS_FROM")
    else:
        raise Exception(f"What is {last_date_of}? Cannot know which files to check")
    if not from_date_str:
        from_date = get_last_processing_date(data_type, last_date_of=last_date_of)
    else:
        from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
    return from_date


def get_last_processing_date(data_type, last_date_of):
    from s3_client import s3_client
    s3_client = s3_client()
    if last_date_of == "raw":
        paths = s3_client.list_s3_dir(S3_FINAL_PATH.format(data_type=data_type))
    elif last_date_of == "final":
        paths = s3_client.list_s3_dir(S3_RAW_DATA_PATH.format(data_type=data_type))

    if not paths:
        return datetime(2000, 1, 1)
    else:
        return select_newest_date(paths)


class InvalidExtensionException(Exception):
    pass

def get_current_dt():
    return datetime.now().strftime("%Y_%m_%dT%H_%M_%S")
