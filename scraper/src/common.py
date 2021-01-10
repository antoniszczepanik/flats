from datetime import datetime, timedelta
import logging
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
LOCAL_ROOT = "/data/"
S3_FINAL_PATH = "/{data_type}/final"

# models s3 paths
S3_MODELS_BUCKET = "flats-models"
COORDS_MAP_MODELS_PATH = S3_MODELS_BUCKET + "/{data_type}/coords_encoding"
MODELS_PATH = S3_MODELS_BUCKET + "/{data_type}/models"

SCRAPING_TEMPDIR_PATH = "/tmp/{data_type}_dump.csv"

# website paths
S3_WEBSITE_BUCKET = 'flats.antoniszczepanik.com'
SITE_DATA_LOCAL_PATH = '/tmp/top_offers.json'
SITE_DATA_S3_PATH = S3_WEBSITE_BUCKET + '/data.json'

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


def get_current_dt():
    return datetime.now().strftime("%Y_%m_%dT%H_%M_%S")


class InvalidExtensionException(Exception):
    pass
