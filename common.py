from datetime import datetime
import logging as log

import boto3
from botocore.exceptions import ClientError
from scipy.spatial.distance import cdist

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

logs_conf = {
    "level": log.INFO,
    "format": "%(filename)-23s %(asctime)s %(levelname)s: %(message)s",
    "datefmt": "%H:%M:%S",
}

HOME_PATH = "/home/ubuntu"
LOG_PATH = f"{HOME_PATH}/flats/setup/logs"

# paths to save parquet files (s3, but local since using s3fs)
PATHS = {
    "sale": {
        "raw": f"{HOME_PATH}/morizon-data/morizon_sale/raw",
        "concated": f"{HOME_PATH}/morizon-data/morizon_sale/concated",
        "clean": f"{HOME_PATH}/morizon-data/morizon_sale/clean",
        "final": f"{HOME_PATH}/morizon-data/morizon_sale/final",
    },
    "rent": {
        "raw": f"{HOME_PATH}/morizon-data/morizon_rent/raw",
        "concated": f"{HOME_PATH}/morizon-data/morizon_rent/concated",
        "clean": f"{HOME_PATH}/morizon-data/morizon_rent/clean",
        "final": f"{HOME_PATH}/morizon-data/morizon_sale/final",
    },
}

# bucket to store coords encoding map
S3_MODELS_BUCKET = "flats-models"
S3_MODELS_CLEANING_MAP_DIR = "coords_encoding"

log.basicConfig(**logs_conf)


def select_most_up_to_date_file(file_paths):
    # select file with most current datetime in name
    if len(file_paths) == 0:
        return None
    dt_strings = []

    for path in file_paths:
        date_numbers = "".join([x for x in path if x.isdigit()])
        # assure only files with datetimes are considered
        if len(date_numbers) == 14:
            dt_strings.append(date_numbers)

    datetimes = [datetime.strptime(x, "%Y%m%d%H%M%S") for x in dt_strings]
    max_pos = datetimes.index(max(datetimes))

    return file_paths[max_pos]


def select_most_up_to_date_date(file_paths):
    # select date from file with most current datetime in name
    if len(file_paths) == 0:
        return None
    dt_strings = []

    for path in file_paths:
        date_numbers = "".join([x for x in path if x.isdigit()])
        # assure only files with datetimes are considered
        if len(date_numbers) == 14:
            dt_strings.append(date_numbers)

    datetimes = [datetime.strptime(x, "%Y%m%d%H%M%S") for x in dt_strings]
    return max(datetimes)


def get_current_dt():
    return datetime.now().strftime("%Y_%m_%dT%H_%M_%S")


def upload_file_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client("s3")
    log.info(f"Sending {object_name} to {bucket} bucket...")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        log.error(e)
        return False
    return True


def download_file_from_s3(file_name, bucket, object_name=None):
    """Download a file to an S3 bucket
    :param bucket: Bucket to download from
    :param object_name: S3 object name. If not specified then file_name is used
    :param file_name: Path to download a file
    :return: True if file was downloaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Download the file
    s3_client = boto3.client("s3")
    log.info(f"Downloading {object_name} ...")
    try:
        response = s3_client.download_file(bucket, object_name, file_name)
    except ClientError as e:
        log.error(e)
        return False
    return True


def list_s3_dir(bucket, path):
    """List given path in S3 bucket. Returns a list of filenames"""
    s3_client = boto3.client("s3")
    return s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
