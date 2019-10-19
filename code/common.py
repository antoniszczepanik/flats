from datetime import datetime
import logging as log
import tempfile

import boto3
from botocore.exceptions import ClientError
import pandas as pd
from scipy.spatial.distance import cdist

logs_conf = {
    "level": log.INFO,
    "format": "%(filename)-30s %(asctime)s %(levelname)s: %(message)s",
    "datefmt": "%H:%M:%S",
}

# data pipelines paths
S3_DATA_BUCKET = 'flats-data'
SALE_PATH_ROOT = 'sale'
RENT_PATH_ROOT = 'rent'
RAW_DATA_PATH = S3_DATA_BUCKET + '/{data_type}/raw'
CONCATED_DATA_PATH = S3_DATA_BUCKET + '/{data_type}/concated'
CLEAN_DATA_PATH = S3_DATA_BUCKET + '/{data_type}/clean'
FINAL_DATA_PATH = S3_DATA_BUCKET + '/{data_type}/final'

# models paths
S3_MODELS_BUCKET = 'flats-models'
S3_MODELS_CLEANING_MAP_DIR = "coords_encoding"

CLEANING_REQUIRED_COLUMNS = [
    "balcony",
    "building_height",
    "building_material",
    "building_type",
    "building_year",
    "conviniences", "date_added",
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


def select_newest_file(file_paths):
    """ Select string with most current datetime in name. """
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


def select_newest_date(file_paths):
    """ Select newest date from list of strings and return datetime object."""
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


def upload_file_to_s3(file_name, s3_path):
    # If S3 object_name was not specified, use file_name
    bucket, path = split_bucket_path(s3_path)
    # Upload the file
    s3_client = boto3.client("s3")
    log.info(f"Sending {path} to {bucket} bucket...")
    try:
        response = s3_client.upload_file(file_name, bucket, path)
    except ClientError as e:
        log.error(e)
        return False
    log.info(f"Successfully uploaded {path} to {bucket} bucket.")
    return True


def download_file_from_s3(filename, s3_path):
    bucket, path = split_bucket_path(s3_path)
    # Download the file
    s3_client = boto3.client("s3")
    log.info(f"Downloading {path} from {bucket} bucket ...")
    try:
        response = s3_client.download_file(bucket, path, filename)
    except ClientError as e:
        log.error(e)
        return False
    log.info(f"Successfully downloaded {path} from {bucket} bucket.")
    return True


def list_s3_dir(s3_dir):
    """Recursively list given dir in S3 bucket. Returns a list of filenames"""
    bucket, path = split_bucket_path(s3_dir)
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
    file_list = [f"{bucket}/{f['Key']}" for f in response['Contents']]
    # do not list directories
    file_list = [f for f in file_list if f[-1] != '/']
    return file_list


def split_bucket_path(s3_path):
    splitted = s3_path.split('/')
    bucket = splitted[0]
    path = '/'.join(splitted[1:])
    return bucket, path

def get_filename(s3_path):
    return s3_path.split('/')[-1]

def upload_df_to_s3(df, s3_path):
    filename = get_filename(s3_path)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = f'{tmpdir}/{filename}'
        extension = tmp_path.split('.')[-1]
        if extension == 'csv':
            df.to_csv(tmp_path, index=False)
        elif extension == 'parquet':
            df.to_parquet(tmp_path)
        else:
            log.error(f'{extension} extension is not supported.')
            raise InvalidExtensionException
        upload_file_to_s3(tmp_path, s3_path)


def read_df_from_s3(s3_path):
    filename = get_filename(s3_path)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = f'{tmpdir}/{filename}'
        download_file_from_s3(tmp_path, s3_path)
        extension = tmp_path.split('.')[-1]
        if extension == 'csv':
            df = pd.read_csv(tmp_path)
        elif extension == 'parquet':
            df = pd.read_parquet(tmp_path)
        else:
            log.error(f'{extension} extension is not supported.')
            raise InvalidExtensionException
    return df


def read_newest_df_from_s3(s3_dir):
    file_list = list_s3_dir(s3_dir)
    newest_s3_path = select_newest_file(file_list)
    return read_df_from_s3(newest_s3_path)


class InvalidExtensionException(Exception):
    pass
