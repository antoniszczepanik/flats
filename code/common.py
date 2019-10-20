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
S3_DATA_BUCKET = "flats-data"
DATA_TYPES = ("sale", "rent")
RAW_DATA_PATH = S3_DATA_BUCKET + "/{data_type}/raw"
CONCATED_DATA_PATH = S3_DATA_BUCKET + "/{data_type}/concated"
CLEAN_DATA_PATH = S3_DATA_BUCKET + "/{data_type}/clean"
FINAL_DATA_PATH = S3_DATA_BUCKET + "/{data_type}/final"

# models paths
S3_MODELS_BUCKET = "flats-models"
S3_MODELS_CLEANING_MAP_DIR = "coords_encoding"

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


def select_newest_file(file_paths):
    """ Select string with most current datetime in name. """
    if len(file_paths) == 0:
        return None
    datetimes = []
    for path in file_paths:
        datetimes.append(get_date_from_filename(path))
    max_pos = datetimes.index(max(datetimes))
    return file_paths[max_pos]


def select_newest_date(file_paths):
    """ Select newest date from list of strings and return datetime object."""
    if len(file_paths) == 0:
        return None
    datetimes = []
    for path in file_paths:
        datetimes.append(get_date_from_filename(path))
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
    """Returns a list of filenames"""
    bucket, path = split_bucket_path(s3_dir)
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
    file_list = [f"{bucket}/{f['Key']}" for f in response["Contents"]]
    # do not list directories
    file_list = [f for f in file_list if f[-1] != "/"]
    # do not list recursively
    file_list = [
        f for f in file_list if len(f.split("/")) == len(s3_dir.split("/")) + 1
    ]
    return file_list


def split_bucket_path(s3_path):
    splitted = s3_path.split("/")
    bucket = splitted[0]
    path = "/".join(splitted[1:])
    return bucket, path


def get_filename(s3_path):
    return s3_path.split("/")[-1]


def upload_df_to_s3(df, s3_path):
    filename = get_filename(s3_path)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = f"{tmpdir}/{filename}"
        extension = tmp_path.split(".")[-1]
        if extension == "csv":
            df.to_csv(tmp_path, index=False)
        elif extension == "parquet":
            df.to_parquet(tmp_path)
        else:
            log.error(f"{extension} extension is not supported.")
            raise InvalidExtensionException
        upload_file_to_s3(tmp_path, s3_path)


def read_df_from_s3(s3_path, columns_to_skip=None):
    filename = get_filename(s3_path)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = f"{tmpdir}/{filename}"
        download_file_from_s3(tmp_path, s3_path)
        extension = tmp_path.split(".")[-1]
        if extension == "csv":
            if columns_to_skip:
                try:
                    # sample columns
                    columns = pd.read_csv(tmp_path, nrows=1).columns
                except pd.errors.EmptyDataError:
                    log.warning(f"Failed to parse dataframe with no columns: {s3_path}")
                    return pd.DataFrame()
                else:
                    columns_to_use = list(set(columns) - set(columns_to_skip))
                    df = pd.read_csv(tmp_path, usecols=columns_to_use, low_memory=True)
            else:
                df = pd.read_csv(tmp_path, low_memory=True)
        elif extension == "parquet":
            df = pd.read_parquet(tmp_path)
        else:
            log.error(f"{extension} extension is not supported.")
            raise InvalidExtensionException
    return df


def read_newest_df_from_s3(s3_dir):
    file_list = list_s3_dir(s3_dir)
    newest_s3_path = select_newest_file(file_list)
    return read_df_from_s3(newest_s3_path)


def get_last_operation_date(s3_dir):
    """Parse datetime from filenames in given s3 directory"""
    pass


class InvalidExtensionException(Exception):
    pass
