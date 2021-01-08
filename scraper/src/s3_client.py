from datetime import datetime
from joblib import dump, load
import logging
import os
import tempfile

import boto3
from botocore.exceptions import ClientError, ProfileNotFound
import pandas as pd

from common import get_current_dt

log = logging.getLogger(__name__)


class s3_client:

    def __init__(self):
        if os.environ.get("USE_MINIO", False) == "true":
            log.info("Will use minio as S3 connection")
            self.client = boto3.Session().client(
                "s3",
                endpoint_url="http://minio:9000",
                aws_access_key_id="minio",
                aws_secret_access_key="miniominio",
            )
        else:
            try:
                self.client = boto3.Session(profile_name='flats').client("s3")
            except ProfileNotFound:
                self.client = boto3.Session().client("s3")

    def upload_file_to_s3(self, file_name, s3_path, metadata=None, content_type=None):
        bucket, path = self.split_bucket_path(s3_path)
        # Upload the file
        log.info(f"Sending {path} to {bucket} bucket...")
        try:
            if metadata is not None:
                response = self.client.upload_file(
                    file_name,
                    bucket,
                    path,
                    ExtraArgs={"Metadata": metadata},
                )
            elif content_type is not None:
                response = self.client.upload_file(
                    file_name,
                    bucket,
                    path,
                    ExtraArgs={"ContentType": content_type},
                )
            else:
                response = self.client.upload_file(file_name, bucket, path)
        except ClientError as e:
            log.error(e)
            return False
        log.info(f"Successfully uploaded {path} to {bucket} bucket.")
        return True


    def download_file_from_s3(self, filename, s3_path):
        bucket, path = self.split_bucket_path(s3_path)
        # Download the file
        log.info(f"Downloading {path} from {bucket} bucket ...")
        try:
            response = self.client.download_file(bucket, path, filename)
        except ClientError as e:
            log.error(e)
            return False
        log.info(f"Successfully downloaded {path} from {bucket} bucket.")
        return True


    def list_s3_dir(self, s3_dir):
        """Returns a list of filenames"""
        bucket, path = self.split_bucket_path(s3_dir)
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=path)
        file_list = [f"{bucket}/{f['Key']}" for f in response["Contents"]]
        # do not list directories
        file_list = [f for f in file_list if f[-1] != "/"]
        # do not list recursively
        file_list = [
            f for f in file_list if len(f.split("/")) == len(s3_dir.split("/")) + 1
        ]
        return file_list

    def upload_df_to_s3(self, df, s3_path):
        filename = self.get_filename(s3_path)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = f"{tmpdir}/{filename}"
            extension = tmp_path.split(".")[-1]
            if extension == "csv":
                df.to_csv(tmp_path, index=False)
            else:
                log.error(f"{extension} extension is not supported.")
                raise InvalidExtensionException
            return self.upload_file_to_s3(tmp_path, s3_path)

    def upload_df_to_s3_with_timestamp(self, df, s3_path, keyword, dtype, extension='csv'):
        """ Assumes s3 path in fomrat 'flats-data/{dtype}/clean'"""
        s3_path = s3_path.format(data_type=dtype)
        current_dt = get_current_dt()
        s3_path += f"/{dtype}_{keyword}_{current_dt}.{extension}"
        return self.upload_df_to_s3(df, s3_path)

    def upload_model_to_s3(self, model, s3_path, metadata=None):
        filename = self.get_filename(s3_path)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = f"{tmpdir}/{filename}"
            s = dump(model, tmp_path, compress=3)
            return self.upload_file_to_s3(tmp_path, s3_path, metadata)

    def upload_model_to_s3_with_timestamp(self, model, s3_path, dtype, keyword, metadata=None):
        """ Assumes s3 path in format 'flats-models/{dtype}'"""
        s3_path=s3_path.format(data_type=dtype)
        current_dt = get_current_dt()
        s3_path += f"/{dtype}_{keyword}_{current_dt}.joblib"
        return self.upload_model_to_s3(model, s3_path, metadata)

    def read_df_from_s3(self, s3_path, columns_to_skip=None):
        filename = self.get_filename(s3_path)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = f"{tmpdir}/{filename}"
            self.download_file_from_s3(tmp_path, s3_path)
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
                        df = pd.read_csv(tmp_path, usecols=columns_to_use, low_memory=False)
                else:
                    df = pd.read_csv(tmp_path, low_memory=False)
            elif extension == "parquet":
                df = pd.read_parquet(tmp_path)
            else:
                log.error(f"{extension} extension is not supported.")
                raise InvalidExtensionException
        return df


    def read_newest_df_from_s3(self, s3_dir, dtype):
        s3_dir = s3_dir.format(data_type=dtype)
        file_list = self.list_s3_dir(s3_dir)
        newest_s3_path = self.select_newest_file(file_list)
        if newest_s3_path:
            return self.read_df_from_s3(newest_s3_path)
        else:
            return None

    def read_model_from_s3(self, s3_path):
        filename = self.get_filename(s3_path)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = f"{tmpdir}/{filename}"
            self.download_file_from_s3(tmp_path, s3_path)
            model = load(tmp_path)
            return model

    def read_newest_model_from_s3(self, s3_dir, dtype):
        s3_dir = s3_dir.format(data_type=dtype)
        file_list = self.list_s3_dir(s3_dir)
        newest_s3_path = self.select_newest_file(file_list)
        if newest_s3_path:
            return self.read_model_from_s3(newest_s3_path)
        else:
            return None

    def split_bucket_path(self, s3_path):
        splitted = s3_path.split("/")
        bucket = splitted[0]
        path = "/".join(splitted[1:])
        return bucket, path

    def select_newest_file(self, file_paths):
        """ Select string with most current datetime in name. """
        if len(file_paths) == 0:
            return None
        datetimes = []
        for path in file_paths:
            date = self.get_date_from_filename(path)
            # filter nans
            if date:
                datetimes.append(date)

        max_pos = datetimes.index(max(datetimes))
        return file_paths[max_pos]


    def get_date_from_filename(self, filename):
        date_numbers = "".join([x for x in filename if x.isdigit()])
        # make sure this is a valid datetime format used accross project
        if len(date_numbers) != 14:
            log.warning(f"Not getting date from invalid file name: {filename}")
            return None
        return datetime.strptime(date_numbers, "%Y%m%d%H%M%S")


    def get_filename(self, s3_path):
        return s3_path.split("/")[-1]
