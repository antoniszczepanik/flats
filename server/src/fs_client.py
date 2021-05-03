from datetime import datetime
from joblib import dump, load
import logging
import os

import pandas as pd

log = logging.getLogger(__name__)


class FsClient:
    def __init__(self, root="/data"):
        self.root = root
        log.info(f"Will use local filesystem as data backend at {root}")

    def get_full_path(self, path):
        return f"{self.root}/{path}"

    def list_dir(self, directory):
        fpath = self.get_full_path(directory)
        files = [directory+"/"+f for f in os.listdir(fpath)]
        if not files:
            log.info("Did not find any files in {fpath}.")
            return None
        return files

    def save_df(self, df, path):
        log.info(f"Saving {path}")
        return df.to_csv(self.get_full_path(path), index=False)

    def save_df_with_timestamp(self, df, path, keyword, dtype, extension='csv'):
        """ Assumes path in fomrat 'flats-data/{dtype}/clean'"""
        path = path.format(data_type=dtype)
        current_dt = datetime.now().strftime("%Y_%m_%dT%H_%M_%S")
        path += f"/{dtype}_{keyword}_{current_dt}.{extension}"
        return self.save_df(df, path)

    def save_model(self, model, path):
        log.info(f"Saving {path}")
        return dump(model, self.get_full_path(path), compress=3)

    def save_model_with_timestamp(self, model, path, dtype, keyword):
        """ Assumes path in format 'flats-models/{dtype}'"""
        path=path.format(data_type=dtype)
        current_dt = datetime.now().strftime("%Y_%m_%dT%H_%M_%S")
        path += f"/{dtype}_{keyword}_{current_dt}.joblib"
        return self.upload_model(model, path)

    def read_df(self, path, columns_to_skip=None):
        ext =path.split(".")[-1]
        if ext == "csv":
            if columns_to_skip:
                # try to sample
                first_row =pd.read_csv(self.get_full_path(path), nrows=1)
                columns_to_select = [c for c in first_row.columns if c not in columns_to_skip]
                return pd.read_csv(self.get_full_path(path), usecols=columns_to_select)
            return pd.read_csv(self.get_full_path(path))
        elif ext == "parquet":
            return pd.read_parquet(self.get_full_path(path))
        else:
            raise Exception(f"Extension {ext} not handled.")



    def read_newest_df(self, directory, dtype):
        directory = directory.format(data_type=dtype)
        file_list = self.list_dir(directory)
        newest_path = self.select_newest_file(file_list)
        if newest_path:
            return self.read_df(newest_path)
        else:
            return None

    def read_model(self, path):
        return load(self.get_full_path(path))

    def read_newest_model(self, directory, dtype):
        directory = directory.format(data_type=dtype)
        file_list = self.list_dir(directory)
        newest_path = self.select_newest_file(file_list)
        if newest_path:
            return self.read_model(newest_path)
        else:
            return None

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
