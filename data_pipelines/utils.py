import logging as log
from datetime import datetime
import pandas as pd


# expected columns 
REQUIRED_COLUMNS =[
    'balcony',
    'building_height',
    'building_material',
    'building_type',
    'building_year',
    'conviniences',
    'date_added',
    'date_refreshed',
    'desc_len',
    'direct',
    'equipment',
    'flat_state',
    'floor',
    'heating',
    'lat',
    'lon',
    'market_type',
    'media',
    'offer_id',
    'price',
    'price_m2',
    'promotion_counter',
    'room_n',
    'size',
    'taras',
    'title',
    'url',
    'view_count',
]
COLUMNS_TO_SKIP = {'image_link','desc'}


log.basicConfig(
    level=log.INFO, format="%(asctime)s %(message)s", datefmt="%m-%d-%Y %I:%M:%S"
)


def concat_dfs(paths):
    """
    Concat all files by paths and drop all duplicates.
    Retrun 2 dataframes - full dataframe and descriptions
    separately.
    Handles .csv files and .parquet files.
    Treats .parquet file as main source of data (base for logging)
    """
    dfs = []
    total_rows_n = 0
    parquet_rows_n = 0

    for path in paths:
        log.info(f'Reading {path}')
        if path.endswith(".csv"):
            # don't read columns unused later
            columns = pd.read_csv(path, nrows=1).columns
            columns_to_use = list(set(columns) - COLUMNS_TO_SKIP)
            df = pd.read_csv(path, usecols=columns_to_use, low_memory=True)
        elif path.endswith(".parquet"):
            # don't read columns unused later
            df = pd.read_parquet(path)
            parquet_rows_n += len(df)
        total_rows_n += len(df)
        dfs.append(df)

    concatinated_df = pd.concat(dfs, sort=True).drop_duplicates(keep="last")

    log.info(f"Concatinated {len(dfs)} files.")
    log.info(f"Dropped {total_rows_n - len(concatinated_df)} duplicates.")
    log.info(f"Previous concatinated file had {parquet_rows_n}")
    log.info(f"Concatinated csv has {len(concatinated_df)} rows.")
    log.info(f"Added {len(concatinated_df)-parquet_rows_n} new rows.")

    return concatinated_df


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


def select_most_up_to_date_file(file_paths):
    # select file with most current datetime in name
    if len(file_paths) == 0:
        return None
    dt_strings = []

    for path in file_paths:
        date_numbers = "".join([x for x in path if x.isdigit()])
        # assure only files with datetimes are considered
        if len(date_numbers)== 14:
            dt_strings.append(date_numbers)

    datetimes = [datetime.strptime(x, "%Y%m%d%H%M%S") for x in dt_strings]
    max_pos = datetimes.index(max(datetimes))

    return file_paths[max_pos]


def name_from_path(filename):
    return filename.split("/")[-1]
