import logging as log
from datetime import datetime
import pandas as pd


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
        if path.endswith(".csv"):
            df = pd.read_csv(path)
        elif path.endswith(".parquet"):
            df = pd.read_parquet(path)
            parquet_rows_n += len(df)
        total_rows_n += len(df)
        dfs.append(df)
    concatinated_df = pd.concat(dfs, sort=True).drop_duplicates(keep="last")
    columns_start = concatinated_df.columns
    concatinated_df = concatinated_df.dropna(axis="columns", how="all")
    columns_end = concatinated_df.columns
    # return descriptions in a different df - to large to handle
    descriptions = concatinated_df[["desc", "offer_id"]].dropna(axis="rows", how="all")
    concatinated_df = concatinated_df.drop("desc", axis=1)

    log.info(f"Removed empty columns {set(columns_start)-set(columns_end)}")
    log.info(f"Concatinated {len(dfs)} files.")
    log.info(f"Dropped {total_rows_n - len(concatinated_df)} duplicates.")
    log.info(f"Previous concatinated file had {parquet_rows_n}")
    log.info(f"Concatinated csv has {len(concatinated_df)} rows.")
    log.info(f"Added {len(concatinated_df)-parquet_rows_n} new rows")

    return concatinated_df, descriptions


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
        return []
    dt_strings = []
    for path in file_paths:
        dt_strings.append("".join([x for x in path if x.isdigit()]))

    datetimes = [datetime.strptime(x, "%Y%m%d%H%M%S") for x in dt_strings]
    max_pos = datetimes.index(max(datetimes))
    return file_paths[max_pos]


def name_from_path(filename):
    return filename.split("/")[-1]
