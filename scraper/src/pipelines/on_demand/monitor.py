import logging

import columns
import common


log = logging.getLogger(__name__)

def monitor(dtype: str):
    """
    Log basic stats of each of the dataframes in the pipeline.
    """
    data_to_monitor = {
        "raw": common.RAW_DATA_PATH,
        "concated": common.CONCATED_DATA_PATH,
        "clean": common.CLEAN_DATA_PATH,
        "final": common.FINAL_DATA_PATH,
        "predicted": common.PREDICTED_DATA_PATH,
    }
    for name, path in data_to_monitor.items():
        log.info(f"Stats of {name} {dtype} data:")
        df = common.fs.read_newest_df(path, dtype)
        log_dataframe_stats(df)
        print()


def log_dataframe_stats(df):
    log.info(f"df.shape: {df.shape}")
    log.info(f"latest date added: {df[columns.DATE_ADDED].max()}")
