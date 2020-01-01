"""
Read predicted dataset, match links to given offers and send an email notification
with summary of the most interesting offers.
"""
import os
import logging as log

import pandas as pd

from common import (
    CONCATED_DATA_PATH,
    PREDICTED_DATA_PATH,
    logs_conf,
)
from s3_client import s3_client


log.basicConfig(**logs_conf)

s3_client = s3_client()

def notify_task(data_type):
    log.info("Starting notify task...")

    predicted_df = s3_client.read_newest_df_from_s3(PREDICTED_DATA_PATH, dtype=data_type)
    concated_df = s3_client.read_newest_df_from_s3(CONCATED_DATA_PATH, dtype=data_type)


    log.info(f"Successfully notified about {data_type} data.")
    log.info("Finished notifiation task.")

