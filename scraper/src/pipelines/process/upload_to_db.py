import logging
import sys
import requests

import pandas as pd

from common import FINAL_PATH, fs

log = logging.getLogger(__name__)

def upload(dtype):
    log.info("Started uploading data to database...")
    df = fs.read_newest_df(
        FINAL_PATH,
        dtype=dtype,
    )
    size = len(df)
    log.info(f"Final dataframe shape: {df.shape}")
    url = "http://server:8000/api/v1/offers/"
    headers = {'Content-type': 'application/json'}
    already_uploaded = 0
    for ix in df.index:
        row_as_json = df.loc[ix].to_json()
        response = requests.post(url, headers=headers, data=str(row_as_json))
        already_uploaded += 1
        if already_uploaded % 500 == 0:
            log.info(f"Already uploaded {already_uploaded}/{size} offers")

    log.info("Successfully uploaded data to database")
