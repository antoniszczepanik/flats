import logging
import sys
import requests

import pandas as pd

from common import S3_FINAL_PATH
from s3_client import s3_client

log = logging.getLogger(__name__)
s3_client = s3_client()

def upload(dtype):
    log.info("Started uploading data to database...")
    df = s3_client.read_newest_df_from_s3(
        S3_FINAL_PATH,
        dtype=dtype,
    )
    size = len(df)
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
