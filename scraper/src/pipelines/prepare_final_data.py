"""
Read predicted dataset, generate json data with the result, and update website.  """
import datetime
import logging
from unidecode import unidecode

import pandas as pd

import columns
from common import (
    CONCATED_DATA_PATH,
    PREDICTED_DATA_PATH,
    TO_UPLOAD_DATA_PATH,
    SITE_DATA_LOCAL_PATH,
    SITE_DATA_S3_PATH,
)
from s3_client import s3_client

log = logging.getLogger(__name__)

s3_client = s3_client()


def task():
    log.info("Starting prepare data task ...")

    df_sale = read_and_merge_required_dfs('sale')
    df_rent = read_and_merge_required_dfs('rent')

    top_sale = prepare_offers(df_sale, 'sale')
    log.info(f'Prepared sale shape {top_sale.shape}')
    top_rent = prepare_offers(df_rent, 'rent')
    log.info(f'Prepared rent shape {top_rent.shape}')

    top_offers = pd.concat([top_sale, top_rent])
    log.info(f'All offers shape {top_offers.shape}')

    s3_client.upload_df_to_s3_with_timestamp(
         top_offers,
         TO_UPLOAD_DATA_PATH,
         keyword='prepared',
         dtype='sale',
    )


def read_and_merge_required_dfs(dtype):
    predicted_df = s3_client.read_newest_df_from_s3(
        PREDICTED_DATA_PATH,
        dtype=dtype,
    )
    concated_df = s3_client.read_newest_df_from_s3(
        CONCATED_DATA_PATH,
        dtype=dtype,
    )
    df = predicted_df.merge(
        concated_df[[columns.OFFER_ID,
                     columns.URL,
                     columns.TITLE]],
        on=columns.OFFER_ID,
        how='left')
    return df

def prepare_offers(df, dtype):
    pred_col = columns.SALE_PRED if dtype == 'sale' else columns.RENT_PRED
    df[pred_col] = df[pred_col] * df[columns.SIZE]
    df = (df.assign(offer_type=dtype)
          .pipe(select_output_cols, pred_col)
          .rename(columns={
              columns.URL: 'url',
              columns.DATE_ADDED: 'added',
              columns.TITLE: 'title',
              columns.SIZE: 'size',
              columns.PRICE: 'price',
              columns.PRICE_M2: 'price_m2',
              pred_col: 'estimate',
              columns.LON: 'lon',
              columns.LAT: 'lat',
          })
          )
    return df

def select_output_cols(df: pd.DataFrame, pred_col: str) -> pd.DataFrame:
    return df[[
        columns.URL,
        columns.DATE_ADDED,
        columns.TITLE,
        columns.SIZE,
        columns.PRICE,
        pred_col,
        columns.PRICE_M2,
        columns.LON,
        columns.LAT,
        'offer_type'
    ]]
