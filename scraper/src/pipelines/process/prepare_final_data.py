"""
Read predicted dataset, generate json data with the result, and update website.  """
import datetime
import logging
from unidecode import unidecode

import pandas as pd

import columns
from common import FINAL_PATH, LOCAL_ROOT, get_process_from_date, fs
from pipelines.utils import read_df
from pipelines.process.cleaning_task import get_df_to_process

log = logging.getLogger(__name__)

def prepare_final(dtype):
    log.info("Starting prepare data task ...")
    df = read_and_merge_required_dfs(dtype)
    top = prepare_offers(df, dtype)
    log.info(f'Final shape {top.shape}')
    fs.save_df_with_timestamp(
         top,
         FINAL_PATH,
         keyword='final',
         dtype=dtype,
    )


def read_and_merge_required_dfs(dtype):
    predicted_df = read_df(LOCAL_ROOT, keyword='predicted', dtype=dtype)
    log.info(f'Predicted shape {predicted_df.shape}')
    from_date = get_process_from_date(dtype, last_date_of="final")
    concated_df = get_df_to_process(dtype, from_date)
    log.info(f'Concated shape {concated_df.shape}')

    df = predicted_df.merge(
        concated_df[[columns.OFFER_ID,
                     columns.URL,
                     columns.TITLE]],
        on=columns.OFFER_ID,
        how='left').drop_duplicates(subset=columns.OFFER_ID, keep="last")
    return df

def prepare_offers(df, dtype):
    pred_col = columns.SALE_PRED if dtype == 'sale' else columns.RENT_PRED
    df[pred_col] = df[pred_col] * df[columns.SIZE]
    df['price_estimate_diff'] = df[pred_col] - df[columns.PRICE]
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
              columns.OFFER_ID: 'offer_id',
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
        columns.OFFER_ID,
        'offer_type',
        'price_estimate_diff',
    ]]
