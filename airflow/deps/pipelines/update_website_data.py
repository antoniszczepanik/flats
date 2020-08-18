"""
Read predicted dataset, generate json data with the result, and update website.
"""
import datetime
import logging
from unidecode import unidecode

import pandas as pd

import columns
from common import (
    CONCATED_DATA_PATH,
    PREDICTED_DATA_PATH,
    SITE_DATA_LOCAL_PATH,
    SITE_DATA_S3_PATH,
)
from s3_client import s3_client

log = logging.getLogger(__name__)

s3_client = s3_client()


def update_website_data_task():
    log.info("Starting update_website task...")

    df_sale = read_and_merge_required_dfs('sale')
    df_rent = read_and_merge_required_dfs('rent')

    today = datetime.date.today()
    ago = str(today - datetime.timedelta(3))

    top_sale = prepare_top_offers(df_sale, 'sale', offers_from=ago)
    log.info(f'Top sale shape {top_sale.shape}')
    top_rent = prepare_top_offers(df_rent, 'rent', offers_from=ago)
    log.info(f'Top rent shape {top_rent.shape}')

    top_offers = pd.concat([top_sale, top_rent])
    log.info(f'Top offers shape {top_offers.shape}')
    top_offers.to_json(SITE_DATA_LOCAL_PATH, orient='records')
    resp = upload_json_data(SITE_DATA_LOCAL_PATH, SITE_DATA_S3_PATH)
    if resp:
        log.info(f"Successfully finished updating final data in json format.")

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

def prepare_top_offers(df, dtype, offers_from=None):
    if dtype == 'sale':
        pred_col = columns.SALE_PRED
        diff_col = columns.SALE_DIFF
    elif dtype == 'rent':
        pred_col = columns.RENT_PRED
        diff_col = columns.RENT_DIFF

    df[diff_col] = df[columns.PRICE_M2] - df[pred_col]
    df[pred_col] = df[pred_col] * df[columns.SIZE]

    if offers_from:
        df = df[df[columns.DATE_ADDED] > offers_from]

    df = (df.pipe(sort_and_filter_by_pred_actual_ratio, pred_col)
            .assign(offer_type=dtype)
            .pipe(select_output_cols, pred_col)
            .pipe(filter_outliers, dtype=dtype)
            .pipe(detect_cities)
            .pipe(remove_duplicates_in_title)
            .rename(columns={
                columns.URL: 'url',
                columns.DATE_ADDED: 'added',
                columns.TITLE: 'title',
                columns.SIZE: 'size',
                columns.PRICE: 'price',
                pred_col: 'estimate',
            })
            .drop(columns=[columns.PRICE_M2])
            .round(0)
            )
    return df

def sort_and_filter_by_pred_actual_ratio(df: pd.DataFrame, pred_col: str) -> pd.DataFrame:
    df = df.copy()
    df['pred_to_price'] = df[pred_col] / df[columns.PRICE]
    return (df.loc[df['pred_to_price'] > 1.1] # only offers more attractive than 10% discount
              .sort_values(by='pred_to_price')
              .drop(columns=['pred_to_price']))

def select_output_cols(df: pd.DataFrame, pred_col: str) -> pd.DataFrame:
    return df[[
        columns.URL,
        columns.DATE_ADDED,
        columns.TITLE,
        columns.SIZE,
        columns.PRICE,
        pred_col,
        columns.PRICE_M2,
        'offer_type'
    ]]

def filter_outliers(df, dtype):
    """ Filter offers based on hardcoded "outliers" indicators"""
    df = df.copy()
    if dtype == 'sale':
        df = df[df[columns.PRICE_M2] > 1800]
        df = df[df[columns.PRICE] > 30000]
    elif dtype == 'rent':
        df = df[df[columns.PRICE_M2] > 10]
        df = df[df[columns.PRICE] > 800]
    df = df[df[columns.SIZE] < 300]
    df = df[df[columns.SIZE]> 15]
    return df

def detect_cities(df):
    """ Detect cities from titles and remove offers where no city is detected """
    def get_city_from_title(title):
        title = unidecode(title).lower()
        city_list = [
            'warszawa',
            'krakow',
            'lodz',
            'wroclaw',
            'poznan',
            'gdansk',
            'katowice',
            'szczecin',
            'bydgoszcz',
            'lublin',
            'bialystok',
        ]
        for city in city_list:
            if city in title:
                return city
        return None

    df['city'] = df[columns.TITLE].apply(get_city_from_title)
    return df

def remove_duplicates_in_title(df):
    df = df.copy()
    df[columns.TITLE] = df[columns.TITLE].apply(
        lambda title: ", ".join(list(set(title.split(', '))))
    )
    return df

def upload_json_data(from_, to):
    response = s3_client.upload_file_to_s3(
        from_,
        to,
        content_type='application/json',
    )
    return response
