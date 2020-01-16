"""
Read predicted dataset, generate html with the result, and update website with them.
"""
import datetime
import os
import logging as log

import pandas as pd

from common import (
    CONCATED_DATA_PATH,
    PREDICTED_DATA_PATH,
    HTML_TEMPLATE_PATH,
    CSS_LOCAL_PATH,
    HTML_LOCAL_PATH,
    HTML_S3_PATH,
    CSS_S3_PATH,
    logs_conf,
)
from s3_client import s3_client

log.basicConfig(**logs_conf)

s3_client = s3_client()


def update_website_task(data_type):
    log.info("Starting update_website task...")

    df_sale = read_and_merge_required_dfs('sale')
    df_rent = read_and_merge_required_dfs('rent')

    today = datetime.date.today()
    week_ago = str(today - datetime.timedelta(7))

    sale_html = prepare_top_offers(df_s, 'sale', offers_from=week_ago)
    rent_html = prepare_top_offers(df_r, 'rent', offers_from=week_ago)

    format_template(sale_html, rent_html, today)
    response = upload_formatted_html()
    if response:
        log.info(f"Successfully finished updating website.")

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

def prepare_top_offers(df, dtype, offers_from=None, offer_number=10):
    if dtype == 'sale':
        pred_col = columns.SALE_PRED
        diff_col = columns.SALE_DIFF
    elif dtype == 'rent':
        pred_col = columns.RENT_PRED
        diff_col = columns.RENT_DIFF

    df[diff_col] = df[columns.PRICE_M2] - df[pred_col]
    if offers_from:
        df = df[df[columns.DATE_ADDED] > offers_from]
    df = df.sort_values(diff_col)
    df = df[[
        columns.DATE_ADDED,
        columns.TITLE,
        columns.SIZE,
        columns.URL,
        columns.PRICE_M2,
        pred_col,
        diff_col
    ]]
    return df.head(offer_number).to_html()


def format_template(sale_html, rent_html, today):
    with open(HTML_LOCAL_PATH, 'w+') as outfile, open(HTML_TEMPLATE_PATH, 'r') as template:
        output = template.read().format(
            sale_html_table=sale_html,
            rent_html_table=rent_html,
            today=today,
        )
        outfile.write(output)

def upload_fromatted_html():
    response_1 = s3_client.upload_file_to_s3(
        HTML_LOCAL_PATH,
        HTML_S3_PATH,
    )
    response_2 = s3_client.upload_file_to_s3(
        CSS_LOCAL_PATH,
        CSS_S3_PATH,
    )
    if response_1 and response_2:
        return True
    return False
