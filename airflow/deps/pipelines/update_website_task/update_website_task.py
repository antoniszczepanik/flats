"""
Read predicted dataset, generate html with the result, and update website with them.
"""
import datetime
import os
import logging as log

import pandas as pd

import columns
from common import (
    CONCATED_DATA_PATH,
    PREDICTED_DATA_PATH,
    HTML_TEMPLATE_PATH,
    CSS_LOCAL_PATH,
    HTML_LOCAL_PATH,
    JS_LOCAL_PATH,
    HTML_S3_PATH,
    CSS_S3_PATH,
    JS_S3_PATH,
    logs_conf,
)
from s3_client import s3_client

log.basicConfig(**logs_conf)

s3_client = s3_client()


def update_website_task():
    log.info("Starting update_website task...")

    df_sale = read_and_merge_required_dfs('sale')
    df_rent = read_and_merge_required_dfs('rent')

    today = datetime.date.today()
    week_ago = str(today - datetime.timedelta(7))

    sale_html = prepare_top_offers(df_sale, 'sale', offers_from=week_ago)
    rent_html = prepare_top_offers(df_rent, 'rent', offers_from=week_ago)

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
        columns.URL,
        columns.DATE_ADDED,
        columns.TITLE,
        columns.SIZE,
        columns.PRICE_M2,
        columns.PRICE,
        pred_col,
        diff_col
    ]]
    df = convert_links_to_a_tags(df)
    return (df.head(offer_number)
              .rename(columns={
                  columns.URL:'Offer url',
                  columns.DATE_ADDED:'Date added',
                  columns.TITLE:'Title',
                  columns.SIZE:'Size',
                  columns.PRICE_M2:'Offer price/m2',
                  columns.PRICE: 'Offer price',
                  pred_col:'Price predicted/m2',
                  diff_col:'Price-prediction difference',
              })
              .round(0)
              .to_html(index=False,
                       escape=False,
                       )
            )

def convert_links_to_a_tags(df):
    a_tag_pattern = '<a href="{link}">Link</a>'
    df[columns.URL] = df[columns.URL].apply(lambda link: a_tag_pattern.format(link=link))
    return df


def format_template(sale_html, rent_html, today):
    with open(HTML_LOCAL_PATH, 'w+') as outfile, open(HTML_TEMPLATE_PATH, 'r') as template:
        output = template.read().format(
            sale_table=sale_html,
            rent_table=rent_html,
            today=today,
        )
        outfile.write(output)

def upload_formatted_html():
    response_html = s3_client.upload_file_to_s3(
        HTML_LOCAL_PATH,
        HTML_S3_PATH,
        content_type='text/html',
    )
    response_css = s3_client.upload_file_to_s3(
        CSS_LOCAL_PATH,
        CSS_S3_PATH,
        content_type='text/css',
    )
    response_js = s3_client.upload_file_to_s3(
        JS_LOCAL_PATH,
        JS_S3_PATH,
        content_type='application/javascript',
    )
    if False not in (response_html, response_css, response_js):
        return True
    return False
