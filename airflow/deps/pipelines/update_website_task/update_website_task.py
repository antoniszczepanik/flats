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

def prepare_top_offers(df, dtype, offers_from=None, offer_number=30):
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
        columns.PRICE,
        columns.PRICE_M2,
        diff_col,
    ]]
    df = (df.pipe(convert_links_to_a_tags)
            .pipe(filter_outliers, dtype=dtype)
            .head(offer_number)
            .pipe(remove_duplicates_in_title)
            .pipe(reverse_sign, column=diff_col)
            .rename(columns={
                columns.URL:'Url',
                columns.DATE_ADDED:'Added',
                columns.TITLE:'Title',
                columns.SIZE:'Size',
                columns.PRICE: 'Price',
                columns.PRICE_M2:'Price / m2',
                diff_col:'Underpriced by',
            })
              .round(0)
              .to_html(index=False, escape=False)
            )
    return df

def filter_outliers(df, dtype):
    """ Filter offers based on custom defined "outliers" indicators"""
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

def convert_links_to_a_tags(df):
    df = df.copy()
    a_tag_pattern = '<a href="{link}" target="_blank">Link</a>'
    df[columns.URL] = df[columns.URL].apply(lambda link: a_tag_pattern.format(link=link))
    return df

def remove_duplicates_in_title(df):
    df = df.copy()
    df[columns.TITLE] = df[columns.TITLE].apply(
        lambda title: ", ".join(list(set(title.split(', '))))
    )
    return df

def reverse_sign(df, column):
    df = df.copy()
    df[column] = -df[column]
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
