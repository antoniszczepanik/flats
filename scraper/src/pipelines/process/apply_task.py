"""
Read final data with all the features and apply a model.
"""
import os
import logging

import pandas as pd

from common import (
    FINAL_DATA_PATH,
    MODELS_PATH,
    RENT_MODEL_INPUTS,
    SALE_MODEL_INPUTS,
    PREDICTED_DATA_PATH,
)
import columns
from s3_client import s3_client
from pipelines.utils import read_df, save_df


log = logging.getLogger(__name__)

s3_client = s3_client()

CHUNK_SIZE = 1000

def model_apply(data_type):
    log.info("Starting apply model pipeline...")
    model = s3_client.read_newest_model_from_s3(MODELS_PATH, dtype=data_type)
    if model is None:
        log.warning(f'Did not find any model for {data_type}')
        return

    final_df = read_df(LOCAL_ROOT, keyword='final', data_type)

    log.info(f'Applying model for {data_type}...')
    if data_type == 'sale':
        final_df = final_df.dropna(subset=SALE_MODEL_INPUTS)
        final_df[columns.SALE_PRED] = get_predictions(model, final_df, SALE_MODEL_INPUTS)
        final_df[columns.SALE_DIFF] = final_df[columns.PRICE_M2] - final_df[columns.SALE_PRED]
    elif data_type == 'rent':
        final_df = final_df.dropna(subset=RENT_MODEL_INPUTS)
        final_df[columns.RENT_PRED] = get_predictions(model, final_df, RENT_MODEL_INPUTS)
        final_df[columns.RENT_DIFF] = final_df[columns.PRICE_M2] - final_df[columns.RENT_PRED]
    save_df(final_df, LOCAL_ROOT, keyword='predicted', data_type)
    log.info(f"Successfully applied model for {data_type}.")
    log.info("Finished apply model pipeline.")

def get_predictions(model, df, input_cols):
    batches = [df[i:i+CHUNK_SIZE] for i in range(0, df.shape[0],CHUNK_SIZE)]
    predictions = []
    for batch_n, batch in enumerate(batches):
        log.info(f"Processing batch number {batch_n} ...")
        predicted_batch = model.predict(batch[input_cols])
        predictions.extend(predicted_batch)
    return predictions
