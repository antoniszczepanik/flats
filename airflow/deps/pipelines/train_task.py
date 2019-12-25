import logging as log

import auto_ml
import pandas as pd
from sklearn.model_selection import train_test_split
from auto_ml import Predictor

import columns
from common import FINAL_DATA_PATH, MODELS_PATH, get_current_dt
from s3_client import s3_client

log.basicConfig(**logs_conf)

s3_client = s3_client()

SALE_MODEL_COLUMNS = [
    columns.DESC_LEN,
    columns.FLOOR,
    columns.BUILDING_TYPE,
    columns.BUILDING_HEIGHT,
    columns.BUILDING_YEAR,
    columns.SIZE,
    columns.LAT,
    columns.LON,
    columns.CLUSTER_ID,
    columns.CLUSTER_MEAN_PRICE,
    columns.CLUSTER_MEAN_PRICE_M2,
    columns.CLUSTER_CENTER_DIST_KM,
    columns.CLUSTER_COORDS_FACTOR,
]

def train_task(data_type):
    log.info(f"Starting train model task for {data_type} data.")
    final_df = s3_client.read_newest_df_from_s3(FINAL_DATA_PATH.format(data_type=data_type))
    final_df = final_df[SALE_MODEL_COLUMNS]

    model = train_model(final_df, data_type)

    current_dt = get_current_dt()
    target_s3_name = f"{data_type}_model_{current_dt}.parquet"
    target_s3_path = (
    MODELS_PATH.format(data_type=data_type) + target_s3_name
    )
    #TODO: implement upload model (and also read model)
    s3_client.upload_model_to_s3(model, target_s3_path)

    log.info(f"Finished train model task for {data_type} data.")

def train_model(df, data_type):
    #TODO: implement training model task
    pass



