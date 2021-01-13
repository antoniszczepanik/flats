import logging

from pipelines.process.cleaning_task import clean
from pipelines.process.add_features import features
from pipelines.process.prepare_final_data import prepare_final
from pipelines.process.apply_task import model_apply
from pipelines.process.upload_to_db import upload

log = logging.getLogger(__name__)

def process_task(dtype):
    log.info("Started processing")
    clean(dtype)
    features(dtype)
    model_apply(dtype)
    prepare_final(dtype)
    upload(dtype)
    log.info("Finished processing")
