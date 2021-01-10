import logging

from pipelines.process.cleaning_task import clean
from pipelines.process.add_features import features
from pipelines.process.prepare_final import prepare_final

log = logging.getLogger(__name__)

def process(dtype):
    log.info("Started processing")
    clean(dtype)
    features(dtype)
    model_apply(dtype)
    prepare_final(dtype)
    log.info("Finished processing")
