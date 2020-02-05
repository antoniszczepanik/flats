import logging as log

from common import (
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

def update_website_source():
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
    log.warning("Failed to upload site source.")
    return False
