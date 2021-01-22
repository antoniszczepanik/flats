from datetime import datetime, timedelta
import logging
import os

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import scrapy

from common import (
    S3_RAW_DATA_PATH,
    SCRAPING_TEMPDIR_PATH,
    get_current_dt,
    select_newest_date,
)
from s3_client import s3_client
from spider.morizon_spider.spiders.morizon_spider import MorizonSpider
from spider.morizon_spider.spiders.morizon_spider_rent import MorizonSpiderRent

spiders = {
    'sale': MorizonSpider,
    'rent': MorizonSpiderRent,
}

# If previously scraped package is less than SKIP_SCRAPING_BUFFER skip scraping
# mainly for development purposes & not to overwhelm morizon
SKIP_SCRAPING_BUFFER = 1

log = logging.getLogger(__name__)

s3_client = s3_client()

def task(data_type):
    # use project settings
    settings_file_path = 'spider.morizon_spider.settings' # The path seen from root, ie. from main.py
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
    spider = spiders[data_type]
    process = CrawlerProcess(get_project_settings())
    process.crawl(spider)
    process.start()

    upload_scraped_file_to_s3(data_type)


def upload_scraped_file_to_s3(data_type):
    output_path = SCRAPING_TEMPDIR_PATH.format(data_type=data_type)
    current_dt = get_current_dt()
    output_target_s3_path  = S3_RAW_DATA_PATH.format(data_type=data_type) + f"/raw_{data_type}_{current_dt}.csv"
    is_success = s3_client.upload_file_to_s3(output_path, output_target_s3_path)
    os.remove(output_path)
    log.info(f'Removed temporary scraping dump file {output_path}.')
    return is_success
