from datetime import datetime
from datetime import timedelta
import logging as log
import os

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from common import (
    RAW_DATA_PATH,
    select_newest_date,
    logs_conf,
)
from s3_client import s3_client
from spider.morizon_spider.spiders.morizon_spider import MorizonSpider
from spider.morizon_spider.spiders.morizon_spider_rent import MorizonSpiderRent

spiders = {
    'sale': MorizonSpider,
    'rent': MorizonSpiderRent,
}

# If previously scraped package is less than SKIP_SCRAPING_BUFFER skip scraping
# mainly for development purposes (working with airflow)
SKIP_SCRAPING_BUFFER = 12

log.basicConfig(**logs_conf)

s3_client = s3_client()

def scrape_task(data_type):
    if is_needed(data_type):
        # use project settings
        settings_file_path = 'spider.morizon_spider.settings' # The path seen from root, ie. from main.py
        os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
        spider = spiders[data_type]
        process = CrawlerProcess(get_project_settings())
        process.crawl(spider)
        process.start()

def is_needed(data_type):
    raw_paths = s3_client.list_s3_dir(RAW_DATA_PATH.format(data_type=data_type))
    newest_date = select_newest_date(raw_paths)
    if datetime.now() - newest_date > timedelta(hours=SKIP_SCRAPING_BUFFER):
        return True
    else:
        log.info(f"Previous scraping date is {newest_date}. Skipping")
        return False
