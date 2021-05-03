from datetime import datetime, timedelta
import logging
import shutil
import os

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import scrapy

from common import (
    RAW_DATA_PATH,
    SCRAPING_TEMPDIR_PATH,
    get_current_dt,
    select_newest_date,
    fs,
)
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

def task(data_type):
    # use project settings
    settings_file_path = 'spider.morizon_spider.settings' # The path seen from root, ie. from main.py
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
    spider = spiders[data_type]
    process = CrawlerProcess(get_project_settings())
    process.crawl(spider)
    process.start()
    mv_scraped_file(data_type)


def mv_scraped_file(data_type):
    output_path = SCRAPING_TEMPDIR_PATH.format(data_type=data_type)
    current_dt = get_current_dt()
    target_path  = RAW_DATA_PATH.format(data_type=data_type) + f"/raw_{data_type}_{current_dt}.csv"
    shutil.copyfile(output_path, "/data/"+target_path)
    os.remove(output_path)
