import os

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from spider.morizon_spider.spiders.morizon_spider import MorizonSpider
from spider.morizon_spider.spiders.morizon_spider_rent import MorizonSpiderRent

def scrape_task():
    # use project settings
    settings_file_path = 'spider.morizon_spider.settings' # The path seen from root, ie. from main.py
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)

    for spider in [MorizonSpider, MorizonSpiderRent]:
        process = CrawlerProcess(get_project_settings())
        process.crawl(spider)

    process.start(stop_after_crawl=False)
