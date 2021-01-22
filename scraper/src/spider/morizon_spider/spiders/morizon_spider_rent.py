import scrapy

from morizon_spider.items import MorizonSpiderItem
from .morizon_spider import MorizonSpider
from common import get_process_from_date


class MorizonSpiderRent(MorizonSpider):

    name = "rent"

    def __init__(self, *args, **kwargs):

        super(MorizonSpiderRent, self).__init__(*args, **kwargs)
        self.chunk_size = 150
        self.max_price = 20000
        self.url_base = "https://www.morizon.pl/do-wynajecia/mieszkania"
        self.start_urls = [
            self.url_base
            + f"/?ps%5Bprice_from%5D={self.chunker}&ps%5Bprice_to%5D={self.chunk_size}"
            + self.date_filter_str
        ]

    def _read_last_scraping_date(self):
        return get_process_from_date("rent", last_date_of="raw")
