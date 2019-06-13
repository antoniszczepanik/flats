import scrapy
from morizon_spider.items import MorizonSpiderItem
from .morizon_spider import MorizonSpider

class MorizonSpiderFlats(MorizonSpider):

    name = 'morizon_flats'  

    def __init__(self, date_range=None, **kwargs):
        # Overwrite only chunker parameters due to different price level and path
        self.chunker = 0
        self.chunk_size = 150
        self.max_price = 20000 
        self.url_base = 'https://www.morizon.pl/do-wynajecia/mieszkania'
        self.date_range = date_range
        self.set_previous_date('2018-01-01', previous_scraping_date_path='morizon_spider/previous_scraping_date_rent.pkl')
        if self.date_range == None: 
            # Check if previous scraping date is available, save current date if possible
            self.date_range = self.read_and_update_previous_scraping_date(previous_scraping_date_path='morizon_spider/previous_scraping_date_rent.pkl')


