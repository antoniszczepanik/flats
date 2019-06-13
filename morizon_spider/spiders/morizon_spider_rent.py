import scrapy
from morizon_spider.items import MorizonSpiderItem
from datetime import datetime, timedelta
from .morizon_spider import MorizonSpider

class MorizonSpiderRent(MorizonSpider):

    name = 'morizon_rent'  

    def __init__(self, date_range=None, **kwargs):

        # Date conversion helper dictionary
        self.months_dict = {
        'stycznia': '1',
        'lutego': '2',
        'marca': '3',
        'kwietnia': '4', 
        'maja': '5',
        'czerwca': '6',
        'lipca': '7',
        'sierpnia': '8',
        'września': '9',
        'października': '10',
        'listopada': '11',
        'grudnia': '12'
        }

        # Allow user defined date_range argument
        super(MorizonSpider, self).__init__(**kwargs)
        self.date_range = date_range
        self.set_previous_date('2018-01-01', previous_scraping_date_path='morizon_spider/previous_scraping_date_rent.pkl')
        if self.date_range == None: 
            # Check if previous scraping date is available, save current date if possible
            self.date_range = self.read_and_update_previous_scraping_date(previous_scraping_date_path='morizon_spider/previous_scraping_date_rent.pkl')

        # Morizon won't display all offers if following pagination - the max n of pages is 200
        # I will introduce chunker variable and chunk all requested offers by price chunks
        # ex. first flats with prices 0-500, then 500-1000 etc 
        self.chunk_size = 150 
        self.chunker = 0
        self.max_price = 20000 
        
        # Handling optional date_range argument
        self.today_str = datetime.now().date().strftime('%d-%m-%Y')
        self.yesterday_str = (datetime.now().date() - timedelta(days=1)).strftime('%d-%m-%Y')
        if self.date_range is not None: 
            self.logger.info(f'Found date range_argument: {self.date_range}') 
            if len(self.date_range.split(',')) == 2:
                self.date_range = self.date_range.split(',')
                self.start_date = datetime.strptime(self.date_range[0], '%Y-%m-%d')
                self.end_date = datetime.strptime(self.date_range[1], '%Y-%m-%d')
            elif self.date_range == 'yesterday':
                self.start_date = datetime.strptime(self.yesterday_str, '%d-%m-%Y')
                self.end_date = datetime.strptime(self.yesterday_str, '%d-%m-%Y')
            else:
                self.start_date = datetime.strptime(self.date_range, '%Y-%m-%d')
                self.end_date  = datetime.strptime(self.yesterday_str, '%d-%m-%Y')
        else:
            self.start_date = datetime.strptime('2000-01-01', '%Y-%m-%d')
            self.end_date = datetime.strptime(self.yesterday_str, '%d-%m-%Y')
            self.logger.info('Did not find date_range argument. Will scrape all offers.') 

        # Optimize scraped space by using date filter arg
        date_diff = (datetime.strptime(self.yesterday_str, '%d-%m-%Y') - self.start_date).days 
        possible_date_selections = [3, 7, 30, 90, 180]
        date_filter = None
        for date_selection in possible_date_selections:
            if date_diff < date_selection:
                date_filter = date_selection
                break

        # Formating start_urls
        if date_filter is not None:
            self.date_filter_str = f"&ps%5Bdate_filter%5D={date_filter}"
        else:
            self.date_filter_str = ''
        self.logger.info(f'Setting start_date to {self.start_date}...')
        self.logger.info(f'Setting end_date to {self.end_date}...')
        self.logger.info(f'Date diff = {date_diff} days')
        self.logger.info(f'Setting date_filter string to "{self.date_filter_str}"')
        self.url_base = 'https://www.morizon.pl/do-wynajecia/mieszkania'
        self.start_urls = [self.url_base + f"/?ps%5Bprice_from%5D={self.chunker}&ps%5Bprice_to%5D={self.chunk_size}" + self.date_filter_str]


