import scrapy
import pickle
from datetime import datetime, timedelta
from morizon_spider.items import MorizonSpiderItem

class MorizonSpider(scrapy.Spider):

    """Base for morizon spiders
    To use go to top spider directory and use command:
    'scrapy crawl -o csv_name.csv -a date_range='yesterday''

    'date_range' argument is optional and allows specifying:
    -range of dates (e.g. '2001-05-05,2015-05-12' - dates in 'YYYY-MM-DD' format separetad by a comma)
    -date from which start scraping (e.g. '2015-04-12') - spider will crawl from specified date untill yesterday
    -'yesterday' - shortcut to scrape only previous day
    When date_range is not specified spider will try to read the datetime from previous_scraping_date.pkl
    If previous date is not available it will scrape whole page info.

    """
    name = "morizon"

    def __init__(self, date_range=None, **kwargs):

        # Date conversion helper dictionary
        MONTHS_DICT = {
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
        #Warning! Use only for testing purposes!
        #self.set_previous_date('2019-06-11')
        self.date_range = date_range
        if self.date_range == None: 
            # Check if previous scraping date is available, save current date if possible
            self.date_range = self.read_and_update_previous_scraping_date()

        # Morizon won't display all offers if following pagination - the max n of pages is 200
        # I will introduce chunker variable and chunk all requested offers by price chunks
        # ex. first flats with prices 0-500, then 500-1000 etc 
        self.chunk_size = 20000
        self.chunker = 0
        self.max_price = 5000000
        
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
        self.url_base = "https://www.morizon.pl/mieszkania"
        self.start_urls = [self.url_base + f"/?ps%5Bprice_from%5D={self.chunker}&ps%5Bprice_to%5D={self.chunk_size}" + self.date_filter_str]

    def parse(self, response):

        # Find all links and their dates on parsed page
        links_to_scrape = [link for link in response.xpath("//a[@class='property_link']/@href").getall() if '/oferta/' in link] 
        links_to_scrape_dates_raw  = response.xpath("//span[@class='single-result__category single-result__category--date']//text()").getall()

        # Format dates
        links_to_scrape_dates = [ x for x in  [ x.replace('\n', '').replace(' ', '') for x in links_to_scrape_dates_raw ] if len(x)>0 ]
        links_to_scrape_dates = [ x.replace('dzisiaj', self.today_str).replace('wczoraj', self.yesterday_str) for x in links_to_scrape_dates ]
        if len(links_to_scrape) != len(links_to_scrape_dates):
            raise ValueError('Found more dates than links to scrape on a single page')

        # Iterate over links and dates - parse only in predefined date_range
        for link, date in zip(links_to_scrape, links_to_scrape_dates):
            date_datetime = datetime.strptime(date, '%d-%m-%Y')
            if date_datetime >= self.start_date and date_datetime <= self.end_date:
                yield scrapy.Request(link, callback=self.parse_offer, errback=self.errback_httpbin)

        #Look for next page button
        next_page = response.xpath("//a[@class='mz-pagination-number__btn mz-pagination-number__btn--next']/@href").get()
        if next_page:
            next_page = "https://www.morizon.pl" + next_page + self.date_filter_str
            yield scrapy.Request(next_page, callback=self.parse)
        else:
            #If button is not available start scraping next price chunk
            self.chunker += self.chunk_size
            range_low = self.chunker
            range_high = self.chunker + self.chunk_size

            # Stop if reached max
            if range_low >= self.max_price:
                raise scrapy.exceptions.CloseSpider(f"{self.max_price} price range reached. Stopping spider")

            #Log current range and number of scraped items
            self.logger.info(f"Currenly scraping offers in {range_low}-{range_high} zł price range")
            self.logger.info(f"Items scraped: {self.crawler.stats.get_value('item_scraped_count')}")
            
            next_page = self.url_base + f"/?ps%5Bprice_from%5D={range_low}&ps%5Bprice_to%5D={range_high}" + self.date_filter_str
            yield scrapy.Request(next_page, callback=self.parse)


    def parse_offer(self, response):

        full_info = MorizonSpiderItem()

        price = response.xpath("//li[@class='paramIconPrice']/em/text()").get()
        if price:
            full_info["price"] = price.replace("\xa0", "").replace(",",".").replace(" ", "").replace("~", "")
        else: # Not interested in offers without price 
            return

        price_m2 = response.xpath("//li[@class='paramIconPriceM2']/em/text()").get()
        if price_m2:
            full_info["price_m2"] = price_m2.replace("\xa0", "").replace(",",".").replace(" ", "").replace("~", "")

        size = response.xpath("//li[@class='paramIconLivingArea']/em/text()").get()
        if size:
            full_info["size"] = size.replace("\xa0", "").replace(",",".").replace(" ", "")

        room_n = response.xpath("//li[@class='paramIconNumberOfRooms']/em/text()").get()
        if room_n:
            full_info["room_n"] = room_n

        # Title
        title = " ".join(response.xpath("//div[@class='col-xs-9']//span/text()").getall()).replace("\n", "")
        full_info["title"] = title
                
        # Paramlist
        values = response.xpath("//section[@class='propertyParams']//tr/td").getall()
        keys = response.xpath("//section[@class='propertyParams']//tr/th").getall()

        for key, value in zip(keys, values):
            key = key.split("\n")[1].split(":")[0]
            value = value.split("\n")[1].split(" </td>")[0]
            if key == "Piętro":
                full_info["floor"] = value
            elif key == "Liczba pięter":
                full_info["building_height"] = value
            elif key == "Numer oferty":
                full_info["offer_id"] = value
            elif key == "Rok budowy":
                full_info["building_year"] = value
            elif key == "Opublikowano":
                # Further confirm if value is in specified range
                value_dt = self.polish_to_datetime(value) 
                if value_dt > self.end_date or value_dt < self.start_date:
                    return
                full_info["date_added"] = value
            elif key == "Zaktualizowano":
                full_info["date_refreshed"] = value
            elif key == "Typ budynku":
                full_info["building_type"] = value
            elif key == "Materiał budowlany":
                full_info["building_material"] = value
            elif key == "Rynek":
                full_info["market_type"] = value
            elif key == "Stan nieruchomości":
                full_info["flat_state"] = value
            elif key == "Balkon":
                full_info["balcony"] = value
            elif key == "Taras":
                full_info["taras"] = value
            elif key == "Winda":
                full_info["lift"] = value

        #GoogleInfo
        lat = response.xpath("//div[@class='GoogleMap']/@data-lat").get()
        lon = response.xpath("//div[@class='GoogleMap']/@data-lng").get()
        
        #Direct?
        if response.xpath("//div[@class='agentOwnerType']/text()").get():
            direct = 1
        else:
            direct = 0

        #Description
        desc = " ".join(response.xpath("//div[@class='description']//text()").getall())
        #Description len
        desc_len = len(desc)

        image_link = response.xpath("//img[@id='imageBig']/@src").get()

        #Offer stats
        stats = " ".join(response.xpath("//div[@class='propertyStat']/p/text()").getall())
        stats = [int(s) for s in stats.split() if s.isdigit()]

        full_info["lat"] = lat
        full_info["lon"] = lon
        full_info["url"] = response.request.url
        full_info["direct"] = direct
        full_info["desc"] = desc
        full_info["desc_len"] = desc_len
        full_info["view_count"] = stats[0]
        full_info["promotion_counter"] = stats[1]
        full_info["image_link"] = image_link

        yield full_info

    def errback_httpbin(self, failure):
        # log all failures
        self.logger.error(repr(failure))
    
    def polish_to_datetime(self, date):
        # change polish date format to datetime
        date = date.replace("<strong>", "").replace("</strong>", "")
        if date == 'dzisiaj':
            date = date.replace('dzisiaj', self.today_str)
        elif date == 'wczoraj':
            date = date.replace('wczoraj', self.yesterday_str)
        else:
            for pol, num in MONTHS_DICT.items():
                date = date.replace(pol, num)
            # Add trailing zeros -,-
            date_elements = date.split()
            if len(date_elements[0]) == 1:
                date_elements[0] = '0' + date_elements[0]
            if len(date_elements[1]) == 1:
                date_elements[1] = '0' + date_elements[1]

            date = '-'.join(date_elements)

        date = datetime.strptime(date, '%d-%m-%Y')
        return date

    def read_and_update_previous_scraping_date(self, previous_scraping_date_path='morizon_spider/previous_scraping_date.pkl'):
        try:
            self.logger.info('Trying to read previous scraping date ...')
            with open(previous_scraping_date_path, 'rb') as date_handle:
                previous_date = pickle.load(date_handle)
                self.logger.info(f'Succesfuly found previous scraping date: {previous_date}')
        except FileNotFoundError:
            self.logger.info('Did not find previous scraping date')
            previous_date = None

        with open(previous_scraping_date_path, 'wb') as date_handle:
            yesterday =(datetime.now().date() - timedelta(days=1)) 
            pickle.dump(yesterday, date_handle)
            self.logger.info(f'Saved yesterday date as previous: {yesterday}')
        # Check if already scraped all offers from previous date    
        #if previous_date != None:
            #if previous_date == yesterday:
                #self.crawler.engine.close_spider(self, reason='Already scraped all offers from previous date!')
        return str(previous_date)[:10]

    def set_previous_date(self, date, previous_scraping_date_path='morizon_spider/previous_scraping_date.pkl'):
        with open(previous_scraping_date_path, 'wb') as date_handle:
            date = datetime.strptime(date, '%Y-%m-%d') 
            pickle.dump(date, date_handle)
            self.logger.info(f'Saved custom date as previous: {date}')
           




