import pickle
from datetime import datetime, timedelta


def read_previous_scraping_date(path='morizon_spider/previous_scraping_date.pkl', crawler_name='morizon_spider'):
	""" Try to read previous scraping date saved locally in
	previous_scraping_date.pkl. If the file is not found
	set the date to None. The assumed file structure is a 
	dictionary with crawler_names and ascending list of 
	previous scraping dates. This functions reads the last
	(most up-to_date) datetime.
	{
	'morizon_spider=['01-01-2018'],
	'morizon_spider_rent'=['01-01-2018'],
	}
	Returns datetime 
	"""
        try:
            self.logger.info('Trying to read previous scraping date ...')
            with open(path, 'rb') as previous_dates:
                previous_date = pickle.load(previous_dates)[crawler_name][-1]
                self.logger.info(f'Succesfuly found previous scraping date for {crawler_name}: {previous_date}')
        except Exception as e:
            self.logger.info(f'Problem occured when reading previous date: {e}. Creating prebious scraping date entry')
	    create_previous_scraping_date_for_crawler
	    read_previous_scraping_date
        return previous_date

def add_date_to_previous_scraping_dates(
	path='previous_scraping_dates.pkl',
	crawler_name='morizon_spider',
	# Yesterdays date as default
	date=datetime.now().date()-timedelta(days=1)
)
	try:
    	    with open(path, 'wb') as previous_dates:
       	    	previous_dates_dict = pickle.load(previous_dates)
		previous_dates_dict[crawler_name].append(date)
		pickle.dump(previous_dates_dict, previous_dates)
            	self.logger.info(f'Saved date as previous: {date}')
	except Exception as e:
            self.logger.info(f'Problem occured when updating previous date: {e}')

def create_previous_scraping_date_for_crawler(
	path = 'previous_scraping_dates.pkl',
	crawler_name = 'morizon_spider',
	date = datetime.strptime('2018-01-01', '%Y-%m-%d')
)
	try:
    	    with open(path, 'wb') as previous_dates:
		try:
       	    	    previous_dates_dict = pickle.load(previous_dates)
		except FileNotFoundError:
            	    self.logger.info(f'Did not find {path}. Creating new...')
		    previous_dates_dict = {}    
    		previous_dates_dict[crawler_name] = [date]
		pickle.dump(previous_dates_dict, previous_dates)
            	self.logger.info(f'Saved {crawler_name} previous date: {date}')
	except Exception as e:
		self.logger.info(f'Error occured wher creating previous date file: {e}')	
		raise e
