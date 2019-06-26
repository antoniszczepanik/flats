from os.path import exists
import  logging
import pickle
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def read_last_scraping_date(
    path='previous_scraping_dates.pkl',
    crawler_name='morizon_spider',
    previous_date_if_none=datetime.strptime('2018-01-01', '%Y-%m-%d').date()
    ):

    if exists(path):
        logger.info('Previous scraping date found:')
        scraping_history_dict = load_obj(path)
    else:
        logger.info('Previous scraping date not found. Created:')
        scraping_history_dict = {crawler_name: [previous_date_if_none]}
        save_obj(scraping_history_dict, path)
        scraping_history_dict = load_obj(path)
    if crawler_name in scraping_history_dict:
        date =  scraping_history_dict[crawler_name][-1]   
    else:
        scraping_history_dict[crawler_name] = [previous_date_if_none]
        logger.info(f'Did not find {crawler_name} previous scraping date.')
        save_obj(scraping_history_dict, path)
        logger.info(f'Added {crawler_name} to previous scraping date file.')
        date = previous_date_if_none
    logger.info(date)
    return date 

def update_last_scraping_date(path='previous_scraping_dates.pkl',
    crawler_name='morizon_spider',
    # Yesterdays date as default 
    date=datetime.now().date()-timedelta(days=1)
    ):
    
    scraping_history_dict = load_obj(path)
    logger.info(f'Loaded previous scraping date from {path}')
    scraping_history_dict[crawler_name].append(date)
    logger.info(f'Added {date} as previous scraping date')
    save_obj(scraping_history_dict, path)
    logger.info(f'Updated {path}')

def save_obj(obj, name ):
    with open(name, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open(name, 'rb') as f:
        return pickle.load(f)
