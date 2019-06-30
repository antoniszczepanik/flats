from os.path import exists
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def read_last_scraping_date(
    path='previous_scraping_dates.txt',
    crawler_name='morizon_spider',
    previous_date_if_none=datetime.strptime('2018-01-01', '%Y-%m-%d').date()):

    previous_date_if_none = str(previous_date_if_none)
    if exists(path):
        logger.info('Found previous scraping date file...')
        scraping_history_dict = load_obj(path)
    else:
        logger.info('Previous scraping date file not found. Initialized new file.')
        scraping_history_dict = {crawler_name: [previous_date_if_none]}
        save_obj(scraping_history_dict, path)
        scraping_history_dict = load_obj(path)
    if crawler_name in scraping_history_dict:
        date =  scraping_history_dict[crawler_name][-1]
    else:
        # create new entry
        scraping_history_dict[crawler_name] = [previous_date_if_none]
        logger.info('Did not find previous scraping date for %s', crawler_name)
        save_obj(scraping_history_dict, path)
        logger.info('Added %s entry to previous scraping date file.'
                    'Initialized new date:', crawler_name)
        date = previous_date_if_none
    logger.info(date)
    return datetime.strptime(date, '%Y-%m-%d').date()

def update_last_scraping_date(path='previous_scraping_dates.txt',
    crawler_name='morizon_spider',
    # Yesterdays date as default
    date=datetime.now().date()-timedelta(days=1)
    ):
    date = str(date)
    scraping_history_dict = load_obj(path)
    logger.info('Loaded previous scraping date from %s', path)
    if scraping_history_dict[-1] == date:
        logger.info('Previous scraping date is up to date. Not updating.')
    else:
        scraping_history_dict[crawler_name].append(date)
        logger.info('Added %s as new previous scraping date', date)
        logger.info('Updated %s', path)
    save_obj(scraping_history_dict, path)


def save_obj(obj, name):
    with open(name, 'w+') as f:
        for spider_name, dates_list in obj.items():
            dates_string = ','.join(dates_list)
            full_string = '{}:{}\n'.format(spider_name, dates_string)
            f.write(full_string)

def load_obj(name):
    result_dict = {}
    with open(name, 'r') as f:
        for line in f.read().split('\n')[:-1]:
            spider_name, dates_string = line.split(':')[0], line.split(':')[1]
            result_dict[spider_name] = dates_string.split(',')
    return result_dict
