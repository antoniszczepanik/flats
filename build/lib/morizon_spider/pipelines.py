# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime, timedelta


class MorizonSpiderPipeline(object):

        months_dict = {
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
        # def __init__(self):
        #       self.ids_seen = set()

        def process_item(self, item, spider):
                # if item['offer_id'] in self.ids_seen:
                #       raise DropItem(f"Duplicate item found with ID {item['offer_id']}")

                item['date_added'] = self.polish_to_datetime(item['date_added'])
                item['date_refreshed'] = self.polish_to_datetime(item['date_refreshed'])

                return item

        def polish_to_datetime(self, date):

                date = date.replace("<strong>", "").replace("</strong>", "")
                if date == 'dzisiaj':
                        date = date.replace('dzisiaj', datetime.today().strftime('%d-%m-%Y'))
                elif date == 'wczoraj':
                        date = date.replace('wczoraj', (datetime.today() - timedelta(days=1)).strftime('%d-%m-%Y'))
                else:
                        for pol, num in self.months_dict.items():
                                date = date.replace(pol, num)
                        # Add trailing zeros -,-
                        date_elements = date.split()
                        if len(date_elements[0]) == 1:
                                date_elements[0] = '0' + date_elements[0]
                        if len(date_elements[1]) == 1:
                                date_elements[1] = '0' + date_elements[1]

                        date = '-'.join(date_elements)
                date = str(datetime.strptime(date, '%d-%m-%Y'))[:10]
                return date





