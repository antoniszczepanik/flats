# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MorizonSpiderItem(scrapy.Item):
    # define the fields for your item here like:
	title = scrapy.Field()
	floor = scrapy.Field()
	building_height = scrapy.Field()
	offer_id = scrapy.Field()
	building_year = scrapy.Field()
	date_added = scrapy.Field()
	date_refreshed = scrapy.Field()
	building_type = scrapy.Field()
	building_material = scrapy.Field()
	market_type = scrapy.Field()
	flat_state = scrapy.Field()
	balcony = scrapy.Field()
	taras = scrapy.Field()
	price = scrapy.Field()
	price_m2 = scrapy.Field()
	size = scrapy.Field()
	room_n = scrapy.Field()
	lat = scrapy.Field()
	lon = scrapy.Field()
	url = scrapy.Field()
	direct = scrapy.Field()
	desc_len = scrapy.Field()
	view_count = scrapy.Field()
	promotion_counter = scrapy.Field()
