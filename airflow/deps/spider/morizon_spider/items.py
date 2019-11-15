# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MorizonSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    title__offer = scrapy.Field()
    floor__offer = scrapy.Field()
    building_height__offer = scrapy.Field()
    offer_id__offer = scrapy.Field()
    building_year__offer = scrapy.Field()
    date_added__offer = scrapy.Field()
    date_refreshed__offer = scrapy.Field()
    building_type__offer = scrapy.Field()
    building_material__offer = scrapy.Field()
    market_type__offer = scrapy.Field()
    flat_state__offer = scrapy.Field()
    balcony__offer = scrapy.Field()
    taras__offer = scrapy.Field()
    price__offer = scrapy.Field()
    price_m2__offer = scrapy.Field()
    size__offer = scrapy.Field()
    room_n__offer = scrapy.Field()
    lat__offer = scrapy.Field()
    lon__offer = scrapy.Field()
    url__offer = scrapy.Field()
    direct__offer = scrapy.Field()
    desc_len__offer = scrapy.Field()
    view_count__offer = scrapy.Field()
    promotion_counter__offer = scrapy.Field()
    desc__offer = scrapy.Field()
    image_link__offer = scrapy.Field()
    heating__offer = scrapy.Field()
    conviniences__offer = scrapy.Field()
    media__offer = scrapy.Field()
    equipment__offer = scrapy.Field()
