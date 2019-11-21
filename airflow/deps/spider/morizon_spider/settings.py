# -*- coding: utf-8 -*-
import boto3

from common import RAW_DATA_PATH
from botocore.exceptions import ProfileNotFound

# Scrapy settings for morizon_spider project
BOT_NAME = "morizon_spider"
SPIDER_MODULES = ["morizon_spider.spiders"]
NEWSPIDER_MODULE = "morizon_spider.spiders"
LOG_LEVEL = "INFO"

# AWS S3 export settings
FEED_URI = (
    "s3://" + RAW_DATA_PATH.format(data_type="%(name)s") + "/raw_%(name)s_%(time)s.csv"
)
FEED_FORMAT = "csv"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# AWS configuration
try:
    session = boto3.Session(profile_name='flats')
except ProfileNotFound:
    # try to get credentials from default profile
    try:
        session = boto3.Session()
    except:
        log.error('Could not get default boto3 session!')
        raise ProfileNotFound
    else:
        log.info('Using default boto3 profile.')
else:
    log.info('Using "flats" boto3 profile.')
finally:
    creds = session.get_credentials()

AWS_ACCESS_KEY_ID = creds.access_key
AWS_SECRET_ACCESS_KEY = creds.secret_key

log.info(f'{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}')

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {"morizon_spider.pipelines.MorizonSpiderPipeline": 300}

# Debugging
# CLOSESPIDER_PAGECOUNT = 100

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'morizon_spider.middlewares.MorizonSpiderSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    'morizon_spider.middlewares.MorizonSpiderDownloaderMiddleware': 543,
# }

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
