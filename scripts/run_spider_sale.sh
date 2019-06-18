#!/bin/bash
echo "Starting sale scraper ..."
cd ~/repos/avm-v2
_now=$(date +"%m_%d_%Y")
_file_path="ftp://user:user@localhost:1821/Scraping_data/sale/sales_up_to_$_now.csv"
scrapy crawl morizon -o $_file_path &&
echo "Saved scraped sale data on GDrive as $_file_path";

