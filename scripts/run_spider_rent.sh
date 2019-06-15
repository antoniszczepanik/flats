#!/bin/bash
echo "Starting rent scraper..."
cd ~/repos/avm-v2
_now=$(date +"%m_%d_%Y")
_file_path="ftp://user:user@localhost:1821/Scraping_data/rent/rent_up_to_$_now.csv"
scrapy crawl morizon_rent -o $_file_path &&
echo "Saved scraped rent data on GDrive as $_file_path"

