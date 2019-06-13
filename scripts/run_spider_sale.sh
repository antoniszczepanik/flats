#!/bin/bash
cd ~/Public/google-drive-ftp-adapter/build
echo "Starting google-drive-ftp-adapter..."
gnome-terminal --title 'ftp' -e 'java -jar google-drive-ftp-adapter-jar-with-dependencies.jar'
gdfa=$!
echo "Google-drive-ftp-adapter started. Starting morizon scraper..."
echo "Google-drive-ftp-adapter started. Starting morizon scraper..."
cd ~/repos/avm-v2
_now=$(date +"%m_%d_%Y")
_file_path="ftp://user:user@localhost:1821/Scraping_data/sale/sales_up_to_$_now.csv"
scrapy crawl morizon -o $_file_path &&
echo "Saved scraped sale data on GDrive as $_file_path";
kill $gdfa
exit

