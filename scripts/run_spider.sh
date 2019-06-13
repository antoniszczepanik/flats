#!/bin/bash
cd ~/Public/google-drive-ftp-adapter/build
echo "Starting google-drive-ftp-adapter..."
gnome-terminal --tab --title="ftp" --command="java -jar google-drive-ftp-adapter-jar-with-dependencies.jar"
echo "Google-drive-ftp-adapter started. Starting morizon scraper..."
cd ~/Repos/Scraping-Spiders/morizon_spider
_now=$(date +"%m_%d_%Y")
_file_path="ftp://user:user@localhost:1821/Scraping_data/sales_up_to_$_now.csv"
scrapy crawl morizon -o $_file_path
echo "Saved scraped data on GDrive as $_file_path"

