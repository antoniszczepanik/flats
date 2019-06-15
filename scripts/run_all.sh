#!/bin/bash
cd ~/Public/google-drive-ftp-adapter/build &&
echo "Starting google-drive-ftp-adapter..."
gnome-terminal --title 'ftp' -e 'java -jar google-drive-ftp-adapter-jar-with-dependencies.jar' &&
echo "Google-drive-ftp-adapter started."
cd ~/repos/avm-v2/scripts
./run_spider_sale.sh &&
./run_spider_rent.sh &&
echo "Run both scripts successfully."&& 
exit
