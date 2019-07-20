apt-get update
apt-get -y install build-essential python3-dev python3-pip
pip3 install -r requirements.txt
# add scrapyd etc to path
export PATH=$PATH:~/.local/bin

# run scrapyd server in background
nohup scrapyd >/dev/null 2>&1 & 
echo "Scrapyd server is up and running!"

cd ../spider
scrapyd-deploy local-target -p morizon_spider
cd ../setup

crontab cron_jobs
echo "Successfully added following jobs to schedule:"
cat cron_jobs

# create logs directories if dont exist
mkdir -p logs/morizon_spider/morizon_rent
mkdir -p logs/morizon_spider/morizon_sale
