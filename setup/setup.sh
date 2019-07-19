apt-get update
apt-get -y install build-essential python3-dev python3-pip
pip3 install -r requirements.txt
export PATH=$PATH:~/.local/bin

nohup scrapyd >/dev/null 2>&1 & 
echo "Scrapyd server is up and running!"

cd ../spider
scrapyd-deploy local-target -p morizon_spider
cd ../setup

crontab cron_jobs
echo "Successfully added following jobs to schedule:"
cat cron_jobs
