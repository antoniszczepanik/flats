sudo apt-get update
sudo apt-get -y install build-essential python3-dev python3-pip
pip3 install -r requirements.txt
cd spider
nohup scrapyd >/dev/null 2>&1 &

