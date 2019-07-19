sudo apt-get update
# scrapyd installed via apt to ease administration work
sudo apt-get -y install build-essential python3-dev python3-pip
sudo pip3 install -r requirements.txt
export PATH=$PATH:~/.local/bin
nohup scrapyd >/dev/null 2>&1 & 
echo "Scrapyd server is up and running!"
cd spider
scrapyd-deploy local-target -p morizon_spider
