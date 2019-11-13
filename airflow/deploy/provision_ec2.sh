#!/usr/bin/env bash

git clone https://github.com/kabuboy/flats.git

# install docker 
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker ubuntu

# install compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.24.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Make externally attached drive a filesystem
# sudo mkfs -t xfs /dev/xvdb
# Create data directory and mount the drive
# sudo mkdir /data
# sudo mount /dev/svdb /data

sudo apt install make

cd flats/airflow && make compose-prod

