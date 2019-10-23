!#/usr/bin/env bash

git clone https://github.com/kabuboy/flats.git

# install docker 
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker ubuntu

# install make and run container
sudo apt install make
cd flats && \
	sudo make docker-run-prod && \
	echo "Successfully running prod container!"

