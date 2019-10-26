!#/usr/bin/env bash

git clone https://github.com/kabuboy/flats.git

# install docker 
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker ubuntu

# compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.24.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# install make and run container
sudo apt install make
cd flats && \
	sudo make docker-run-prod && \
	echo "Successfully running prod container!"

