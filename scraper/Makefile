build:
	docker build -t flats-scraper .
bash: build
	docker run -it --rm \
		-v ${PWD}/src:/usr/local/src \
		-v ${HOME}/.aws/credentials:/root/.aws/credentials:ro \
		-p 8888:8888 \
		--network flats \
		--entrypoint /bin/bash flats-scraper

jupyter: build
	docker run -it --rm \
		-v ${PWD}/src:/usr/local/src \
		-v ${HOME}/.aws/credentials:/root/.aws/credentials:ro \
		-p 8888:8888 \
		--network flats \
		--entrypoint ./jupyter.sh flats-scraper
