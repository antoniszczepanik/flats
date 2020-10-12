mkfile_dir := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

build:
	docker build $(mkfile_dir)code \
		-f $(mkfile_dir)docker/Dockerfile \
		-t 'flats'
	docker tag flats:latest antoniszczepanik/flats:latest
push:
	docker push antoniszczepanik/flats:latest
full-pipeline-rent: scrape-rent concat-rent clean-rent features-rent apply-rent update-website-data
