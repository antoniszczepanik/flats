mkfile_dir := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: deploy deploy-hard

build:
	docker build $(mkfile_dir)code \
		-f $(mkfile_dir)docker/Dockerfile \
		-t 'flats'
build-jupyter:
	docker build $(mkfile_dir)/airflow/ \
		-f $(mkfile_dir)/docker/Dockerfile-jupyter \
		-t 'airflow_jupyter'
compose: 
	docker-compose -f $(mkfile_dir)/docker/docker-compose-dev.yml up -d --force-recreate
compose-build: 
	docker-compose -f $(mkfile_dir)/docker/docker-compose-dev.yml build
compose-down:
	docker-compose -f $(mkfile_dir)/docker/docker-compose-dev.yml down
compose-restart:
	docker-compose -f $(mkfile_dir)/docker/docker-compose-dev.yml restart

compose-prod:
	docker-compose -f $(mkfile_dir)/docker/docker-compose-prod.yml up -d
compose-prod-down:
	docker-compose -f $(mkfile_dir)/docker/docker-compose-prod.yml down
deploy:
	cd deploy && ./deploy.sh
deploy-hard:
	cd deploy && ./deploy.sh restart
dev:
	docker run -it  \
		   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		   -v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
		   -v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
		   'airflow_webserver' /bin/bash 
update-website-source: 
	cd site && npm run build
	aws s3 sync site/build/ s3://flats.antoniszczepanik.com/ --profile=flats
full-pipeline-rent: scrape-rent concat-rent clean-rent features-rent apply-rent update-website-data
full-pipeline-sale: scrape-sale concat-sale clean-sale features-sale apply-sale update-website-data
jupyter:
	docker run -d --rm \
	-v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
	-v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
	-v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
	-e USE_MINIO=true \
	-p 8889:8889 \
	--name airflow_jupyter \
	--network flats \
	--entrypoint "jupyter" \
	'airflow_jupyter' lab --ip=0.0.0.0 --port=8889 --NotebookApp.token=''
	sleep 5
	xdg-open http://localhost:8889/lab? > /dev/null 2>&1 &
