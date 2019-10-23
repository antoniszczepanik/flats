black:
	black ./code
test:
	pytest ./code
docker-build:
	docker build  . \
		-f deploy/Dockerfile-dev \
		-t flats
docker-run: docker-build
	docker run \
		-it --rm \
		-v $(PWD)/code:/code \
		-e AWS_ACCESS_KEY_ID=$(shell aws configure get aws_access_key_id) \
		-e AWS_SECRET_ACCESS_KEY=$(shell aws configure get aws_secret_access_key) \
		flats
scrape-sale: docker-build
	docker run \
		--name flats \
		--rm -it -d \
		-v $(PWD)/code:/code \
		-e AWS_ACCESS_KEY_ID=$(shell aws configure get aws_access_key_id) \
		-e AWS_SECRET_ACCESS_KEY=$(shell aws configure get aws_secret_access_key) \
		flats
	-docker exec -w /code/spider -it flats scrapy crawl sale
	docker kill flats
docker-run-detached:
	docker run \
		--name flats \
		--rm -it -d \
		-v $(PWD)/code:/code \
		-e AWS_ACCESS_KEY_ID=$(shell aws configure get aws_access_key_id) \
		-e AWS_SECRET_ACCESS_KEY=$(shell aws configure get aws_secret_access_key) \
		flats
scrape-rent: docker-build docker-run-detached
	-docker exec -w /code/spider -it flats scrapy crawl rent
	docker kill flats
scrape: scrape-sale scrape-rent
concat: docker-build docker-run-detached
	-docker exec -w /code/pipelines -it flats ./concat_task.py
	docker kill flats
clean: docker-build docker-run-detached
	-docker exec -w /code/pipelines -it flats ./cleaning_task.py
	docker kill flats
coords-map: docker-build docker-run-detached
	-docker exec -w /code/pipelines -it flats ./create_coords_map_task.py
	docker kill flats
features: docker-build docker-run-detached
	-docker exec -w /code/pipelines -it flats ./feature_engineering_task.py
	docker kill flats

docker-build-prod:
	docker build  . \
		-f deploy/Dockerfile-prod \
		-t flats
docker-run-prod:
	docker run flats
