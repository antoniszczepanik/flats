
black:
	black ./code

test:
	pytest ./code

docker-run:
	docker build  . \
		-f deploy/Dockerfile-dev \
		-t flats
	docker run \
		-it \
		-v $(PWD)/code:/code \
		flats
