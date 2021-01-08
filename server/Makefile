build:
	docker build -t flats_v2_server .
run: build
	docker run -it --rm\
		-p 8000:8000 \
		-v ${PWD}/src:/src \
		--network flats \
		--name server \
		-t flats_v2_server \
		flats_v2_server
bash: build
	docker run -it --rm\
		-p 8000:8000 \
		-v ${PWD}/src:/src \
		--network flats \
		--name postgres \
		flats_v2_server bash
pg:
	docker run -it --rm \
		-p 5432:5432 \
		-e POSTGRES_USER=admin \
		-e POSTGRES_PASSWORD=admin \
		--network flats \
		postgres

