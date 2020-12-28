build:
	docker build -t flats-server .

# Mounting directories does not work in below for some reason
run-dev: build
	docker run -it -p 5000:5000 \
		-v src:/src \
		--rm \
		--name flats-server \
		flats-server
run-prod: build
	docker run \
		-p 5000:5000 \
		--rm \
		--name \
		flats-server-prod \
		--entrypoint /bin/bash \
		flats-server gunicorn -w 4 app:app
bash:
	docker run \
		-it \
		-p 5000:5000 \
		-v src:/src \
		--rm \
		--name flats-server \
		--entrypoint /bin/bash \
		flats-server
