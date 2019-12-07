build-webserver:
	docker build airflow/ \
		-f ./docker/Dockerfile-webserver \
		-t 'airflow_webserver'
build-jupyter:
	docker build ./airflow/ \
		-f docker/Dockerfile-jupyter \
		-t 'airflow_jupyter'
build-dev: build-webserver build-jupyter
dev: build-webserver
	docker run -it  \
		   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		   -v airflow/dags:/usr/local/airflow/dags \
		   -v airflow/deps:/usr/local/airflow/deps \
		   'airflow_webserver' /bin/bash 
clean-sale: build-webserver
	docker run -it  \
		   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		   -v airflow/dags:/usr/local/airflow/dags \
		   -v airflow/deps:/usr/local/airflow/deps \
		   'airflow_webserver' \
		   python3 -c "from deps.pipelines.cleaning_task import cleaning_task; cleaning_task('sale')"
clean-rent: build-webserver
	docker run -it  \
	   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
	   -v airflow/dags:/usr/local/airflow/dags \
	   -v airflow/deps:/usr/local/airflow/deps \
	   'airflow_webserver' \
	   python3 -c "from deps.pipelines.cleaning_task import cleaning_task; cleaning_task('sale')"
coords-map-rent: build-webserver
	docker run -it  \
		-v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		-v airflow/dags:/usr/local/airflow/dags \
		-v airflow/deps:/usr/local/airflow/deps \
		'airflow_webserver' \
		python3 -c "from deps.pipelines.coords_map_task import coords_map_task; coords_map_task('rent')"
coords-map-sale: build-webserver
	docker run -it  \
		-v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		-v airflow/dags:/usr/local/airflow/dags \
		-v airflow/deps:/usr/local/airflow/deps \
		'airflow_webserver' \
		python3 -c "from deps.pipelines.coords_map_task import coords_map_task; coords_map_task('sale')"
compose: 
	docker-compose -f docker/docker-compose-dev.yml up -d
compose-down:
	docker-compose -f docker/docker-compose-dev.yml down
compose-restart:
	docker-compose -f docker/docker-compose-dev.yml restart
compose-prod:
	docker-compose -f docker/docker-compose-prod.yml up -d
