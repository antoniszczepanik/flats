mkfile_dir := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

build-webserver:
	docker build $(mkfile_dir)/airflow/ \
		-f $(mkfile_dir)/docker/Dockerfile-webserver \
		-t 'airflow_webserver'
build-jupyter:
	docker build $(mkfile_dir)/airflow/ \
		-f $(mkfile_dir)/docker/Dockerfile-jupyter \
		-t 'airflow_jupyter'
build-dev: build-webserver build-jupyter
dev: build-webserver
	docker run -it  \
		   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		   -v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
		   -v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
		   'airflow_webserver' /bin/bash 
clean-sale: build-webserver
	docker run -it  \
		   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		   -v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
		   -v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
		   'airflow_webserver' \
		   python3 -c "from deps.pipelines.cleaning_task import cleaning_task; cleaning_task('sale')"
clean-rent: build-webserver
	docker run -it  \
	   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
	   -v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
	   -v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
	   'airflow_webserver' \
	   python3 -c "from deps.pipelines.cleaning_task import cleaning_task; cleaning_task('rent')"
concat-sale: build-webserver
	docker run -it  \
	   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
	   -v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
	   -v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
	   'airflow_webserver' \
	   python3 -c "from deps.pipelines.concat_task import concat_data_task; concat_data_task('sale')"
concat-rent: build-webserver
	docker run -it  \
	   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
	   -v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
	   -v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
	   'airflow_webserver' \
	   python3 -c "from deps.pipelines.concat_task import concat_data_task; concat_data_task('rent')"
coords-map-rent: build-webserver
	docker run -it  \
		-v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		-v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
		-v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
		'airflow_webserver' \
		python3 -c "from deps.pipelines.coords_map_task import coords_map_task; coords_map_task('rent')"
coords-map-sale: build-webserver
	docker run -it  \
		-v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		-v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
		-v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
		'airflow_webserver' \
		python3 -c "from deps.pipelines.coords_map_task import coords_map_task; coords_map_task('sale')"
unify-raw-sale: build-webserver
	docker run -it  \
		-v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		-v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
		-v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
		'airflow_webserver' \
		python3 -c "from deps.pipelines.unify_raw_task import unify_raw_data_task; unify_raw_data_task('sale')"
unify-raw-rent: build-webserver
	docker run -it  \
		-v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		-v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
		-v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
		'airflow_webserver' \
		python3 -c "from deps.pipelines.unify_raw_task import unify_raw_data_task; unify_raw_data_task('rent')"
apply-rent: build-webserver
	docker run -it  \
	   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
	   -v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
	   -v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
	   'airflow_webserver' \
	   python3 -c "from deps.pipelines.apply_task import apply_task; apply_task('rent')"
apply-sale: build-webserver
	docker run -it  \
	   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
	   -v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
	   -v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
	   'airflow_webserver' \
	   python3 -c "from deps.pipelines.apply_task import apply_task; apply_task('sale')"
update-website-data: build-webserver
	docker run -it  \
	   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
	   -v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
	   -v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
	   'airflow_webserver' \
	   python3 -c "from deps.pipelines.update_website_task.update_website_data import update_website_data_task; update_website_data_task()"
update-website-source: build-webserver
	docker run -it  \
	   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
	   -v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
	   -v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
	   'airflow_webserver' \
	   python3 -c "from deps.pipelines.update_website_task.update_website_source import update_website_source; update_website_source()"

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
jupyter:
	docker run -d --rm \
	-v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
	-v $(mkfile_dir)/airflow/dags:/usr/local/airflow/dags \
	-v $(mkfile_dir)/airflow/deps:/usr/local/airflow/deps \
	-p 8889:8889 \
	--name airflow_jupyter \
	--entrypoint "jupyter" \
	'airflow_jupyter' lab --ip=0.0.0.0 --port=8889 --NotebookApp.token=''
	sleep 5
	xdg-open http://localhost:8889/lab? > /dev/null 2>&1 &
