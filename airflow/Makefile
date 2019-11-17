build-dev:
	docker build . -t 'flats-container'
	docker run -it \
		   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		   -v $(PWD)/dags:/usr/local/airflow/dags \
		   -v $(PWD)/deps:/usr/local/airflow/deps \
		   'flats-container' /bin/bash 
dev:
	docker run -it  \
		   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		   -v $(PWD)/dags:/usr/local/airflow/dags \
		   -v $(PWD)/deps:/usr/local/airflow/deps \
		   'flats-container' /bin/bash 
clean-sale:
	docker run -it  \
		   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		   -v $(PWD)/dags:/usr/local/airflow/dags \
		   -v $(PWD)/deps:/usr/local/airflow/deps \
		   'flats-container' \
		   python3 -c "from deps.pipelines.cleaning_task import cleaning_task; cleaning_task('sale')"
clean-rent:
	docker run -it  \
		   -v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
		   -v $(PWD)/dags:/usr/local/airflow/dags \
		   -v $(PWD)/deps:/usr/local/airflow/deps \
		   'flats-container' \
		   python3 -c "from deps.pipelines.cleaning_task import cleaning_task; cleaning_task('sale')"
compose: 
	docker-compose -f docker-compose-dev.yml up -d
compose-prod:
	docker-compose -f docker-compose-prod.yml up -d
compose-down:
	docker-compose -f docker-compose-dev.yml down
compose-restart:
	docker-compose -f docker-compose-dev.yml restart
coords-map-rent:
	docker run -it  \
			-v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
			-v $(PWD)/dags:/usr/local/airflow/dags \
			-v $(PWD)/deps:/usr/local/airflow/deps \
			'flats-container' \
			python3 -c "from deps.pipelines.coords_map_task import coords_map_task; coords_map_task('rent')"
coords-map-sale:
	docker run -it  \
			-v $(HOME)/.aws/credentials:/usr/local/airflow/.aws/credentials:ro \
			-v $(PWD)/dags:/usr/local/airflow/dags \
			-v $(PWD)/deps:/usr/local/airflow/deps \
			'flats-container' \
			python3 -c "from deps.pipelines.coords_map_task import coords_map_task; coords_map_task('sale')"
