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
