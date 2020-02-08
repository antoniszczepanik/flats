#!/usr/bin/env bash

IP=$(aws ec2 describe-instances \
		--filters  "Name=tag:Name,Values=Airflow" \
		--query "Reservations[].Instances[].PublicIpAddress" \
		--output text | tr '\n' ' ')

if [ "$1" == "restart" ];then
	ssh -i 'airflow.pem' ubuntu@$IP  "bash -s" < deploy_on_target.sh
else
	ssh -i 'airflow.pem' ubuntu@$IP  "cd /flats && sudo git pull"
fi
