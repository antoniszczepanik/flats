#!/usr/bin/env bash

# TODO: add query to terminate only working instances - not listing terminated etc
aws ec2 terminate-instances --instance-ids \
	$(aws ec2 describe-instances \
		--filters  "Name=tag:Name,Values=Airflow" \
		--query "Reservations[].Instances[].[InstanceId]" \
		--output text | tr '\n' ' ')

# run a new one
aws ec2 run-instances \
	       --image-id ami-0cc0a36f626a4fdf5 \
	       --count 1 \
	       --instance-type t2.micro \
	       --key-name flats_worker \
	       --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=Airflow}]' \
	       --user-data provision_ec2.sh

IP=$(aws ec2 describe-instances \
		--filters  "Name=tag:Name,Values=Airflow" \
		--query "Reservations[].Instances[].PublicIpAddress" \
		--output text | tr '\n' ' ')

ssh -i 'airflow.pem' "ubuntu@$IP"
