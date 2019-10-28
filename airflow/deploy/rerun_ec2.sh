#!/usr/bin/env bash

# kill existing flats worker
# TODO: add query to terminate only working instances - not listing terminated etc
aws ec2 terminate-instances --instance-ids \
	$(aws ec2 describe-instances \
		--filters  "Name=tag:Name,Values=flats_worker" \
		--query "Reservations[].Instances[].[InstanceId]" \
		--output text | tr '\n' ' ')

# run a new one
aws ec2 run-instances \
	       --image-id ami-0cc0a36f626a4fdf5 \
	       --count 1 \
	       --instance-type t2.micro \
	       --key-name flats_worker \
	       --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=flats_worker}]' \
	       --user-data provision_ec2.sh

# show ssh connection line
IP=$(aws ec2 describe-instances \
		--filters  "Name=tag:Name,Values=flats_worker" \
		--query "Reservations[].Instances[].PublicIpAddress" \
		--output text | tr '\n' ' ')
echo 'To ssh into the instance use:'
SSH_CMD="ssh -i 'flats_worker.pem' ubuntu@$IP"
echo $SSH_CMD
