#!/usr/bin/env bash

aws ec2 run-instances \
	--image-id ami-0cc0a36f626a4fdf5 \
	--count 1 \
	--instance-type t2.micro \
	--key-name flats_worker
