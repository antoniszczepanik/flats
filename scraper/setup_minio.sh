#!/usr/bin/env bash

# Arguments:
# $1 - host path to root directory of "s3" data
mkdir -p ~/flats_buckets

docker run --rm -itd \
-p 9999:9000 \
--name minio \
--user $(id -u):$(id -g) \
--network flats \
-e "MINIO_ACCESS_KEY=minio" \
-e "MINIO_SECRET_KEY=miniominio" \
-v ~/flats_buckets:/data \
minio/minio -- server /data

