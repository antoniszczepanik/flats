#!/usr/bin/env bash

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
