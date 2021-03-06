#!/usr/bin/env bash

mkdir -p $(cd .. && pwd)/buckets/flats-data
mkdir -p $(cd .. && pwd)/buckets/flats-models

docker run --rm -itd \
  -p 9999:9000 \
  --name minio \
  --user $(id -u):$(id -g) \
  --network flats \
  -e "MINIO_ACCESS_KEY=minio" \
  -e "MINIO_SECRET_KEY=miniominio" \
  -v $(cd .. && pwd)/buckets:/data \
  minio/minio -- server /data
