#!/usr/bin/env bash

mkdir -p "$PWD/../data/flats-data"
mkdir -p "$PWD/../data/flats-models"
docker run -it --rm \
    -v "$PWD/src:/usr/local/src" \
    -v "$PWD/../data:/data" \
    -v "$HOME/.aws/credentials:/root/.aws/credentials:ro" \
    --network=flats \
    flats-scraper "$@"
