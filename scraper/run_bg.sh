#!/usr/bin/env bash

docker run --rm \
    -v "$PWD/src:/usr/local/src" \
    -v "$HOME/.aws/credentials:/root/.aws/credentials:ro" \
    --network=flats \
    flats-scraper "$@"
