#!/usr/bin/env bash

docker run -it --rm \
    -v "$PWD/code:/usr/local/code" \
    -v "$HOME/.aws/credentials:/root/.aws/credentials:ro" \
    --network=flats \
    --name=flats \
    antoniszczepanik/flats "$@"
