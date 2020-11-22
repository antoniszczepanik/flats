#!/usr/bin/env bash

OFFER_TYPE=$1

./run_task.sh scrape $OFFER_TYPE --use-remote
./run_task.sh concat $OFFER_TYPE --use-remote
./run_task.sh clean $OFFER_TYPE --use-remote
./run_task.sh features $OFFER_TYPE --use-remote
./run_task.sh apply $OFFER_TYPE --use-remote
./run_task.sh update-data $OFFER_TYPE --use-remote
