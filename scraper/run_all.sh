#!/usr/bin/env bash

./run_task.sh scrape rent
./run_task.sh concat rent
./run_task.sh clean rent
./run_task.sh features rent
./run_task.sh apply rent

./run_task.sh scrape sale
./run_task.sh concat sale
./run_task.sh clean sale
./run_task.sh features sale
./run_task.sh apply sale

./run_task.sh prepare-final whatever
