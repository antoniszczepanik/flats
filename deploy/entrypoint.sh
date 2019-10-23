#!/usr/bin/env bash

crontab /cron_jobs

echo "Added following jobs to crontab:"
cat /cron_jobs
