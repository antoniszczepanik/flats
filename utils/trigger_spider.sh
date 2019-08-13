#!/usr/bin/env bash
if [ "$1" = "morizon_sale" ] || [ "$1" = "morizon_rent" ]; then
  curl http://localhost:6800/schedule.json -d project=morizon_spider -d spider=$1
else
  echo "You should specify one of 'morizon_rent' or 'morizon_sale' as a parameter"
fi
