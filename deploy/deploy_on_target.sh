#!/usr/bin/env bash
cd /flats
git pull
make compose-prod-down
make compose-prod
