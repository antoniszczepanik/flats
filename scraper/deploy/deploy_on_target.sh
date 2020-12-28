#!/usr/bin/env bash
cd /flats
sudo git stash && sudo git pull
make compose-prod-down
make compose-prod
