#!usr/bin/env bash

# stop executing commands on failure
set -e

# make sure in the correct directory
REPO_ROOT=$(git rev-parse --show-toplevel)
cd $REPO_ROOT/setup

# install requirements
apt-get update
apt-get -y install build-essential python3-dev python3-pip s3fs
pip3 install -r requirements.txt

# add scrapyd etc to path
export PATH=$PATH:~/.local/bin
# run scrapyd server in background
nohup scrapyd >/dev/null 2>&1 & 
echo "Scrapyd server is up and running!"

# deploy spider
cd $REPO_ROOT/spider
scrapyd-deploy local-target -p morizon_spider
cd $REPO_ROOT/setup

# setup cron jobs
crontab cron_jobs
echo "Successfully added following jobs to schedule:"
cat cron_jobs

# mount s3 in morizon-data
echo "Mounting s3 in ~/morizon-data"
mkdir -p ~/morizon-data
s3fs morizon-data ~/morizon-data -o nonempty -o iam_role='S3_permission_for_ec2' -o use_cache=/tmp -o allow_other -o mp_umask=077 -o uid=1000 -o gid=1000


